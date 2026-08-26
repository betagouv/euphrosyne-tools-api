from __future__ import annotations

import json
import logging
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clients.python_sessions import PythonExecutionResult, PythonSessionsClient
from data_visualization.llm import (
    DataVisualizationCompletion,
    DataVisualizationLlmClient,
)
from data_visualization.llm_trace import trace_llm_exchange
from data_visualization.models import (
    DataVisualization,
    GeneratedVisualizationResponse,
    PreparedDataVisualization,
)

logger = logging.getLogger(__name__)

DATA_VISUALIZATION_TIMEOUT_SECONDS = 300
RESPONSE_FORMAT_NAME = "data_visualizations"
PYTHON_TOOL_NAME = "execute_python"
CALCULATION_RESULT_FILENAME = "analysis_result.json"
MAX_PYTHON_EXECUTIONS = 3
MAX_VISUALIZATION_ATTEMPTS = 2
MAX_MODEL_DATA_BYTES = 1_000_000
MAX_CALCULATION_RESULT_BYTES = 1_000_000


class PythonExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    code: str = Field(min_length=1, max_length=50_000)


class DataVisualizationError(RuntimeError):
    """Raised when normalized data or a model response cannot be used."""


class CalculationResultError(ValueError):
    """Raised when a Python calculation did not produce a usable result file."""


@dataclass(frozen=True)
class DataVisualizationResult:
    answer: str
    visualizations: tuple[DataVisualization, ...]
    elapsed_seconds: float
    llm_calls: int
    usage: tuple[dict[str, Any], ...]


VISUALIZATION_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": RESPONSE_FORMAT_NAME,
        "description": (
            "Réponse à une question sur des données normalisées avec des "
            "visualisations ECharts autonomes."
        ),
        "strict": True,
        "schema": GeneratedVisualizationResponse.model_json_schema(),
    },
}


PYTHON_TOOL = {
    "type": "function",
    "function": {
        "name": PYTHON_TOOL_NAME,
        "description": (
            "Exécute du code Python dans la session contenant les données normalisées."
        ),
        "strict": True,
        "parameters": PythonExecutionRequest.model_json_schema(),
    },
}


class LlmCallRecorder:
    """Track one visualization request's LLM calls and usage."""

    def __init__(
        self,
        llm_client: DataVisualizationLlmClient,
        request_id: UUID | str,
    ) -> None:
        self._llm_client = llm_client
        self._request_id = request_id
        self.calls = 0
        self.usages: list[dict[str, Any]] = []

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] = "auto",
        response_format: dict[str, Any] | None = None,
    ) -> DataVisualizationCompletion:
        request_messages = deepcopy(messages)
        request: dict[str, Any] = {
            "call": self.calls + 1,
            "messages": request_messages,
        }
        if tools is not None:
            request["tools"] = tools
            request["tool_choice"] = tool_choice
        if response_format is not None:
            request["response_format"] = response_format
        trace_llm_exchange(
            self._request_id,
            "llm_request",
            request,
        )
        completion = self._llm_client.complete(
            request_messages,
            tools,
            tool_choice=tool_choice,
            response_format=response_format,
        )
        self.calls += 1
        self.usages.append(completion.usage)
        trace_llm_exchange(
            self._request_id,
            "llm_response",
            {
                "call": self.calls,
                "message": completion.message,
                "usage": completion.usage,
                "model": completion.model,
                "finish_reason": completion.finish_reason,
            },
        )
        return completion


class DataVisualizationService:
    def __init__(
        self,
        llm_client: DataVisualizationLlmClient,
        sessions_client: PythonSessionsClient,
    ) -> None:
        self._llm_client = llm_client
        self._sessions_client = sessions_client

    def run(
        self,
        prepared: PreparedDataVisualization,
        question: str,
        *,
        request_id: UUID | str = "unknown",
    ) -> DataVisualizationResult:
        question = question.strip()
        if not question:
            raise ValueError("The question must not be empty")
        if len(prepared.content) > MAX_MODEL_DATA_BYTES:
            raise DataVisualizationError(
                "The normalized dataset is too large for visualization"
            )
        started_at = time.monotonic()
        recorder = LlmCallRecorder(self._llm_client, request_id)

        session_id = f"visualization-{uuid4().hex}"
        try:
            self._upload_dataset(session_id, prepared)
            calculation_result = self._calculate(
                session_id,
                question,
                prepared,
                recorder,
            )
            generated, visualizations = self._generate_visualizations(
                question,
                prepared,
                calculation_result,
                recorder,
            )
        finally:
            try:
                self._sessions_client.delete_session(session_id)
            except Exception:
                logger.exception("data_visualization_session_cleanup_error")
        return DataVisualizationResult(
            answer=generated.answer,
            visualizations=visualizations,
            elapsed_seconds=time.monotonic() - started_at,
            llm_calls=recorder.calls,
            usage=tuple(recorder.usages),
        )

    def _generate_visualizations(
        self,
        question: str,
        prepared: PreparedDataVisualization,
        calculation_result: str,
        recorder: LlmCallRecorder,
    ) -> tuple[
        GeneratedVisualizationResponse,
        tuple[DataVisualization, ...],
    ]:
        system_message = {
            "role": "system",
            "content": _visualization_system_prompt(prepared),
        }
        messages: list[dict[str, Any]] = [
            system_message,
            {
                "role": "user",
                "content": (
                    f"Question : {question}\n"
                    "Contexte des données :\n"
                    f"{json.dumps(prepared.descriptor, ensure_ascii=False)}\n"
                    "Résultat calculé par Python :\n"
                    f"{calculation_result}"
                ),
            },
        ]
        for attempt in range(1, MAX_VISUALIZATION_ATTEMPTS + 1):
            completion = recorder.complete(
                messages,
                response_format=VISUALIZATION_RESPONSE_FORMAT,
            )
            try:
                generated = _generated_response(completion.message)
                return generated, tuple(generated.visualizations)
            except DataVisualizationError as error:
                logger.info(
                    "data_visualization_validation_failed attempt=%s error=%r",
                    attempt,
                    str(error),
                )
                if attempt == MAX_VISUALIZATION_ATTEMPTS:
                    raise
                content = completion.message.get("content")
                if not isinstance(content, str) or not content.strip():
                    raise
                messages = [
                    system_message,
                    {"role": "assistant", "content": content},
                    {
                        "role": "user",
                        "content": (
                            "La visualisation précédente est invalide : "
                            f"{error}. Corrige uniquement la réponse JSON ci-dessus "
                            "en conservant exactement ses valeurs et libellés."
                        ),
                    },
                ]
        raise DataVisualizationError("The model returned no visualization")

    def _upload_dataset(
        self,
        session_id: str,
        prepared: PreparedDataVisualization,
    ) -> None:
        try:
            self._sessions_client.upload_file(
                session_id,
                prepared.filename,
                prepared.content,
            )
        except Exception as error:
            raise DataVisualizationError(
                "The Python execution session could not be prepared"
            ) from error

    def _calculate(
        self,
        session_id: str,
        question: str,
        prepared: PreparedDataVisualization,
        recorder: LlmCallRecorder,
    ) -> str:
        data_directory = self._sessions_client.data_directory.rstrip("/")
        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": _calculation_system_prompt(data_directory, prepared),
            },
            {
                "role": "user",
                "content": (
                    f"Question : {question}\n"
                    "Descripteur des données :\n"
                    f"{json.dumps(prepared.descriptor, ensure_ascii=False)}"
                ),
            },
        ]
        tool_choice = {
            "type": "function",
            "function": {"name": PYTHON_TOOL_NAME},
        }
        for attempt in range(1, MAX_PYTHON_EXECUTIONS + 1):
            completion = recorder.complete(
                messages,
                tools=[PYTHON_TOOL],
                tool_choice=tool_choice,
            )
            call_id, code, tool_call = _python_call(completion.message)
            messages.append(
                {
                    "role": "assistant",
                    "content": completion.message.get("content"),
                    "tool_calls": [tool_call],
                }
            )
            try:
                execution = self._sessions_client.execute(
                    session_id,
                    _append_result_persistence(code, data_directory),
                )
            except Exception as error:
                raise DataVisualizationError(
                    "The Python calculation could not be executed"
                ) from error
            tool_output = execution.tool_output()
            calculation_result: str | None = None
            try:
                calculation_result = self._calculation_result(
                    session_id,
                    execution,
                )
            except CalculationResultError as error:
                tool_output["result_file_error"] = str(error)
                logger.info(
                    "data_visualization_python_execution_rejected "
                    "attempt=%s status=%s error=%r",
                    attempt,
                    execution.status,
                    str(error),
                )
            else:
                tool_output["result_file"] = CALCULATION_RESULT_FILENAME
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": json.dumps(
                        tool_output,
                        ensure_ascii=False,
                        default=str,
                    ),
                }
            )
            if calculation_result is not None:
                return calculation_result
        raise DataVisualizationError(
            "The model could not produce a valid Python calculation result"
        )

    def _calculation_result(
        self,
        session_id: str,
        execution: PythonExecutionResult,
    ) -> str:
        if execution.status != "Succeeded":
            raise CalculationResultError("Python execution failed")
        result_file = next(
            (
                file
                for file in self._sessions_client.list_files(session_id)
                if file.name == CALCULATION_RESULT_FILENAME
            ),
            None,
        )
        if result_file is None:
            raise CalculationResultError(
                f"{CALCULATION_RESULT_FILENAME} was not created"
            )
        if result_file.size_in_bytes > MAX_CALCULATION_RESULT_BYTES:
            raise CalculationResultError(f"{CALCULATION_RESULT_FILENAME} is too large")
        content = self._sessions_client.download_file(
            session_id,
            CALCULATION_RESULT_FILENAME,
        )
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError as error:
            raise CalculationResultError(
                f"{CALCULATION_RESULT_FILENAME} is invalid: {error}"
            ) from error


def _calculation_system_prompt(
    data_directory: str,
    prepared: PreparedDataVisualization,
) -> str:
    return f"""
Tu prépares par Python les données nécessaires pour répondre à une question de
visualisation. La session contient uniquement la représentation normalisée des
données dans {data_directory}/{prepared.filename}.

Le code de chaque appel à execute_python doit, dans une seule exécution, ouvrir les
données, effectuer le calcul demandé et définir result. Un appel qui se contente
d'inspecter le fichier, d'afficher son schéma ou de tester son existence sera rejeté.
Si une inspection est nécessaire au calcul, effectue-la dans ce même code puis
poursuis immédiatement jusqu'à la définition de result.

{prepared.calculation_instructions}

Le code doit effectuer tous les calculs demandés et affecter à une variable globale
nommée result un objet Python sérialisable en JSON. Le backend écrit lui-même cette
variable dans {data_directory}/{CALCULATION_RESULT_FILENAME}. result doit contenir
toutes les valeurs et métadonnées nécessaires à la visualisation finale, jamais un
échantillon, une image ou du code ECharts. Affiche seulement un bref résumé avec
print.

Tu peux utiliser la bibliothèque standard, pandas, numpy et openpyxl. N'installe
aucun paquet, n'utilise pas le réseau et ne produis ni image, ni HTML, ni JavaScript.
Si une exécution précédente a échoué ou produit un fichier invalide, corrige le code.
N'invente aucune mesure.
""".strip()


def _visualization_system_prompt(prepared: PreparedDataVisualization) -> str:
    return f"""
Tu construis une réponse et des visualisations à partir de résultats déjà calculés
et validés par une exécution Python.

Réponds exclusivement avec l'objet JSON demandé par le format de réponse structuré.
Produis entre une et huit options ECharts 6 JSON autonomes. Respecte en priorité le
type de graphique explicitement demandé et choisis librement tout type de série
ECharts intégré. Utilise directement les valeurs du résultat Python et ne les
recalcule pas. N'omet aucun point demandé. Rédige answer en texte simple et concis,
sans titre, tableau ou autre syntaxe Markdown.

Copie strictement les identifiants et libellés du résultat Python, caractère par
caractère, même s'ils semblent mal encodés. Ne les corrige pas et ne les traduis pas.

{prepared.visualization_instructions}

Pour toute visualisation cartésienne, active grid.containLabel et réserve des marges
suffisantes. Lorsque des catégories ont des libellés longs, conserve leurs valeurs
complètes dans les données et les infobulles, mais utilise axisLabel.width,
axisLabel.overflow="truncate" et axisLabel.hideOverlap pour éviter qu'elles se
chevauchent.

N'invente aucune mesure et conserve l'unité fournie. Produis uniquement du JSON :
aucune fonction JavaScript, ressource externe, URL, image ou HTML.

N'utilise jamais les champs ECharts interdits suivants : tooltip.formatter,
tooltip.extraCssText, toolbox.feature.dataView.optionToContent,
toolbox.feature.dataView.title, toolbox.feature.dataView.lang,
toolbox.feature.saveAsImage.name, toolbox.feature.saveAsImage.type, title.link,
title.sublink, link dans les données d'une série, dataset.transform.config.reg, ni
une image ou un symbole référençant une ressource externe.
""".strip()


def _python_call(
    message: dict[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    try:
        tool_calls = message["tool_calls"]
        if not isinstance(tool_calls, list) or len(tool_calls) != 1:
            raise ValueError("exactly one Python call is required")
        tool_call = tool_calls[0]
        if not isinstance(tool_call, dict) or tool_call.get("type") != "function":
            raise ValueError("invalid tool call")
        function = tool_call["function"]
        if function["name"] != PYTHON_TOOL_NAME:
            raise ValueError("unexpected tool")
        arguments = function["arguments"]
        payload = json.loads(arguments) if isinstance(arguments, str) else arguments
        request = PythonExecutionRequest.model_validate(payload)
        return str(tool_call["id"]), request.code, tool_call
    except (KeyError, TypeError, ValueError) as error:
        raise DataVisualizationError(
            "The model returned an invalid Python execution request"
        ) from error


def _append_result_persistence(code: str, data_directory: str) -> str:
    result_path = f"{data_directory}/{CALCULATION_RESULT_FILENAME}"
    return (
        f"{code.rstrip()}\n\n"
        "# Persist the model calculation through the backend contract.\n"
        "import json as _euphrosyne_json\n"
        "from pathlib import Path as _EuphrosynePath\n"
        "if not isinstance(result, (dict, list)):\n"
        "    raise TypeError('result must be a dict or list')\n"
        f"_EuphrosynePath({result_path!r}).write_text(\n"
        "    _euphrosyne_json.dumps(\n"
        "        result,\n"
        "        ensure_ascii=False,\n"
        "        allow_nan=False,\n"
        "        separators=(',', ':'),\n"
        "    ),\n"
        "    encoding='utf-8',\n"
        ")\n"
    )


def _generated_response(message: dict[str, Any]) -> GeneratedVisualizationResponse:
    try:
        content = message["content"]
        payload = json.loads(content) if isinstance(content, str) else content
        return GeneratedVisualizationResponse.model_validate(payload)
    except ValidationError as error:
        raise DataVisualizationError(str(error)) from error
    except (KeyError, TypeError, ValueError) as error:
        raise DataVisualizationError(
            "The model returned an invalid JSON visualization response"
        ) from error
