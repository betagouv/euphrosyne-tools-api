from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clients.python_sessions import PythonExecutionResult, PythonSessionsClient
from data_visualization.llm import (
    DataVisualizationCompletion,
    DataVisualizationLlmClient,
)
from data_visualization.models import (
    DataVisualization,
    GeneratedVisualizationResponse,
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
ExchangeLogger = Callable[[dict[str, Any]], None]


class PythonExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    code: str = Field(min_length=1, max_length=50_000)


class DataVisualizationError(RuntimeError):
    """Raised when normalized data or a model response cannot be used."""


@dataclass(frozen=True)
class PreparedDataVisualization:
    filename: str
    content: bytes
    descriptor: dict[str, Any]
    calculation_instructions: str = ""
    visualization_instructions: str = ""


@dataclass(frozen=True)
class DataVisualizationResult:
    answer: str
    visualizations: tuple[DataVisualization, ...]
    elapsed_seconds: float
    llm_calls: int
    usage: tuple[dict[str, Any], ...]


def _inline_json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    definitions = schema.get("$defs", {})

    def expand(value: Any) -> Any:
        if isinstance(value, list):
            return [expand(item) for item in value]
        if not isinstance(value, dict):
            return value
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith("#/$defs/"):
            resolved = expand(definitions[reference.removeprefix("#/$defs/")])
            siblings = {
                key: expand(item) for key, item in value.items() if key != "$ref"
            }
            return {**resolved, **siblings}
        return {key: expand(item) for key, item in value.items() if key != "$defs"}

    return expand(schema)


VISUALIZATION_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": RESPONSE_FORMAT_NAME,
        "description": (
            "Réponse à une question sur des données normalisées avec des "
            "visualisations ECharts autonomes."
        ),
        "strict": True,
        "schema": _inline_json_schema(
            GeneratedVisualizationResponse.model_json_schema()
        ),
    },
}


PYTHON_TOOL = {
    "type": "function",
    "function": {
        "name": PYTHON_TOOL_NAME,
        "description": (
            "Exécute du code Python dans la session contenant les données "
            "normalisées."
        ),
        "strict": True,
        "parameters": _inline_json_schema(PythonExecutionRequest.model_json_schema()),
    },
}


class DataVisualizationService:
    def __init__(
        self,
        llm: DataVisualizationLlmClient,
        sessions: PythonSessionsClient,
    ) -> None:
        self._llm = llm
        self._sessions = sessions

    def run(
        self,
        prepared: PreparedDataVisualization,
        question: str,
        *,
        exchange_logger: ExchangeLogger | None = None,
    ) -> DataVisualizationResult:
        question = question.strip()
        if not question:
            raise ValueError("The question must not be empty")
        if len(prepared.content) > MAX_MODEL_DATA_BYTES:
            raise DataVisualizationError(
                "The normalized dataset is too large for visualization"
            )
        started_at = time.monotonic()
        usages: list[dict[str, Any]] = []
        llm_calls = 0

        def complete(
            messages: list[dict[str, Any]],
            *,
            tools: list[dict[str, Any]] | None = None,
            tool_choice: str | dict[str, Any] = "auto",
            response_format: dict[str, Any] | None = None,
        ) -> DataVisualizationCompletion:
            nonlocal llm_calls
            request_messages = deepcopy(messages)
            request: dict[str, Any] = {
                "call": llm_calls + 1,
                "messages": request_messages,
            }
            if tools is not None:
                request["tools"] = tools
                request["tool_choice"] = tool_choice
            if response_format is not None:
                request["response_format"] = response_format
            _emit_exchange(exchange_logger, "llm_request", **request)
            completion = self._llm.complete(
                request_messages,
                tools,
                tool_choice=tool_choice,
                response_format=response_format,
            )
            llm_calls += 1
            usages.append(completion.usage)
            _emit_exchange(
                exchange_logger,
                "llm_response",
                call=llm_calls,
                message=completion.message,
                usage=completion.usage,
                model=completion.model,
                finish_reason=completion.finish_reason,
            )
            return completion

        session_id = f"visualization-{uuid4().hex}"
        try:
            self._upload_dataset(session_id, prepared)
            calculation_result = self._calculate(
                session_id,
                question,
                prepared,
                complete,
                exchange_logger,
            )
            final_messages = [
                {
                    "role": "system",
                    "content": _visualization_system_prompt(prepared),
                },
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
            generated, visualizations = self._generate_visualizations(
                final_messages,
                complete,
                exchange_logger,
            )
        finally:
            try:
                self._sessions.delete_session(session_id)
            except Exception as error:
                logger.exception("data_visualization_session_cleanup_error")
                _emit_exchange(
                    exchange_logger,
                    "python_session_cleanup_error",
                    error_type=type(error).__name__,
                    error=str(error),
                )
        for index, visualization in enumerate(visualizations, start=1):
            _emit_exchange(
                exchange_logger,
                "visualization_result",
                index=index,
                visualization=visualization.model_dump(mode="json"),
            )
        return DataVisualizationResult(
            answer=generated.answer,
            visualizations=visualizations,
            elapsed_seconds=time.monotonic() - started_at,
            llm_calls=llm_calls,
            usage=tuple(usages),
        )

    def _generate_visualizations(
        self,
        messages: list[dict[str, Any]],
        complete: Callable[..., DataVisualizationCompletion],
        exchange_logger: ExchangeLogger | None,
    ) -> tuple[
        GeneratedVisualizationResponse,
        tuple[DataVisualization, ...],
    ]:
        last_error: DataVisualizationError | None = None
        base_messages = deepcopy(messages)
        retry_messages = deepcopy(messages)
        for attempt in range(1, MAX_VISUALIZATION_ATTEMPTS + 1):
            completion = complete(
                retry_messages,
                response_format=VISUALIZATION_RESPONSE_FORMAT,
            )
            try:
                generated = _generated_response(completion.message)
                return generated, tuple(generated.visualizations)
            except DataVisualizationError as error:
                last_error = error
                _emit_exchange(
                    exchange_logger,
                    "visualization_validation_failed",
                    attempt=attempt,
                    error=str(error),
                )
                if attempt == MAX_VISUALIZATION_ATTEMPTS:
                    break
                correction = _visualization_correction_message(error)
                content = completion.message.get("content")
                retry_messages = deepcopy(base_messages)
                if isinstance(content, str) and content.strip():
                    retry_messages.append({"role": "assistant", "content": content})
                retry_messages.append({"role": "user", "content": correction})
        if last_error is not None:
            raise last_error
        raise DataVisualizationError("The model returned no visualization")

    def _upload_dataset(
        self,
        session_id: str,
        prepared: PreparedDataVisualization,
    ) -> None:
        try:
            self._sessions.upload_file(
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
        complete: Callable[..., DataVisualizationCompletion],
        exchange_logger: ExchangeLogger | None,
    ) -> str:
        data_directory = self._sessions.data_directory.rstrip("/")
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
            completion = complete(
                messages,
                tools=[PYTHON_TOOL],
                tool_choice=tool_choice,
            )
            call_id, code, tool_call = _python_call(completion.message)
            messages.append(_assistant_tool_message(completion.message, tool_call))
            try:
                execution = self._sessions.execute(
                    session_id,
                    _instrument_python(code, data_directory),
                )
            except Exception as error:
                raise DataVisualizationError(
                    "The Python calculation could not be executed"
                ) from error
            calculation_result, result_error = self._calculation_result(
                session_id,
                execution,
            )
            tool_output = execution.tool_output()
            if result_error is not None:
                tool_output["result_file_error"] = result_error
            else:
                tool_output["result_file"] = CALCULATION_RESULT_FILENAME
            messages.append(_tool_message(call_id, tool_output))
            _emit_exchange(
                exchange_logger,
                "python_execution",
                attempt=attempt,
                code=code,
                output=tool_output,
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
    ) -> tuple[str | None, str | None]:
        if execution.status != "Succeeded":
            return None, "Python execution failed"
        try:
            result_file = next(
                (
                    file
                    for file in self._sessions.list_files(session_id)
                    if file.get("name") == CALCULATION_RESULT_FILENAME
                ),
                None,
            )
            if result_file is None:
                return None, f"{CALCULATION_RESULT_FILENAME} was not created"
            size = result_file.get("sizeInBytes")
            if isinstance(size, int) and size > MAX_CALCULATION_RESULT_BYTES:
                return None, f"{CALCULATION_RESULT_FILENAME} is too large"
            content = self._sessions.download_file(
                session_id,
                CALCULATION_RESULT_FILENAME,
            )
            if len(content) > MAX_CALCULATION_RESULT_BYTES:
                return None, f"{CALCULATION_RESULT_FILENAME} is too large"
            payload = json.loads(content)
            if not isinstance(payload, (dict, list)):
                return None, f"{CALCULATION_RESULT_FILENAME} must contain an object"
            return (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                ),
                None,
            )
        except (OSError, TypeError, ValueError) as error:
            return None, f"{CALCULATION_RESULT_FILENAME} is invalid: {error}"


def _calculation_system_prompt(
    data_directory: str,
    prepared: PreparedDataVisualization,
) -> str:
    return f"""
Tu prépares par Python les données nécessaires pour répondre à une question de
visualisation. La session contient uniquement la représentation normalisée des
données dans {data_directory}/{prepared.filename}.

Appelle execute_python avec un calcul complet dès le premier appel. Chaque appel doit
définir result : n'utilise jamais l'outil seulement pour explorer ou afficher le
schéma.

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


def _instrument_python(code: str, data_directory: str) -> str:
    result_path = f"{data_directory}/{CALCULATION_RESULT_FILENAME}"
    return (
        f"{code.rstrip()}\n\n"
        "# Persist the model calculation through the backend contract.\n"
        "import json as _euphrosyne_json\n"
        "from pathlib import Path as _EuphrosynePath\n"
        f"_EuphrosynePath({result_path!r}).write_text(\n"
        "    _euphrosyne_json.dumps(result, ensure_ascii=False, allow_nan=False),\n"
        "    encoding='utf-8',\n"
        ")\n"
    )


def _assistant_tool_message(
    message: dict[str, Any],
    tool_call: dict[str, Any],
) -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": message.get("content"),
        "tool_calls": [tool_call],
    }


def _tool_message(call_id: str, content: dict[str, Any]) -> dict[str, Any]:
    return {
        "role": "tool",
        "tool_call_id": call_id,
        "content": json.dumps(content, ensure_ascii=False, default=str),
    }


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


def _visualization_correction_message(error: DataVisualizationError) -> str:
    return (
        "La visualisation précédente est invalide : "
        f"{error}. Corrige uniquement le JSON ECharts en conservant exactement "
        "les valeurs et libellés du résultat Python."
    )


def _emit_exchange(
    exchange_logger: ExchangeLogger | None,
    event: str,
    **details: Any,
) -> None:
    if exchange_logger is not None:
        exchange_logger(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event,
                **details,
            }
        )
