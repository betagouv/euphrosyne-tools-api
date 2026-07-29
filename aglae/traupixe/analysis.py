from __future__ import annotations

import json
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clients.albert import AlbertClient, AlbertCompletion
from clients.local_python import PythonExecutionResult

from .dataset import (
    TraupixeDataset,
    load_traupixe_dataset,
    serialize_traupixe_for_model,
)

RESPONSE_FORMAT_NAME = "traupixe_visualizations"
PYTHON_TOOL_NAME = "execute_python"
WORKBOOK_FILENAME = "TRAUPIXE.xlsx"
DATASET_FILENAME = "traupixe_data.json"
CALCULATION_RESULT_FILENAME = "analysis_result.json"
MAX_VISUALIZATIONS = 8
MAX_PYTHON_EXECUTIONS = 6
MAX_VISUALIZATION_ATTEMPTS = 3
MAX_MODEL_DATA_BYTES = 1_000_000
MAX_CALCULATION_RESULT_BYTES = 1_000_000
MAX_GENERATED_OPTION_BYTES = 200_000
ExchangeLogger = Callable[[dict[str, Any]], None]


class PythonSessionsClient(Protocol):
    data_directory: str

    def upload_file(self, session_id: str, filename: str, content: bytes) -> None: ...

    def execute(self, session_id: str, code: str) -> PythonExecutionResult: ...

    def list_files(self, session_id: str) -> list[dict[str, Any]]: ...

    def download_file(self, session_id: str, filename: str) -> bytes: ...

    def delete_session(self, session_id: str) -> None: ...


class PythonExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    code: str = Field(min_length=1, max_length=50_000)


class TraupixeVisualization(BaseModel):
    """A self-contained ECharts option generated from TRAUPIXE data."""

    model_config = ConfigDict(extra="forbid", strict=True)

    title: str = Field(min_length=1, max_length=200)
    option: dict[str, Any]

    @model_validator(mode="after")
    def validate_option(self) -> "TraupixeVisualization":
        series = self.option.get("series")
        if isinstance(series, dict):
            series_count = 1
        elif isinstance(series, list):
            series_count = len(series)
        else:
            series_count = 0
        if series_count == 0:
            raise ValueError("an ECharts option requires at least one series")
        if series_count > 100:
            raise ValueError("an ECharts option contains too many series")
        try:
            encoded = json.dumps(self.option, allow_nan=False)
        except (TypeError, ValueError) as error:
            raise ValueError("the ECharts option must be finite JSON") from error
        if len(encoded.encode("utf-8")) > MAX_GENERATED_OPTION_BYTES:
            raise ValueError("the ECharts option is too large")
        _reject_external_resources(self.option)
        return self


class GeneratedVisualizationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    answer: str = Field(min_length=1, max_length=10_000)
    visualizations: list[TraupixeVisualization] = Field(
        min_length=1,
        max_length=MAX_VISUALIZATIONS,
    )


class TraupixeAnalysisError(RuntimeError):
    """Raised when the normalized dataset or model response cannot be used."""


@dataclass(frozen=True)
class TraupixeAnalysisResult:
    answer: str
    visualizations: tuple[TraupixeVisualization, ...]
    elapsed_seconds: float
    albert_calls: int
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


ALBERT_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": RESPONSE_FORMAT_NAME,
        "description": (
            "Réponse à une question sur des données TRAUPIXE normalisées avec "
            "des visualisations ECharts autonomes."
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
            "Exécute du code Python dans la session contenant le classeur et "
            "les données TRAUPIXE normalisées."
        ),
        "strict": True,
        "parameters": _inline_json_schema(PythonExecutionRequest.model_json_schema()),
    },
}


def _calculation_system_prompt(data_directory: str) -> str:
    return f"""
Tu prépares par Python les données nécessaires pour répondre à une question de
visualisation TRAUPIXE. La session contient le classeur original dans
{data_directory}/{WORKBOOK_FILENAME} et sa représentation normalisée dans
{data_directory}/{DATASET_FILENAME}.

Appelle execute_python avec un calcul complet dès le premier appel. Chaque appel doit
définir result : n'utilise jamais l'outil seulement pour explorer ou afficher le
schéma. Lis en priorité le JSON normalisé, dont le contrat exact est :

- data["analytes"] est une liste d'objets ;
- names = [item["name"] for item in data["analytes"]] donne l'ordre des analytes ;
- chaque objet de data["analyses"] possède identifier, label, zone, kind, puis les
  trois LISTES parallèles values, detected et detectors, dans l'ordre de names ;
- accède donc à un analyte avec index = names.index("Fe2O3"), puis
  analysis["values"][index], jamais analysis["values"]["Fe2O3"] ;
- detected[index] est un booléen : false désigne une limite de détection, pas une
  mesure quantitative.

Utilise par défaut uniquement les analyses dont kind == "object".
Pour une matrice ou une heatmap, produis result en format long : une liste plate
d'objets contenant les coordonnées, la valeur et les libellés de chaque cellule.
Conserve aussi les listes ordonnées des lignes et colonnes. Pour une matrice de
détecteurs, conserve une cellule par couple analyse d'objet × analyte, y compris
lorsque le détecteur est absent ; ne regroupe jamais les analyses par analyte.

Le code doit effectuer tous les calculs demandés et affecter à une variable globale
nommée result un objet Python sérialisable en JSON. Le backend écrit lui-même cette
variable dans {data_directory}/{CALCULATION_RESULT_FILENAME}. result doit contenir
toutes les valeurs et métadonnées nécessaires à la visualisation finale, jamais un
échantillon, une ellipse ou du code ECharts. Affiche seulement un bref résumé avec
print.

Tu peux utiliser la bibliothèque standard, pandas, numpy et openpyxl. N'installe
aucun paquet, n'utilise pas le réseau et ne produis ni image, ni HTML, ni JavaScript.
Si une exécution précédente a échoué ou produit un fichier invalide, corrige le code.
N'invente aucune mesure.
""".strip()


def _visualization_system_prompt() -> str:
    return """
Tu construis une réponse et des visualisations à partir de résultats déjà calculés
et validés par une exécution Python.

Réponds exclusivement avec l'objet JSON demandé par le format de réponse structuré.
Produis entre une et huit options ECharts 6 JSON autonomes. Respecte en priorité le
type de graphique explicitement demandé et choisis librement tout type de série
ECharts intégré. Utilise directement les valeurs du résultat Python et ne les
recalcule pas. N'omet aucun point demandé. Rédige answer en texte simple et concis,
sans titre, tableau ou autre syntaxe Markdown.

Copie strictement les identifiants, labels et zones du résultat Python, caractère par
caractère, même s'ils semblent mal encodés. Ne les corrige pas et ne les traduis pas.
Pour une matrice de détecteurs TRAUPIXE, les deux axes sont les analyses d'objet et
les analytes ; le détecteur est la valeur de chaque cellule, jamais un axe. Conserve
une cellule par couple analyse × analyte, y compris les détecteurs absents. Place les
analytes, dont les libellés sont courts, sur l'axe horizontal et les analyses sur
l'axe vertical. Conserve les libellés complets dans les données et dans les noms des
points pour qu'ils restent accessibles dans les infobulles. Encode les détecteurs
comme valeurs numériques et restitue leurs libellés avec visualMap.

Pour toute visualisation cartésienne, active grid.containLabel et réserve des marges
suffisantes. Lorsque des catégories ont des libellés longs, conserve leurs valeurs
complètes dans les données et les infobulles, mais utilise axisLabel.width,
axisLabel.overflow="truncate" et axisLabel.hideOverlap pour éviter qu'elles se
chevauchent.

N'invente aucune mesure et conserve l'unité fournie. Produis uniquement du JSON :
aucune fonction JavaScript, ressource externe, URL, image ou HTML.
""".strip()


class TraupixeAlbertAnalysis:
    def __init__(
        self,
        albert: AlbertClient,
        sessions: PythonSessionsClient,
    ) -> None:
        self._albert = albert
        self._sessions = sessions

    def run(
        self,
        workbook: bytes,
        question: str,
        *,
        exchange_logger: ExchangeLogger | None = None,
    ) -> TraupixeAnalysisResult:
        if not question.strip():
            raise ValueError("The question must not be empty")
        started_at = time.monotonic()
        dataset = load_traupixe_dataset(workbook)
        encoded_dataset = json.dumps(
            serialize_traupixe_for_model(dataset),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        if len(encoded_dataset.encode("utf-8")) > MAX_MODEL_DATA_BYTES:
            raise TraupixeAnalysisError(
                "The normalized TRAUPIXE dataset is too large for visualization"
            )
        usages: list[dict[str, Any]] = []
        albert_calls = 0

        def complete(
            messages: list[dict[str, Any]],
            *,
            tools: list[dict[str, Any]] | None = None,
            tool_choice: str | dict[str, Any] = "auto",
            response_format: dict[str, Any] | None = None,
        ) -> AlbertCompletion:
            nonlocal albert_calls
            request_messages = deepcopy(messages)
            request: dict[str, Any] = {
                "call": albert_calls + 1,
                "messages": request_messages,
            }
            if tools is not None:
                request["tools"] = tools
                request["tool_choice"] = tool_choice
            if response_format is not None:
                request["response_format"] = response_format
            _emit_exchange(exchange_logger, "albert_request", **request)
            completion = self._albert.complete(
                request_messages,
                tools,
                tool_choice=tool_choice,
                response_format=response_format,
            )
            albert_calls += 1
            usages.append(completion.usage)
            _emit_exchange(
                exchange_logger,
                "albert_response",
                call=albert_calls,
                message=completion.message,
                usage=completion.usage,
                model=completion.model,
                finish_reason=completion.finish_reason,
            )
            return completion

        session_id = f"traupixe-{uuid4().hex}"
        try:
            self._upload_session_files(session_id, workbook, encoded_dataset)
            calculation_result = self._calculate(
                session_id,
                question.strip(),
                dataset,
                complete,
                exchange_logger,
            )
            final_messages = [
                {"role": "system", "content": _visualization_system_prompt()},
                {
                    "role": "user",
                    "content": (
                        f"Question : {question.strip()}\n"
                        "Contexte TRAUPIXE :\n"
                        f"{json.dumps(dataset.descriptor(), ensure_ascii=False)}\n"
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
        return TraupixeAnalysisResult(
            answer=generated.answer,
            visualizations=visualizations,
            elapsed_seconds=time.monotonic() - started_at,
            albert_calls=albert_calls,
            usage=tuple(usages),
        )

    def _generate_visualizations(
        self,
        messages: list[dict[str, Any]],
        complete: Callable[..., AlbertCompletion],
        exchange_logger: ExchangeLogger | None,
    ) -> tuple[
        GeneratedVisualizationResponse,
        tuple[TraupixeVisualization, ...],
    ]:
        last_error: TraupixeAnalysisError | None = None
        base_messages = deepcopy(messages)
        retry_messages = deepcopy(messages)
        for attempt in range(1, MAX_VISUALIZATION_ATTEMPTS + 1):
            completion = complete(
                retry_messages,
                response_format=ALBERT_RESPONSE_FORMAT,
            )
            try:
                generated = _generated_response(completion.message)
                visualizations = tuple(generated.visualizations)
                return generated, visualizations
            except TraupixeAnalysisError as error:
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
                if isinstance(content, str) and content.strip():
                    retry_messages = [
                        *deepcopy(base_messages),
                        {
                            "role": "assistant",
                            "content": content,
                        },
                        {
                            "role": "user",
                            "content": correction,
                        },
                    ]
                elif retry_messages == base_messages:
                    retry_messages = [
                        *deepcopy(base_messages),
                        {
                            "role": "user",
                            "content": correction,
                        },
                    ]
        if last_error is not None:
            raise last_error
        raise TraupixeAnalysisError("Albert returned no visualization")

    def _upload_session_files(
        self,
        session_id: str,
        workbook: bytes,
        encoded_dataset: str,
    ) -> None:
        try:
            self._sessions.upload_file(session_id, WORKBOOK_FILENAME, workbook)
            self._sessions.upload_file(
                session_id,
                DATASET_FILENAME,
                encoded_dataset.encode("utf-8"),
            )
        except Exception as error:
            raise TraupixeAnalysisError(
                "The Python execution session could not be prepared"
            ) from error

    def _calculate(
        self,
        session_id: str,
        question: str,
        dataset: TraupixeDataset,
        complete: Callable[..., AlbertCompletion],
        exchange_logger: ExchangeLogger | None,
    ) -> str:
        data_directory = self._sessions.data_directory.rstrip("/")
        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": _calculation_system_prompt(data_directory),
            },
            {
                "role": "user",
                "content": (
                    f"Question : {question}\n"
                    "Descripteur TRAUPIXE :\n"
                    f"{json.dumps(dataset.descriptor(), ensure_ascii=False)}"
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
                raise TraupixeAnalysisError(
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
        raise TraupixeAnalysisError(
            "Albert could not produce a valid Python calculation result"
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
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            )
            if not isinstance(payload, (dict, list)):
                return None, f"{CALCULATION_RESULT_FILENAME} must contain an object"
            return encoded, None
        except (OSError, TypeError, ValueError) as error:
            return None, f"{CALCULATION_RESULT_FILENAME} is invalid: {error}"


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
        raise TraupixeAnalysisError(
            "Albert returned an invalid Python execution request"
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
    except (KeyError, TypeError, ValueError) as error:
        raise TraupixeAnalysisError(
            "Albert returned an invalid JSON visualization response"
        ) from error


def _visualization_correction_message(
    error: TraupixeAnalysisError,
) -> str:
    return (
        "La visualisation précédente est invalide : "
        f"{error}. Corrige uniquement le JSON ECharts en conservant exactement "
        "les valeurs et libellés du résultat Python."
    )


def _reject_external_resources(value: Any) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _reject_external_resources(item)
        return
    if isinstance(value, list):
        for item in value:
            _reject_external_resources(item)
        return
    if isinstance(value, str) and value.lstrip().casefold().startswith(
        ("http://", "https://", "data:", "image://")
    ):
        raise TraupixeAnalysisError(
            "The generated ECharts option references an external resource"
        )


def _emit_exchange(
    logger: ExchangeLogger | None,
    event: str,
    **details: Any,
) -> None:
    if logger is not None:
        logger(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event,
                **details,
            }
        )
