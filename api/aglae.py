from __future__ import annotations

import logging
import traceback
from pathlib import Path
from typing import Any, BinaryIO, Callable
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field

from aglae.exchange_log import write_albert_exchange
from aglae.traupixe.analysis import (
    PythonSessionsClient,
    TraupixeAlbertAnalysis,
    TraupixeAnalysisError,
    TraupixeVisualization,
)
from aglae.traupixe.selection import (
    MAX_SOURCE_SIZE_BYTES,
    TRAUPIXE_EXTENSION,
    TRAUPIXE_NAME_MARKER,
)
from auth import verify_project_membership
from clients.albert import AlbertAPIError, AlbertClient
from clients.data_client import AbstractDataClient
from dependencies import (
    get_project_data_client,
    get_traupixe_llm_client,
    get_traupixe_python_sessions_client,
)
from path import IncorrectDataFilePath, RunDataTypeRef

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/aglae", tags=["aglae"])

USER_ERROR = "Impossible de traiter cette demande. Veuillez réessayer."
WORKBOOK_ERROR = "Le classeur TRAUPIXE n'a pas pu être interprété."


class TraupixeVisualizationQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)

    path: str = Field(min_length=1, max_length=2048)
    question: str = Field(min_length=1, max_length=2000)


class TraupixeVisualizationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    request_id: UUID
    answer: str
    visualizations: list[TraupixeVisualization]


@router.post(
    "/{project_slug}/visualizations",
    dependencies=[Depends(verify_project_membership)],
    response_model=TraupixeVisualizationResponse,
)
def create_traupixe_visualization(
    project_slug: str,
    query: TraupixeVisualizationQuery,
    response: Response,
    data_client: AbstractDataClient = Depends(get_project_data_client),
    llm_client: AlbertClient = Depends(get_traupixe_llm_client),
    python_sessions: PythonSessionsClient = Depends(
        get_traupixe_python_sessions_client
    ),
) -> TraupixeVisualizationResponse:
    request_id = uuid4()
    response.headers["X-Request-ID"] = str(request_id)
    logger.info(
        "traupixe_visualization_started request_id=%s project=%s path=%s",
        request_id,
        project_slug,
        query.path,
    )
    exchange_logger = _exchange_logger(request_id)
    exchange_logger(
        {
            "event": "request_started",
            "project": project_slug,
            "path": query.path,
            "question": query.question,
        }
    )
    try:
        _validate_traupixe_path(project_slug, query.path)
        workbook = _download_workbook(data_client, query.path)
    except HTTPException as error:
        exchange_logger(
            {
                "event": "request_input_error",
                "status_code": error.status_code,
                "detail": error.detail,
            }
        )
        error.headers = {
            **(error.headers or {}),
            "X-Request-ID": str(request_id),
        }
        logger.info(
            "traupixe_visualization_input_error request_id=%s status=%s",
            request_id,
            error.status_code,
        )
        raise
    try:
        result = TraupixeAlbertAnalysis(llm_client, python_sessions).run(
            workbook,
            query.question,
            exchange_logger=exchange_logger,
        )
    except httpx.TimeoutException as error:
        _log_exchange_failure(exchange_logger, "request_timeout", error)
        logger.warning(
            "traupixe_visualization_timeout request_id=%s",
            request_id,
        )
        raise HTTPException(
            status_code=504,
            detail=USER_ERROR,
            headers={"X-Request-ID": str(request_id)},
        ) from error
    except (AlbertAPIError, httpx.HTTPError) as error:
        _log_exchange_failure(exchange_logger, "llm_error", error)
        logger.warning(
            "traupixe_visualization_llm_error request_id=%s error=%s",
            request_id,
            type(error).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail=USER_ERROR,
            headers={"X-Request-ID": str(request_id)},
        ) from error
    except (TraupixeAnalysisError, OSError, ValueError) as error:
        _log_exchange_failure(exchange_logger, "analysis_rejected", error)
        public_reason = (
            str(error) if isinstance(error, TraupixeAnalysisError) else WORKBOOK_ERROR
        )
        logger.info(
            "traupixe_visualization_rejected request_id=%s error=%s reason=%r",
            request_id,
            type(error).__name__,
            str(error),
        )
        raise HTTPException(
            status_code=422,
            detail={
                "message": USER_ERROR,
                "reason": public_reason,
                "request_id": str(request_id),
            },
            headers={"X-Request-ID": str(request_id)},
        ) from error
    except Exception as error:
        _log_exchange_failure(exchange_logger, "unexpected_error", error)
        logger.exception(
            "traupixe_visualization_unexpected_error request_id=%s",
            request_id,
        )
        raise HTTPException(
            status_code=500,
            detail=USER_ERROR,
            headers={"X-Request-ID": str(request_id)},
        ) from error
    total_tokens = sum(
        usage.get("total_tokens", 0)
        for usage in result.usage
        if isinstance(usage.get("total_tokens"), int)
    )
    exchange_logger(
        {
            "event": "request_completed",
            "calls": result.albert_calls,
            "elapsed_seconds": result.elapsed_seconds,
            "total_tokens": total_tokens,
        }
    )
    logger.info(
        "traupixe_visualization_completed request_id=%s calls=%s "
        "elapsed_seconds=%.3f total_tokens=%s",
        request_id,
        result.albert_calls,
        result.elapsed_seconds,
        total_tokens,
    )
    return TraupixeVisualizationResponse(
        request_id=request_id,
        answer=result.answer,
        visualizations=list(result.visualizations),
    )


def _validate_traupixe_path(project_slug: str, path: str) -> None:
    workbook_path = Path(path)
    if ".." in workbook_path.parts:
        raise HTTPException(status_code=422, detail="Chemin de fichier invalide.")
    try:
        reference = RunDataTypeRef.from_path(workbook_path)
    except IncorrectDataFilePath as error:
        raise HTTPException(
            status_code=422, detail="Chemin de fichier invalide."
        ) from error
    if reference.project_slug != project_slug:
        raise HTTPException(status_code=422, detail="Chemin de fichier invalide.")
    if (
        workbook_path.suffix.casefold() != TRAUPIXE_EXTENSION
        or TRAUPIXE_NAME_MARKER not in workbook_path.name.casefold()
    ):
        raise HTTPException(
            status_code=422,
            detail="Le fichier sélectionné doit être un classeur TRAUPIXE.",
        )


def _download_workbook(data_client: AbstractDataClient, path: str) -> bytes:
    workbook_file: BinaryIO | None = None
    try:
        workbook_file = data_client.download_run_file(path)
        content_length = getattr(workbook_file, "content_length", None)
        if (
            isinstance(content_length, int)
            and not isinstance(content_length, bool)
            and content_length > MAX_SOURCE_SIZE_BYTES
        ):
            raise HTTPException(
                status_code=413,
                detail="Le classeur TRAUPIXE dépasse la taille autorisée.",
            )
        workbook = workbook_file.read(MAX_SOURCE_SIZE_BYTES + 1)
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=502, detail=USER_ERROR) from error
    finally:
        if workbook_file is not None:
            workbook_file.close()
    if not isinstance(workbook, bytes) or not workbook:
        raise HTTPException(status_code=422, detail=USER_ERROR)
    if len(workbook) > MAX_SOURCE_SIZE_BYTES:
        raise HTTPException(
            status_code=413,
            detail="Le classeur TRAUPIXE dépasse la taille autorisée.",
        )
    return workbook


def _exchange_logger(request_id: UUID) -> Callable[[dict[str, Any]], None]:
    def log_exchange(exchange: dict[str, Any]) -> None:
        try:
            write_albert_exchange(request_id, exchange)
        except Exception:
            logger.warning(
                "traupixe_visualization_exchange_file_error request_id=%s",
                request_id,
                exc_info=True,
            )
        logger.info(
            "traupixe_visualization_exchange request_id=%s event=%s",
            request_id,
            exchange.get("event", "unknown"),
        )

    return log_exchange


def _log_exchange_failure(
    exchange_logger: Callable[[dict[str, Any]], None],
    event: str,
    error: BaseException,
) -> None:
    exchange_logger(
        {
            "event": event,
            "error_type": type(error).__name__,
            "error": str(error),
            "traceback": "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
        }
    )
