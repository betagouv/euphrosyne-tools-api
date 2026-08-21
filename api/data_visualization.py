from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field

from auth import verify_project_membership
from clients.data_client import AbstractDataClient
from data_visualization.exchange_log import (
    is_data_visualization_exchange_logging_enabled,
    write_data_visualization_exchange,
)
from data_visualization.handlers import (
    DataVisualizationHandler,
    UnsupportedDataVisualizationFile,
    resolve_data_visualization_handler,
)
from data_visualization.models import DataVisualization
from data_visualization.service import DataVisualizationService
from dependencies import (
    get_data_visualization_service,
    get_project_data_client,
)
from path import IncorrectDataFilePath, RunDataTypeRef

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data-visualization"])

INVALID_FILE_PATH = "INVALID_FILE_PATH"
UNSUPPORTED_FILE_TYPE = "UNSUPPORTED_FILE_TYPE"
FILE_TOO_LARGE = "FILE_TOO_LARGE"
INVALID_DATA_FILE = "INVALID_DATA_FILE"


class DataVisualizationRequestError(ValueError):
    def __init__(self, code: str, status_code: int) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


class DataVisualizationQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)

    path: str = Field(min_length=1, max_length=2048)
    question: str = Field(min_length=1, max_length=2000)


class DataVisualizationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    request_id: UUID
    answer: str
    visualizations: list[DataVisualization]


@router.post(
    "/{project_slug}/visualizations",
    dependencies=[Depends(verify_project_membership)],
    response_model=DataVisualizationResponse,
)
def create_data_visualization(
    project_slug: str,
    query: DataVisualizationQuery,
    response: Response,
    data_client: AbstractDataClient = Depends(get_project_data_client),
    visualization_service: DataVisualizationService = Depends(
        get_data_visualization_service
    ),
) -> DataVisualizationResponse:
    request_id = uuid4()
    response.headers["X-Request-ID"] = str(request_id)
    logger.info(
        "data_visualization_started request_id=%s project=%s path=%s question=%r",
        request_id,
        project_slug,
        query.path,
        query.question,
    )
    exchange_logger = (
        _exchange_logger(request_id)
        if is_data_visualization_exchange_logging_enabled()
        else None
    )
    _emit_exchange(
        exchange_logger,
        {
            "event": "request_started",
            "project": project_slug,
            "path": query.path,
            "question": query.question,
        },
    )
    try:
        handler = _resolve_handler(project_slug, query.path)
        workbook = _download_workbook(
            data_client,
            query.path,
            handler.max_source_size_bytes,
        )
    except DataVisualizationRequestError as error:
        logger.info(
            "data_visualization_input_error request_id=%s status=%s code=%s",
            request_id,
            error.status_code,
            error.code,
        )
        raise _http_error(error, request_id) from error
    try:
        prepared = handler.prepare(workbook)
    except ValueError as error:
        logger.info(
            "data_visualization_invalid_file request_id=%s error=%s reason=%r",
            request_id,
            type(error).__name__,
            str(error),
        )
        request_error = DataVisualizationRequestError(INVALID_DATA_FILE, 422)
        raise _http_error(request_error, request_id) from error
    result = visualization_service.run(
        prepared,
        query.question,
        exchange_logger=exchange_logger,
    )
    total_tokens = sum(
        usage.get("total_tokens", 0)
        for usage in result.usage
        if isinstance(usage.get("total_tokens"), int)
    )
    _emit_exchange(
        exchange_logger,
        {
            "event": "request_completed",
            "calls": result.llm_calls,
            "elapsed_seconds": result.elapsed_seconds,
            "total_tokens": total_tokens,
        },
    )
    logger.info(
        "data_visualization_completed request_id=%s calls=%s "
        "elapsed_seconds=%.3f total_tokens=%s",
        request_id,
        result.llm_calls,
        result.elapsed_seconds,
        total_tokens,
    )
    return DataVisualizationResponse(
        request_id=request_id,
        answer=result.answer,
        visualizations=list(result.visualizations),
    )


def _resolve_handler(
    project_slug: str,
    path: str,
) -> DataVisualizationHandler:
    workbook_path = Path(path)
    if ".." in workbook_path.parts:
        raise DataVisualizationRequestError(INVALID_FILE_PATH, 422)
    try:
        reference = RunDataTypeRef.from_path(workbook_path)
    except IncorrectDataFilePath as error:
        raise DataVisualizationRequestError(INVALID_FILE_PATH, 422) from error
    if reference.project_slug != project_slug:
        raise DataVisualizationRequestError(INVALID_FILE_PATH, 422)
    try:
        return resolve_data_visualization_handler(workbook_path)
    except UnsupportedDataVisualizationFile as error:
        raise DataVisualizationRequestError(UNSUPPORTED_FILE_TYPE, 422) from error


def _download_workbook(
    data_client: AbstractDataClient,
    path: str,
    max_source_size_bytes: int,
) -> bytes:
    workbook_file: BinaryIO | None = None
    try:
        workbook_file = data_client.download_run_file(path)
        content_length = getattr(workbook_file, "content_length", None)
        if (
            isinstance(content_length, int)
            and not isinstance(content_length, bool)
            and content_length > max_source_size_bytes
        ):
            raise DataVisualizationRequestError(FILE_TOO_LARGE, 413)
        workbook = workbook_file.read(max_source_size_bytes + 1)
    finally:
        if workbook_file is not None:
            workbook_file.close()
    if not isinstance(workbook, bytes) or not workbook:
        raise DataVisualizationRequestError(INVALID_DATA_FILE, 422)
    if len(workbook) > max_source_size_bytes:
        raise DataVisualizationRequestError(FILE_TOO_LARGE, 413)
    return workbook


def _exchange_logger(request_id: UUID) -> Callable[[dict[str, Any]], None]:
    def log_exchange(exchange: dict[str, Any]) -> None:
        try:
            write_data_visualization_exchange(request_id, exchange)
        except Exception:
            logger.warning(
                "data_visualization_exchange_file_error request_id=%s",
                request_id,
                exc_info=True,
            )
        logger.info(
            "data_visualization_exchange request_id=%s event=%s",
            request_id,
            exchange.get("event", "unknown"),
        )

    return log_exchange


def _emit_exchange(
    exchange_logger: Callable[[dict[str, Any]], None] | None,
    exchange: dict[str, Any],
) -> None:
    if exchange_logger is not None:
        exchange_logger(exchange)


def _http_error(
    error: DataVisualizationRequestError,
    request_id: UUID,
) -> HTTPException:
    return HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "request_id": str(request_id)},
        headers={"X-Request-ID": str(request_id)},
    )
