from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated
from uuid import UUID, uuid4

import sentry_sdk
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field

from auth import verify_project_membership
from clients.data_client import AbstractDataClient
from data_visualization.dependencies import get_data_visualization_service
from data_visualization.handlers import (
    DataVisualizationHandler,
    UnsupportedDataVisualizationFile,
    resolve_data_visualization_handler,
)
from data_visualization.models import DataVisualization
from data_visualization.service import DataVisualizationService
from dependencies import get_project_data_client
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
    data_client: Annotated[
        AbstractDataClient,
        Depends(get_project_data_client),
    ],
    visualization_service: Annotated[
        DataVisualizationService,
        Depends(get_data_visualization_service),
    ],
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
    _capture_visualization_usage(project_slug, query, request_id)
    try:
        handler = _resolve_handler(project_slug, query.path)
        data_file = _download_data_file(
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
        prepared = handler.prepare(data_file)
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
        request_id=request_id,
    )
    total_tokens = sum(
        usage.get("total_tokens", 0)
        for usage in result.usage
        if isinstance(usage.get("total_tokens"), int)
    )
    logger.info(
        "data_visualization_completed request_id=%s calls=%s "
        "elapsed_seconds=%.3f total_tokens=%s visualizations=%s",
        request_id,
        result.llm_calls,
        result.elapsed_seconds,
        total_tokens,
        len(result.visualizations),
    )
    return DataVisualizationResponse(
        request_id=request_id,
        answer=result.answer,
        visualizations=list(result.visualizations),
    )


def _capture_visualization_usage(
    project_slug: str,
    query: DataVisualizationQuery,
    request_id: UUID,
) -> None:
    """Send an isolated Sentry message for one authorized user question."""
    file_name = Path(query.path).name
    with sentry_sdk.new_scope() as scope:
        scope.fingerprint = ["data-visualization-question"]
        scope.set_tag("feature", "data_visualization")
        scope.set_tag("data_visualization.project", project_slug)
        scope.set_context(
            "data_visualization_usage",
            {
                "request_id": str(request_id),
                "project": project_slug,
                "file_name": file_name,
                "file_path": query.path,
                "question": query.question,
            },
        )
        sentry_sdk.capture_message(
            f"Data visualization question for {file_name}: {query.question}",
            level="info",
            scope=scope,
        )


def _resolve_handler(
    project_slug: str,
    path: str,
) -> DataVisualizationHandler:
    data_file_path = Path(path)
    if ".." in data_file_path.parts:
        raise DataVisualizationRequestError(INVALID_FILE_PATH, 422)
    try:
        reference = RunDataTypeRef.from_path(data_file_path)
    except IncorrectDataFilePath as error:
        raise DataVisualizationRequestError(INVALID_FILE_PATH, 422) from error
    if reference.project_slug != project_slug:
        raise DataVisualizationRequestError(INVALID_FILE_PATH, 422)
    try:
        return resolve_data_visualization_handler(data_file_path)
    except UnsupportedDataVisualizationFile as error:
        raise DataVisualizationRequestError(UNSUPPORTED_FILE_TYPE, 422) from error


def _download_data_file(
    data_client: AbstractDataClient,
    path: str,
    max_source_size_bytes: int,
) -> bytes:
    with data_client.download_run_file(path) as data_file_handle:
        content_length = getattr(data_file_handle, "content_length", None)
        if (
            isinstance(content_length, int)
            and not isinstance(content_length, bool)
            and content_length > max_source_size_bytes
        ):
            raise DataVisualizationRequestError(FILE_TOO_LARGE, 413)
        data_file = data_file_handle.read(max_source_size_bytes + 1)
    if not isinstance(data_file, bytes) or not data_file:
        raise DataVisualizationRequestError(INVALID_DATA_FILE, 422)
    if len(data_file) > max_source_size_bytes:
        raise DataVisualizationRequestError(FILE_TOO_LARGE, 413)
    return data_file


def _http_error(
    error: DataVisualizationRequestError,
    request_id: UUID,
) -> HTTPException:
    return HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "request_id": str(request_id)},
        headers={"X-Request-ID": str(request_id)},
    )
