"""Export API routes for downloading query results.

Provides endpoints for:
- CSV export with streaming download
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from collections.abc import AsyncGenerator
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from iceberg_explorer.config import get_settings
from iceberg_explorer.query.executor import get_executor, validate_sql
from iceberg_explorer.query.models import InvalidSQLError, QueryState

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/export", tags=["export"])


class CSVExportRequest(BaseModel):
    """Request to export data as CSV."""

    query_id: str | None = Field(
        default=None,
        description="Query ID from a previously executed query.",
    )
    sql: str | None = Field(
        default=None,
        description="SQL query to execute and export. Only SELECT statements allowed.",
    )
    filename: str | None = Field(
        default=None,
        description="Custom filename for the downloaded file (without extension).",
    )


def _format_value(value: object) -> str:
    """Format a value for CSV output."""
    if value is None:
        return ""
    if hasattr(value, "as_py"):
        value = value.as_py()
        if value is None:
            return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


class CSVExportError(Exception):
    """Error during CSV export streaming."""


async def _stream_csv(
    query_id: UUID,
    max_size_bytes: int,
) -> AsyncGenerator[bytes]:
    """Stream query results as CSV.

    Args:
        query_id: ID of completed query.
        max_size_bytes: Maximum export size in bytes.

    Yields:
        CSV data chunks.

    Raises:
        CSVExportError: If query times out, is cancelled, fails, or not found.
    """
    executor = get_executor()
    result = executor.get_status(query_id)

    if result is None:
        raise CSVExportError(f"Query not found: {query_id}")

    max_wait_seconds = 3600
    waited = 0.0
    while result.state in (QueryState.PENDING, QueryState.RUNNING):
        await asyncio.sleep(0.1)
        waited += 0.1
        if waited >= max_wait_seconds:
            raise CSVExportError(
                f"Query timed out after {max_wait_seconds} seconds waiting for completion"
            )
        result = executor.get_status(query_id)
        if result is None:
            raise CSVExportError(f"Query disappeared while waiting: {query_id}")

    if result.state == QueryState.CANCELLED:
        raise CSVExportError("Query was cancelled")
    if result.state == QueryState.FAILED:
        error_msg = result.error_message if result.error_message else "Unknown error"
        raise CSVExportError(f"Query failed: {error_msg}")
    if result.state != QueryState.COMPLETED:
        raise CSVExportError(f"Query in unexpected state: {result.state}")

    schema = result.schema
    batches = result.batches

    if schema is None:
        return

    column_names = [field.name for field in schema]
    bytes_written = 0

    header_buffer = io.StringIO()
    header_writer = csv.writer(header_buffer)
    header_writer.writerow(column_names)
    header_bytes = header_buffer.getvalue().encode("utf-8")
    bytes_written += len(header_bytes)
    yield header_bytes

    for batch in batches:
        row_buffer = io.StringIO()
        row_writer = csv.writer(row_buffer)

        for row_idx in range(batch.num_rows):
            row = [_format_value(batch.column(col)[row_idx]) for col in range(batch.num_columns)]
            row_writer.writerow(row)

            if row_buffer.tell() >= 8192:
                chunk = row_buffer.getvalue().encode("utf-8")
                bytes_written += len(chunk)
                if bytes_written > max_size_bytes:
                    raise CSVExportError(f"Export size exceeds maximum of {max_size_bytes} bytes")
                yield chunk
                row_buffer = io.StringIO()
                row_writer = csv.writer(row_buffer)

        remaining = row_buffer.getvalue()
        if remaining:
            chunk = remaining.encode("utf-8")
            bytes_written += len(chunk)
            if bytes_written > max_size_bytes:
                raise ValueError(f"Export size exceeds maximum of {max_size_bytes} bytes")
            yield chunk


@router.post("/csv")
async def export_csv(request: CSVExportRequest) -> StreamingResponse:
    """Export query results as CSV.

    Accepts either a query_id from a previously executed query, or inline SQL
    to execute first. Streams the CSV as a chunked download.

    Args:
        request: Export request with query_id or sql.

    Returns:
        StreamingResponse with CSV content.

    Raises:
        HTTPException: 400 for invalid requests, 404 for unknown queries.
    """
    if request.query_id is None and request.sql is None:
        raise HTTPException(
            status_code=400,
            detail="Either query_id or sql must be provided",
        )

    if request.query_id is not None and request.sql is not None:
        raise HTTPException(
            status_code=400,
            detail="Provide either query_id or sql, not both",
        )

    executor = get_executor()
    settings = get_settings()

    if request.sql is not None:
        try:
            validate_sql(request.sql)
        except InvalidSQLError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        try:
            result = await asyncio.to_thread(executor.execute, request.sql)
        except Exception:
            logger.exception("Query execution failed for inline SQL export")
            raise HTTPException(status_code=500, detail="Internal server error") from None

        query_uuid = result.query_id
    else:
        try:
            query_uuid = UUID(request.query_id)  # type: ignore[arg-type]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query ID format: {request.query_id}",
            ) from e

        result = executor.get_status(query_uuid)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Query not found: {request.query_id}",
            )

    filename = request.filename or f"export_{query_uuid}"
    # Sanitize for cross-platform filesystem and HTTP header safety
    for char in '"/<>:\\|?*\x00\n\r':
        filename = filename.replace(char, "_")
    filename = filename[:200]  # Limit length

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}.csv"',
        "Cache-Control": "no-cache",
    }

    return StreamingResponse(
        _stream_csv(query_uuid, settings.export.max_size_bytes),
        media_type="text/csv; charset=utf-8",
        headers=headers,
    )
