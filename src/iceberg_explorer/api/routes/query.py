"""Query API routes for executing SQL queries.

Provides endpoints for:
- Executing SQL queries
- Getting query results (streaming)
- Checking query status
- Cancelling queries
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from iceberg_explorer.models.query import (
    CancelQueryResponse,
    ExecuteQueryRequest,
    ExecuteQueryResponse,
    QueryStatusResponse,
    ResultsBatch,
    ResultsComplete,
    ResultsMetadata,
    ResultsProgress,
)
from iceberg_explorer.query.executor import get_executor, validate_sql
from iceberg_explorer.query.models import InvalidSQLError, QueryState

router = APIRouter(prefix="/api/v1/query", tags=["query"])

VALID_PAGE_SIZES = frozenset({100, 250, 500, 1000})


@router.post("/execute", response_model=ExecuteQueryResponse)
async def execute_query(
    request: ExecuteQueryRequest,
) -> ExecuteQueryResponse:
    """Execute a SQL query.

    Validates the SQL is a SELECT statement, then executes it with the
    configured timeout.

    Args:
        request: Query execution request with SQL and optional timeout.

    Returns:
        ExecuteQueryResponse with query_id for tracking.

    Raises:
        HTTPException: 400 if SQL validation fails.
    """
    try:
        validate_sql(request.sql)
    except InvalidSQLError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    executor = get_executor()

    try:
        result = await asyncio.to_thread(executor.execute, request.sql, request.timeout)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return ExecuteQueryResponse(
        query_id=str(result.query_id),
        status=result.state.value,
    )


def _convert_value(value: object) -> object:
    """Convert Arrow values to JSON-serializable Python types."""
    if value is None:
        return None
    if hasattr(value, "as_py"):
        return value.as_py()
    return value


async def _stream_results(
    query_id: str,
    page_size: int,
    offset: int,
) -> AsyncGenerator[str, None]:
    """Stream query results as JSON lines.

    Format:
    1. Metadata message (columns, total rows)
    2. Data batches (rows in chunks)
    3. Progress updates (after each batch)
    4. Complete message (execution metrics)
    """
    executor = get_executor()

    try:
        result_uuid = UUID(query_id)
    except ValueError:
        error = {"type": "error", "error": f"Invalid query ID format: {query_id}"}
        yield json.dumps(error) + "\n"
        return

    result = executor.get_status(result_uuid)
    if result is None:
        error = {"type": "error", "error": f"Query not found: {query_id}"}
        yield json.dumps(error) + "\n"
        return

    if result.state not in (QueryState.COMPLETED, QueryState.RUNNING):
        error = {
            "type": "error",
            "error": f"Query not ready: {result.state.value}",
            "status": result.state.value,
        }
        if result.error_message:
            error["error"] = result.error_message
        yield json.dumps(error) + "\n"
        return

    max_wait_seconds = 3600  # 1 hour max wait
    waited = 0.0
    while result.state == QueryState.RUNNING:
        await asyncio.sleep(0.1)
        waited += 0.1
        if waited >= max_wait_seconds:
            error = {"type": "error", "error": "Timeout waiting for query completion"}
            yield json.dumps(error) + "\n"
            return
        result = executor.get_status(result_uuid)
        if result is None:
            error = {"type": "error", "error": "Query result was removed"}
            yield json.dumps(error) + "\n"
            return

    if result.state != QueryState.COMPLETED:
        error = {
            "type": "error",
            "error": result.error_message or f"Query {result.state.value}",
            "status": result.state.value,
        }
        yield json.dumps(error) + "\n"
        return

    schema = result.schema
    batches = result.batches

    if schema is None:
        columns: list[dict] = []
    else:
        columns = [{"name": field.name, "type": str(field.type)} for field in schema]

    total_rows = sum(batch.num_rows for batch in batches)

    metadata = ResultsMetadata(
        query_id=query_id,
        columns=columns,
        total_rows=total_rows,
    )
    yield metadata.model_dump_json() + "\n"

    rows_sent = 0
    batch_index = 0
    current_offset = offset
    rows_to_send: list[list] = []
    batch_size = 100

    for batch in batches:
        batch_rows = batch.num_rows

        if current_offset >= batch_rows:
            current_offset -= batch_rows
            continue

        start_idx = current_offset
        current_offset = 0

        for row_idx in range(start_idx, batch_rows):
            if rows_sent >= page_size:
                break

            row = [_convert_value(batch.column(col)[row_idx]) for col in range(batch.num_columns)]
            rows_to_send.append(row)
            rows_sent += 1

            if len(rows_to_send) >= batch_size or rows_sent >= page_size:
                data_batch = ResultsBatch(
                    rows=rows_to_send,
                    batch_index=batch_index,
                )
                yield data_batch.model_dump_json() + "\n"
                batch_index += 1
                rows_to_send = []

                progress = ResultsProgress(
                    rows_sent=rows_sent + offset,
                    total_rows=total_rows,
                )
                yield progress.model_dump_json() + "\n"

        if rows_sent >= page_size:
            break

    if len(rows_to_send) > 0:
        data_batch = ResultsBatch(
            rows=rows_to_send,
            batch_index=batch_index,
        )
        yield data_batch.model_dump_json() + "\n"

        progress = ResultsProgress(
            rows_sent=rows_sent + offset,
            total_rows=total_rows,
        )
        yield progress.model_dump_json() + "\n"

    complete = ResultsComplete(
        query_id=query_id,
        rows_returned=rows_sent,
        duration_seconds=result.metrics.duration_seconds,
    )
    yield complete.model_dump_json() + "\n"


@router.get("/{query_id}/results")
async def get_results(
    query_id: str,
    page_size: int = Query(default=100, description="Number of rows per page"),
    offset: int = Query(default=0, ge=0, description="Row offset for pagination"),
) -> StreamingResponse:
    """Stream query results as JSON lines.

    Returns a streaming response with JSON lines containing:
    - metadata: Column definitions and total row count
    - data: Row batches
    - progress: Progress updates after each batch
    - complete: Final message with execution metrics

    Args:
        query_id: Query identifier from execute endpoint.
        page_size: Number of rows to return (100, 250, 500, 1000).
        offset: Row offset for pagination.

    Returns:
        StreamingResponse with JSON lines content.
    """
    if page_size not in VALID_PAGE_SIZES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid page_size. Must be one of: {sorted(VALID_PAGE_SIZES)}",
        )

    return StreamingResponse(
        _stream_results(query_id, page_size, offset),
        media_type="application/x-ndjson",
    )


@router.get("/{query_id}/status", response_model=QueryStatusResponse)
async def get_query_status(query_id: str) -> QueryStatusResponse:
    """Get the status of a query.

    Returns the current state of the query along with progress information
    for running queries and error messages for failed queries.

    Args:
        query_id: Query identifier from execute endpoint.

    Returns:
        QueryStatusResponse with current state and relevant info.

    Raises:
        HTTPException: 404 if query not found, 400 if invalid query ID.
    """
    try:
        result_uuid = UUID(query_id)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid query ID format: {query_id}",
        ) from e

    executor = get_executor()
    result = executor.get_status(result_uuid)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Query not found: {query_id}",
        )

    rows_processed: int | None = None
    if result.state in (QueryState.RUNNING, QueryState.COMPLETED):
        rows_processed = result.metrics.rows_returned

    return QueryStatusResponse(
        query_id=query_id,
        status=result.state.value,
        rows_processed=rows_processed,
        error_message=result.error_message,
    )


@router.post("/{query_id}/cancel", response_model=CancelQueryResponse)
async def cancel_query(query_id: str) -> CancelQueryResponse:
    """Cancel a running query.

    Attempts to cancel the specified query. Returns success even if
    the query has already finished (completed, failed, or cancelled).

    Args:
        query_id: Query identifier from execute endpoint.

    Returns:
        CancelQueryResponse with cancellation result.

    Raises:
        HTTPException: 404 if query not found, 400 if invalid query ID.
    """
    try:
        result_uuid = UUID(query_id)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid query ID format: {query_id}",
        ) from e

    executor = get_executor()

    result = executor.get_status(result_uuid)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Query not found: {query_id}",
        )

    was_cancelled = executor.cancel(result_uuid)

    result = executor.get_status(result_uuid)
    current_status = result.state.value if result else "unknown"

    return CancelQueryResponse(
        query_id=query_id,
        cancelled=was_cancelled,
        status=current_status,
    )
