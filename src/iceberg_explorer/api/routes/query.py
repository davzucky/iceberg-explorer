"""Query API routes for executing SQL queries.

Provides endpoints for:
- Executing SQL queries
- Getting query results (streaming)
- Checking query status
- Cancelling queries
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from iceberg_explorer.models.query import (
    ExecuteQueryRequest,
    ExecuteQueryResponse,
)
from iceberg_explorer.query.executor import get_executor, validate_sql
from iceberg_explorer.query.models import InvalidSQLError

router = APIRouter(prefix="/api/v1/query", tags=["query"])


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
        result = executor.execute(request.sql, timeout=request.timeout)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return ExecuteQueryResponse(
        query_id=str(result.query_id),
        status=result.state.value,
    )
