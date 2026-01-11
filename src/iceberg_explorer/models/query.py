"""API models for query execution.

Provides Pydantic models for:
- Query execution requests
- Query execution responses
- Query status responses
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ExecuteQueryRequest(BaseModel):
    """Request to execute a SQL query."""

    sql: str = Field(
        ...,
        description="SQL query to execute. Must be a SELECT statement.",
        min_length=1,
    )
    timeout: int | None = Field(
        default=None,
        description="Query timeout in seconds (10-3600). Defaults to 300s.",
        ge=10,
        le=3600,
    )


class ExecuteQueryResponse(BaseModel):
    """Response after submitting a query for execution."""

    query_id: str = Field(
        ...,
        description="Unique identifier for tracking the query.",
    )
    status: str = Field(
        ...,
        description="Current query status: pending, running, completed, failed, cancelled.",
    )


class QueryErrorResponse(BaseModel):
    """Error response for query execution failures."""

    error: str = Field(
        ...,
        description="Error message describing what went wrong.",
    )
    query_id: str | None = Field(
        default=None,
        description="Query ID if one was assigned before the error occurred.",
    )
