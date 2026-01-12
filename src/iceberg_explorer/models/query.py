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


class ResultsMetadata(BaseModel):
    """Metadata about query results, sent as first streaming message."""

    type: str = Field(default="metadata", description="Message type identifier.")
    query_id: str = Field(..., description="Query identifier.")
    columns: list[dict] = Field(
        ...,
        description="Column definitions with name and type.",
    )
    total_rows: int = Field(..., description="Total number of rows in result set.")


class ResultsBatch(BaseModel):
    """A batch of result rows, streamed as JSON lines."""

    type: str = Field(default="data", description="Message type identifier.")
    rows: list[list] = Field(..., description="Rows in this batch as arrays.")
    batch_index: int = Field(..., description="Index of this batch (0-based).")


class ResultsProgress(BaseModel):
    """Progress update during streaming."""

    type: str = Field(default="progress", description="Message type identifier.")
    rows_sent: int = Field(..., description="Number of rows sent so far.")
    total_rows: int = Field(..., description="Total number of rows.")


class ResultsComplete(BaseModel):
    """Completion message with execution metrics."""

    type: str = Field(default="complete", description="Message type identifier.")
    query_id: str = Field(..., description="Query identifier.")
    rows_returned: int = Field(..., description="Total rows returned.")
    duration_seconds: float = Field(..., description="Query execution duration.")


class QueryStatusResponse(BaseModel):
    """Response for query status check."""

    query_id: str = Field(..., description="Query identifier.")
    status: str = Field(
        ...,
        description="Current query status: pending, running, completed, failed, cancelled.",
    )
    rows_processed: int | None = Field(
        default=None,
        description="Number of rows processed (available for running/completed queries).",
    )
    error_message: str | None = Field(
        default=None,
        description="Error message if query failed.",
    )


class CancelQueryResponse(BaseModel):
    """Response for query cancellation."""

    query_id: str = Field(..., description="Query identifier.")
    cancelled: bool = Field(
        ...,
        description="True if query was actively cancelled, False if already finished.",
    )
    status: str = Field(
        ...,
        description="Current query status after cancellation attempt.",
    )
