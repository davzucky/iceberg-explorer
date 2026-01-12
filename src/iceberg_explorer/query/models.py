"""Data models for query execution.

Provides Pydantic models for:
- Query execution requests and results
- Execution metrics tracking
- Query state management
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    import pyarrow as pa


class QueryState(str, Enum):
    """Query execution states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ExecutionMetrics:
    """Metrics collected during query execution."""

    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    rows_scanned: int = 0
    rows_returned: int = 0

    @property
    def duration_seconds(self) -> float:
        """Get query execution duration in seconds."""
        end = self.end_time or time.time()
        return end - self.start_time

    def complete(self, rows_returned: int = 0) -> None:
        """Mark query as complete and record final metrics."""
        self.end_time = time.time()
        self.rows_returned = rows_returned


@dataclass
class QueryResult:
    """Result of a query execution.

    Holds the query state, any results (as Arrow batches), and metrics.
    """

    query_id: UUID = field(default_factory=uuid4)
    sql: str = ""
    state: QueryState = QueryState.PENDING
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    error_message: str | None = None
    _batches: list[pa.RecordBatch] = field(default_factory=list)
    _schema: pa.Schema | None = None

    def set_running(self) -> None:
        """Mark query as running."""
        self.state = QueryState.RUNNING
        self.metrics.start_time = time.time()

    def set_completed(self, batches: list[pa.RecordBatch], schema: pa.Schema) -> None:
        """Mark query as completed with results.

        Args:
            batches: Arrow record batches containing results.
            schema: Arrow schema for the result set.
        """
        self.state = QueryState.COMPLETED
        self._batches = batches
        self._schema = schema
        total_rows = sum(batch.num_rows for batch in batches)
        self.metrics.complete(rows_returned=total_rows)

    def set_failed(self, error: str) -> None:
        """Mark query as failed with error message.

        Args:
            error: Error message describing the failure.
        """
        self.state = QueryState.FAILED
        self.error_message = error
        self.metrics.end_time = time.time()

    def set_cancelled(self) -> None:
        """Mark query as cancelled."""
        self.state = QueryState.CANCELLED
        self.metrics.end_time = time.time()

    @property
    def batches(self) -> list[pa.RecordBatch]:
        """Get result batches."""
        return self._batches

    @property
    def schema(self) -> pa.Schema | None:
        """Get result schema."""
        return self._schema

    @property
    def is_terminal(self) -> bool:
        """Check if query is in a terminal state."""
        return self.state in (QueryState.COMPLETED, QueryState.FAILED, QueryState.CANCELLED)


class InvalidSQLError(Exception):
    """Raised when SQL validation fails."""

    pass


class QueryTimeoutError(Exception):
    """Raised when query execution times out."""

    pass


class QueryCancelledError(Exception):
    """Raised when query is cancelled."""

    pass
