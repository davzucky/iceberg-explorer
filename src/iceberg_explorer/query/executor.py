"""Query executor for running SQL queries against Iceberg tables.

Provides:
- SQL validation (SELECT-only)
- Query execution with timeout
- Results as Arrow batches for streaming
- Query cancellation via interrupt
- Execution metrics tracking
"""

from __future__ import annotations

import re
import threading
from typing import TYPE_CHECKING
from uuid import UUID

import duckdb

from iceberg_explorer.config import get_settings
from iceberg_explorer.query.engine import get_engine
from iceberg_explorer.query.models import (
    InvalidSQLError,
    QueryCancelledError,
    QueryResult,
    QueryState,
    QueryTimeoutError,
)

if TYPE_CHECKING:
    from iceberg_explorer.query.engine import DuckDBEngine

FORBIDDEN_KEYWORDS = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "REPLACE",
        "MERGE",
        "UPSERT",
        "GRANT",
        "REVOKE",
        "ATTACH",
        "DETACH",
        "COPY",
        "EXPORT",
        "IMPORT",
        "VACUUM",
        "CHECKPOINT",
        "LOAD",
        "INSTALL",
    }
)

FORBIDDEN_PATTERN = re.compile(
    r"^\s*(" + "|".join(FORBIDDEN_KEYWORDS) + r")\b",
    re.IGNORECASE | re.MULTILINE,
)


def validate_sql(sql: str) -> None:
    """Validate that SQL is a read-only SELECT statement.

    Args:
        sql: SQL query string to validate.

    Raises:
        InvalidSQLError: If SQL contains forbidden keywords or is not a SELECT.
    """
    stripped = sql.strip()
    if not stripped:
        raise InvalidSQLError("Empty SQL query")

    # Enforce single-statement queries (prevents `SELECT ...; DROP ...`).
    if ";" in stripped.rstrip(";"):
        raise InvalidSQLError("Multiple statements or semicolons are not allowed")

    match = FORBIDDEN_PATTERN.search(stripped)
    if match:
        keyword = match.group(1).upper() if match else "unknown"
        raise InvalidSQLError(f"Write operations are not allowed: {keyword}")

    normalized = " ".join(stripped.split()).upper()

    allowed_prefixes = ("WITH ", "SELECT ", "EXPLAIN ", "DESCRIBE ", "SHOW ")
    if not any(normalized.startswith(prefix) for prefix in allowed_prefixes):
        raise InvalidSQLError("Only SELECT, EXPLAIN, DESCRIBE, and SHOW statements are allowed")


class QueryExecutor:
    """Executes SQL queries and manages query lifecycle.

    Provides thread-safe query execution with:
    - Configurable timeout (10s - 3600s)
    - Query cancellation
    - Arrow batch result streaming
    - Execution metrics
    """

    def __init__(self, engine: DuckDBEngine | None = None) -> None:
        """Initialize the executor.

        Args:
            engine: DuckDB engine to use. If None, uses global engine.
        """
        self._engine = engine or get_engine()
        self._settings = get_settings()
        self._active_queries: dict[UUID, QueryResult] = {}
        self._cancel_flags: dict[UUID, threading.Event] = {}
        self._active_conn: dict[UUID, duckdb.DuckDBPyConnection] = {}
        self._lock = threading.Lock()

    def _validate_timeout(self, timeout: int | None) -> int:
        """Validate and normalize query timeout.

        Args:
            timeout: Requested timeout in seconds, or None for default.

        Returns:
            Validated timeout within configured bounds.
        """
        query_config = self._settings.query
        if timeout is None:
            return query_config.default_timeout
        return max(query_config.min_timeout, min(timeout, query_config.max_timeout))

    def execute(self, sql: str, timeout: int | None = None) -> QueryResult:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query to execute.
            timeout: Query timeout in seconds (10-3600). Defaults to 300s.

        Returns:
            QueryResult with execution state and results.

        Raises:
            InvalidSQLError: If SQL validation fails.
            QueryTimeoutError: If query exceeds timeout.
            QueryCancelledError: If query is cancelled.
        """
        validate_sql(sql)
        validated_timeout = self._validate_timeout(timeout)

        result = QueryResult(sql=sql)
        cancel_event = threading.Event()

        with self._lock:
            self._active_queries[result.query_id] = result
            self._cancel_flags[result.query_id] = cancel_event

        try:
            result.set_running()
            self._execute_query(result, validated_timeout, cancel_event)
        finally:
            with self._lock:
                self._cancel_flags.pop(result.query_id, None)

        return result

    def _execute_query(
        self, result: QueryResult, timeout: int, cancel_event: threading.Event
    ) -> None:
        """Execute the query and populate result.

        Args:
            result: QueryResult to populate.
            timeout: Query timeout in seconds.
            cancel_event: Event to signal cancellation.
        """
        exception_holder: list[Exception] = []
        execution_complete = threading.Event()

        def run_query() -> None:
            conn: duckdb.DuckDBPyConnection | None = None
            try:
                if cancel_event.is_set():
                    return

                with self._engine.get_connection() as conn:
                    with self._lock:
                        self._active_conn[result.query_id] = conn

                    if cancel_event.is_set():
                        return

                    arrow_result = conn.execute(result.sql).fetch_arrow_table()

                    if cancel_event.is_set():
                        return

                    batches = arrow_result.to_batches()
                    result.set_completed(batches, arrow_result.schema)
            except Exception as e:
                if not cancel_event.is_set():
                    exception_holder.append(e)
            finally:
                with self._lock:
                    self._active_conn.pop(result.query_id, None)
                execution_complete.set()

        thread = threading.Thread(target=run_query, daemon=True)
        thread.start()

        completed = execution_complete.wait(timeout=timeout)

        if not completed:
            cancel_event.set()
            with self._lock:
                active_conn = self._active_conn.get(result.query_id)
                if active_conn is not None:
                    try:
                        active_conn.interrupt()
                    except Exception:
                        pass
                    self._active_conn.pop(result.query_id, None)
            result.set_failed("Query timeout exceeded")
            raise QueryTimeoutError(f"Query exceeded {timeout}s timeout")

        if cancel_event.is_set():
            result.set_cancelled()
            raise QueryCancelledError("Query was cancelled")

        if exception_holder:
            error = exception_holder[0]
            result.set_failed(str(error))
            raise error

    def cancel(self, query_id: UUID) -> bool:
        """Cancel a running query.

        Args:
            query_id: ID of query to cancel.

        Returns:
            True if query was cancelled, False if not found or already complete.
        """
        with self._lock:
            cancel_event = self._cancel_flags.get(query_id)
            result = self._active_queries.get(query_id)
            active_conn = self._active_conn.get(query_id)

            if cancel_event is None or result is None:
                return False

            if result.is_terminal:
                return False

            cancel_event.set()

            if active_conn is not None:
                try:
                    active_conn.interrupt()
                except Exception:
                    pass
                self._active_conn.pop(query_id, None)

            if result.state == QueryState.RUNNING:
                result.set_cancelled()

        return True

    def get_status(self, query_id: UUID) -> QueryResult | None:
        """Get the status of a query.

        Args:
            query_id: ID of query to check.

        Returns:
            QueryResult if found, None otherwise.
        """
        with self._lock:
            return self._active_queries.get(query_id)

    def cleanup(self, query_id: UUID) -> None:
        """Remove a completed query from tracking.

        Args:
            query_id: ID of query to clean up.
        """
        with self._lock:
            self._active_queries.pop(query_id, None)
            self._cancel_flags.pop(query_id, None)
            self._active_conn.pop(query_id, None)


_executor: QueryExecutor | None = None


def get_executor() -> QueryExecutor:
    """Get the global query executor (cached).

    Returns:
        The global QueryExecutor instance.
    """
    global _executor
    if _executor is None:
        _executor = QueryExecutor()
    return _executor


def reset_executor() -> None:
    """Reset the global executor (useful for testing)."""
    global _executor
    _executor = None
