"""Tests for query executor."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch
from uuid import uuid4

import duckdb
import pyarrow as pa
import pytest

from iceberg_explorer.config import (
    CatalogConfig,
    CatalogType,
    DuckDBConfig,
    QueryConfig,
    Settings,
    reset_settings,
)
from iceberg_explorer.query.engine import DuckDBEngine, reset_engine
from iceberg_explorer.query.executor import (
    QueryExecutor,
    get_executor,
    reset_executor,
    validate_sql,
)
from iceberg_explorer.query.models import (
    ExecutionMetrics,
    InvalidSQLError,
    QueryCancelledError,
    QueryResult,
    QueryState,
)


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global state before and after each test."""
    reset_settings()
    reset_engine()
    reset_executor()
    yield
    reset_settings()
    reset_engine()
    reset_executor()


@pytest.fixture
def mock_settings() -> Settings:
    """Create mock settings for testing."""
    return Settings(
        catalog=CatalogConfig(
            type=CatalogType.LOCAL,
            warehouse="/tmp/test_warehouse",
            name="test_catalog",
        ),
        duckdb=DuckDBConfig(
            memory_limit="1GB",
            threads=2,
        ),
        query=QueryConfig(
            max_rows=1000,
            default_timeout=300,
            min_timeout=10,
            max_timeout=3600,
        ),
    )


@pytest.fixture
def mock_engine(mock_settings: Settings) -> DuckDBEngine:
    """Create a mock engine with real DuckDB connection."""
    engine = DuckDBEngine(settings=mock_settings)
    engine._connection = duckdb.connect(":memory:")
    engine._catalog_attached = True
    return engine


class TestValidateSql:
    """Tests for SQL validation function."""

    def test_valid_select(self):
        """Test valid SELECT statement passes."""
        validate_sql("SELECT * FROM table1")

    def test_valid_select_with_where(self):
        """Test SELECT with WHERE clause passes."""
        validate_sql("SELECT id, name FROM users WHERE id > 10")

    def test_valid_select_with_join(self):
        """Test SELECT with JOIN passes."""
        validate_sql("""
            SELECT u.name, o.total
            FROM users u
            JOIN orders o ON u.id = o.user_id
        """)

    def test_valid_select_with_cte(self):
        """Test SELECT with CTE passes."""
        validate_sql("""
            WITH active_users AS (
                SELECT * FROM users WHERE active = true
            )
            SELECT * FROM active_users
        """)

    def test_valid_explain(self):
        """Test EXPLAIN statement passes."""
        validate_sql("EXPLAIN SELECT * FROM users")

    def test_valid_describe(self):
        """Test DESCRIBE statement passes."""
        validate_sql("DESCRIBE users")

    def test_valid_show(self):
        """Test SHOW statement passes."""
        validate_sql("SHOW TABLES")

    def test_empty_sql_rejected(self):
        """Test empty SQL is rejected."""
        with pytest.raises(InvalidSQLError, match="Empty SQL"):
            validate_sql("")

    def test_whitespace_only_rejected(self):
        """Test whitespace-only SQL is rejected."""
        with pytest.raises(InvalidSQLError, match="Empty SQL"):
            validate_sql("   \n\t  ")

    def test_insert_rejected(self):
        """Test INSERT is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("INSERT INTO users VALUES (1, 'test')")

    def test_update_rejected(self):
        """Test UPDATE is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("UPDATE users SET name = 'test' WHERE id = 1")

    def test_delete_rejected(self):
        """Test DELETE is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("DELETE FROM users WHERE id = 1")

    def test_drop_rejected(self):
        """Test DROP is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("DROP TABLE users")

    def test_create_rejected(self):
        """Test CREATE is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("CREATE TABLE users (id INT)")

    def test_alter_rejected(self):
        """Test ALTER is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("ALTER TABLE users ADD COLUMN age INT")

    def test_truncate_rejected(self):
        """Test TRUNCATE is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("TRUNCATE TABLE users")

    def test_attach_rejected(self):
        """Test ATTACH is rejected."""
        with pytest.raises(InvalidSQLError, match="Write operations"):
            validate_sql("ATTACH DATABASE 'test.db' AS test")

    def test_invalid_statement_rejected(self):
        """Test unknown statements are rejected."""
        with pytest.raises(InvalidSQLError, match="Only SELECT"):
            validate_sql("RANDOM STATEMENT HERE")


class TestExecutionMetrics:
    """Tests for ExecutionMetrics class."""

    def test_duration_before_complete(self):
        """Test duration calculation while running."""
        metrics = ExecutionMetrics()
        time.sleep(0.1)
        duration = metrics.duration_seconds
        assert duration >= 0.1

    def test_duration_after_complete(self):
        """Test duration is fixed after completion."""
        metrics = ExecutionMetrics()
        time.sleep(0.05)
        metrics.complete(rows_returned=100)
        duration1 = metrics.duration_seconds
        time.sleep(0.05)
        duration2 = metrics.duration_seconds
        assert duration1 == duration2

    def test_complete_records_rows(self):
        """Test complete records row count."""
        metrics = ExecutionMetrics()
        metrics.complete(rows_returned=42)
        assert metrics.rows_returned == 42


class TestQueryResult:
    """Tests for QueryResult class."""

    def test_initial_state(self):
        """Test initial state is pending."""
        result = QueryResult(sql="SELECT 1")
        assert result.state == QueryState.PENDING
        assert not result.is_terminal

    def test_set_running(self):
        """Test set_running transitions state."""
        result = QueryResult(sql="SELECT 1")
        result.set_running()
        assert result.state == QueryState.RUNNING
        assert not result.is_terminal

    def test_set_completed(self):
        """Test set_completed with results."""
        result = QueryResult(sql="SELECT 1")
        schema = pa.schema([("value", pa.int64())])
        batch = pa.record_batch([[42]], schema=schema)
        result.set_completed([batch], schema)
        assert result.state == QueryState.COMPLETED
        assert result.is_terminal
        assert len(result.batches) == 1
        assert result.schema == schema
        assert result.metrics.rows_returned == 1

    def test_set_failed(self):
        """Test set_failed with error."""
        result = QueryResult(sql="SELECT 1")
        result.set_failed("Something went wrong")
        assert result.state == QueryState.FAILED
        assert result.is_terminal
        assert result.error_message == "Something went wrong"

    def test_set_cancelled(self):
        """Test set_cancelled."""
        result = QueryResult(sql="SELECT 1")
        result.set_cancelled()
        assert result.state == QueryState.CANCELLED
        assert result.is_terminal


class TestQueryExecutor:
    """Tests for QueryExecutor class."""

    def test_execute_simple_query(self, mock_engine: DuckDBEngine):
        """Test executing a simple SELECT query."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT 42 as value")

        assert result.state == QueryState.COMPLETED
        assert len(result.batches) == 1
        assert result.metrics.rows_returned == 1

    def test_execute_returns_arrow_batches(self, mock_engine: DuckDBEngine):
        """Test results are Arrow batches."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT 1 as a, 'test' as b")

        assert result.schema is not None
        assert len(result.schema) == 2
        assert result.schema.field("a") is not None
        assert result.schema.field("b") is not None

    def test_execute_multi_row_result(self, mock_engine: DuckDBEngine):
        """Test query returning multiple rows."""
        with mock_engine.get_connection() as conn:
            conn.execute("CREATE TABLE test_data (id INT, name VARCHAR)")
            conn.execute("INSERT INTO test_data VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT * FROM test_data")

        assert result.state == QueryState.COMPLETED
        assert result.metrics.rows_returned == 3

    def test_execute_invalid_sql_rejected(self, mock_engine: DuckDBEngine):
        """Test invalid SQL raises InvalidSQLError."""
        executor = QueryExecutor(engine=mock_engine)

        with pytest.raises(InvalidSQLError):
            executor.execute("INSERT INTO users VALUES (1)")

    def test_execute_tracks_metrics(self, mock_engine: DuckDBEngine):
        """Test execution metrics are tracked."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT * FROM range(100)")

        assert result.metrics.duration_seconds > 0
        assert result.metrics.rows_returned == 100
        assert result.metrics.end_time is not None

    def test_timeout_validation_default(self, mock_engine: DuckDBEngine, mock_settings: Settings):
        """Test default timeout is applied."""
        executor = QueryExecutor(engine=mock_engine)
        timeout = executor._validate_timeout(None)
        assert timeout == mock_settings.query.default_timeout

    def test_timeout_validation_min(self, mock_engine: DuckDBEngine, mock_settings: Settings):
        """Test minimum timeout is enforced."""
        executor = QueryExecutor(engine=mock_engine)
        timeout = executor._validate_timeout(1)
        assert timeout == mock_settings.query.min_timeout

    def test_timeout_validation_max(self, mock_engine: DuckDBEngine, mock_settings: Settings):
        """Test maximum timeout is enforced."""
        executor = QueryExecutor(engine=mock_engine)
        timeout = executor._validate_timeout(10000)
        assert timeout == mock_settings.query.max_timeout

    def test_get_status_returns_result(self, mock_engine: DuckDBEngine):
        """Test get_status returns tracked query."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT 1")

        status = executor.get_status(result.query_id)
        assert status is not None
        assert status.query_id == result.query_id

    def test_get_status_unknown_query(self, mock_engine: DuckDBEngine):
        """Test get_status returns None for unknown query."""
        executor = QueryExecutor(engine=mock_engine)
        status = executor.get_status(uuid4())
        assert status is None

    def test_cleanup_removes_query(self, mock_engine: DuckDBEngine):
        """Test cleanup removes query from tracking."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT 1")

        executor.cleanup(result.query_id)
        status = executor.get_status(result.query_id)
        assert status is None


class TestQueryCancellation:
    """Tests for query cancellation."""

    def test_cancel_completed_query(self, mock_engine: DuckDBEngine):
        """Test cancelling already completed query returns False."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT 1")

        assert not executor.cancel(result.query_id)

    def test_cancel_unknown_query(self, mock_engine: DuckDBEngine):
        """Test cancelling unknown query returns False."""
        executor = QueryExecutor(engine=mock_engine)
        assert not executor.cancel(uuid4())

    def test_cancel_running_query(self, mock_engine: DuckDBEngine):
        """Test cancelling a running query."""
        executor = QueryExecutor(engine=mock_engine)
        query_id = None
        cancel_triggered = threading.Event()
        execution_started = threading.Event()

        def slow_execute():
            nonlocal query_id
            with mock_engine.get_connection() as conn:
                conn.execute("CREATE TABLE slow_test AS SELECT * FROM range(1000000)")

            execution_started.set()
            try:
                executor.execute("SELECT * FROM slow_test, slow_test")
            except QueryCancelledError:
                cancel_triggered.set()

        result = QueryResult(sql="SELECT 1")
        result.set_running()

        with executor._lock:
            executor._active_queries[result.query_id] = result
            executor._cancel_flags[result.query_id] = threading.Event()

        success = executor.cancel(result.query_id)
        assert success
        assert result.state == QueryState.CANCELLED


class TestGlobalExecutor:
    """Tests for global executor singleton."""

    def test_get_executor_returns_same_instance(self):
        """Test get_executor returns cached instance."""
        with patch("iceberg_explorer.query.executor.get_engine") as mock:
            mock_engine = MagicMock()
            mock.return_value = mock_engine

            executor1 = get_executor()
            executor2 = get_executor()
            assert executor1 is executor2

    def test_reset_executor_clears_cache(self):
        """Test reset_executor clears the cached executor."""
        with patch("iceberg_explorer.query.executor.get_engine") as mock:
            mock_engine = MagicMock()
            mock.return_value = mock_engine

            executor1 = get_executor()
            reset_executor()
            executor2 = get_executor()
            assert executor1 is not executor2


class TestQueryExecutorIntegration:
    """Integration tests with real DuckDB queries."""

    def test_execute_with_aggregation(self, mock_engine: DuckDBEngine):
        """Test query with aggregation functions."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("SELECT COUNT(*) as cnt, SUM(i) as total FROM range(10) t(i)")

        assert result.state == QueryState.COMPLETED
        table = pa.Table.from_batches(result.batches, schema=result.schema)
        assert table.column("cnt")[0].as_py() == 10
        assert table.column("total")[0].as_py() == 45

    def test_execute_with_cte(self, mock_engine: DuckDBEngine):
        """Test query with CTE."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("""
            WITH numbers AS (
                SELECT i FROM range(5) t(i)
            )
            SELECT i * 2 as doubled FROM numbers
        """)

        assert result.state == QueryState.COMPLETED
        assert result.metrics.rows_returned == 5

    def test_execute_explain(self, mock_engine: DuckDBEngine):
        """Test EXPLAIN query works."""
        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("EXPLAIN SELECT 1")

        assert result.state == QueryState.COMPLETED
        assert result.metrics.rows_returned > 0

    def test_execute_describe(self, mock_engine: DuckDBEngine):
        """Test DESCRIBE query works."""
        with mock_engine.get_connection() as conn:
            conn.execute("CREATE TABLE desc_test (id INT, name VARCHAR)")

        executor = QueryExecutor(engine=mock_engine)
        result = executor.execute("DESCRIBE desc_test")

        assert result.state == QueryState.COMPLETED

    def test_failed_query_records_error(self, mock_engine: DuckDBEngine):
        """Test failed query records error message."""
        executor = QueryExecutor(engine=mock_engine)

        with pytest.raises((duckdb.Error, RuntimeError)):
            executor.execute("SELECT * FROM nonexistent_table_xyz")

        status = executor.get_status(list(executor._active_queries.keys())[0])
        assert status.state == QueryState.FAILED
        assert status.error_message is not None
        assert "nonexistent_table_xyz" in status.error_message.lower()
