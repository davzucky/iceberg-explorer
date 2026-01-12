"""Tests for query API routes."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from iceberg_explorer.config import reset_settings
from iceberg_explorer.main import app
from iceberg_explorer.query.engine import reset_engine
from iceberg_explorer.query.executor import reset_executor
from iceberg_explorer.query.models import ExecutionMetrics, QueryResult, QueryState


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
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


class TestExecuteQueryModels:
    """Tests for query execution Pydantic models."""

    def test_execute_query_request_model(self):
        """Test ExecuteQueryRequest model with valid SQL."""
        from iceberg_explorer.models.query import ExecuteQueryRequest

        request = ExecuteQueryRequest(sql="SELECT * FROM table1")
        assert request.sql == "SELECT * FROM table1"
        assert request.timeout is None

    def test_execute_query_request_with_timeout(self):
        """Test ExecuteQueryRequest model with custom timeout."""
        from iceberg_explorer.models.query import ExecuteQueryRequest

        request = ExecuteQueryRequest(sql="SELECT * FROM table1", timeout=60)
        assert request.sql == "SELECT * FROM table1"
        assert request.timeout == 60

    def test_execute_query_request_min_timeout(self):
        """Test ExecuteQueryRequest model with minimum timeout."""
        from iceberg_explorer.models.query import ExecuteQueryRequest

        request = ExecuteQueryRequest(sql="SELECT 1", timeout=10)
        assert request.timeout == 10

    def test_execute_query_request_max_timeout(self):
        """Test ExecuteQueryRequest model with maximum timeout."""
        from iceberg_explorer.models.query import ExecuteQueryRequest

        request = ExecuteQueryRequest(sql="SELECT 1", timeout=3600)
        assert request.timeout == 3600

    def test_execute_query_request_timeout_too_low(self):
        """Test ExecuteQueryRequest model with timeout below minimum."""
        from pydantic import ValidationError

        from iceberg_explorer.models.query import ExecuteQueryRequest

        with pytest.raises(ValidationError) as exc_info:
            ExecuteQueryRequest(sql="SELECT 1", timeout=5)
        assert "greater than or equal to 10" in str(exc_info.value)

    def test_execute_query_request_timeout_too_high(self):
        """Test ExecuteQueryRequest model with timeout above maximum."""
        from pydantic import ValidationError

        from iceberg_explorer.models.query import ExecuteQueryRequest

        with pytest.raises(ValidationError) as exc_info:
            ExecuteQueryRequest(sql="SELECT 1", timeout=7200)
        assert "less than or equal to 3600" in str(exc_info.value)

    def test_execute_query_request_empty_sql(self):
        """Test ExecuteQueryRequest model with empty SQL."""
        from pydantic import ValidationError

        from iceberg_explorer.models.query import ExecuteQueryRequest

        with pytest.raises(ValidationError) as exc_info:
            ExecuteQueryRequest(sql="")
        assert "at least 1 character" in str(exc_info.value).lower()

    def test_execute_query_response_model(self):
        """Test ExecuteQueryResponse model."""
        from iceberg_explorer.models.query import ExecuteQueryResponse

        response = ExecuteQueryResponse(
            query_id="550e8400-e29b-41d4-a716-446655440000",
            status="completed",
        )
        assert response.query_id == "550e8400-e29b-41d4-a716-446655440000"
        assert response.status == "completed"

    def test_query_error_response_model(self):
        """Test QueryErrorResponse model."""
        from iceberg_explorer.models.query import QueryErrorResponse

        response = QueryErrorResponse(
            error="Invalid SQL query",
            query_id=None,
        )
        assert response.error == "Invalid SQL query"
        assert response.query_id is None

    def test_query_error_response_with_query_id(self):
        """Test QueryErrorResponse model with query_id."""
        from iceberg_explorer.models.query import QueryErrorResponse

        response = QueryErrorResponse(
            error="Query timeout",
            query_id="550e8400-e29b-41d4-a716-446655440000",
        )
        assert response.error == "Query timeout"
        assert response.query_id == "550e8400-e29b-41d4-a716-446655440000"


class TestExecuteQueryEndpoint:
    """Tests for POST /api/v1/query/execute endpoint."""

    def test_execute_valid_select(self, client: TestClient):
        """Test executing a valid SELECT query."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT * FROM table1"},
            )

        assert response.status_code == 200
        data = response.json()
        assert "query_id" in data
        assert data["status"] == "completed"

    def test_execute_select_with_timeout(self, client: TestClient):
        """Test executing a SELECT query with custom timeout."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT * FROM table1", "timeout": 60},
            )

        assert response.status_code == 200
        mock_executor.execute.assert_called_once_with("SELECT * FROM table1", 60)

    def test_execute_select_with_min_timeout(self, client: TestClient):
        """Test executing a SELECT query with minimum timeout."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT 1", "timeout": 10},
            )

        assert response.status_code == 200

    def test_execute_select_with_max_timeout(self, client: TestClient):
        """Test executing a SELECT query with maximum timeout."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT 1", "timeout": 3600},
            )

        assert response.status_code == 200

    def test_execute_with_where_clause(self, client: TestClient):
        """Test executing a SELECT query with WHERE clause."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT id, name FROM users WHERE active = true"},
            )

        assert response.status_code == 200

    def test_execute_with_join(self, client: TestClient):
        """Test executing a SELECT query with JOIN."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={
                    "sql": "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
                },
            )

        assert response.status_code == 200

    def test_execute_with_cte(self, client: TestClient):
        """Test executing a SELECT query with CTE (WITH clause)."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={
                    "sql": "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users"
                },
            )

        assert response.status_code == 200

    def test_execute_invalid_insert(self, client: TestClient):
        """Test that INSERT is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "INSERT INTO table1 VALUES (1, 2, 3)"},
        )

        assert response.status_code == 400
        assert "INSERT" in response.json()["detail"].upper()

    def test_execute_invalid_update(self, client: TestClient):
        """Test that UPDATE is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "UPDATE table1 SET col1 = 'value'"},
        )

        assert response.status_code == 400
        assert "UPDATE" in response.json()["detail"].upper()

    def test_execute_invalid_delete(self, client: TestClient):
        """Test that DELETE is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "DELETE FROM table1 WHERE id = 1"},
        )

        assert response.status_code == 400
        assert "DELETE" in response.json()["detail"].upper()

    def test_execute_invalid_drop(self, client: TestClient):
        """Test that DROP is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "DROP TABLE table1"},
        )

        assert response.status_code == 400
        assert "DROP" in response.json()["detail"].upper()

    def test_execute_invalid_create(self, client: TestClient):
        """Test that CREATE is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "CREATE TABLE table1 (id INT)"},
        )

        assert response.status_code == 400
        assert "CREATE" in response.json()["detail"].upper()

    def test_execute_invalid_alter(self, client: TestClient):
        """Test that ALTER is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "ALTER TABLE table1 ADD COLUMN col2 INT"},
        )

        assert response.status_code == 400
        assert "ALTER" in response.json()["detail"].upper()

    def test_execute_invalid_truncate(self, client: TestClient):
        """Test that TRUNCATE is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "TRUNCATE TABLE table1"},
        )

        assert response.status_code == 400
        assert "TRUNCATE" in response.json()["detail"].upper()

    def test_execute_empty_sql_body(self, client: TestClient):
        """Test that empty SQL in body is rejected by Pydantic validation."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": ""},
        )

        assert response.status_code == 422

    def test_execute_missing_sql_field(self, client: TestClient):
        """Test that missing SQL field is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={},
        )

        assert response.status_code == 422

    def test_execute_timeout_too_low(self, client: TestClient):
        """Test that timeout below minimum is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "SELECT 1", "timeout": 5},
        )

        assert response.status_code == 422

    def test_execute_timeout_too_high(self, client: TestClient):
        """Test that timeout above maximum is rejected."""
        response = client.post(
            "/api/v1/query/execute",
            json={"sql": "SELECT 1", "timeout": 7200},
        )

        assert response.status_code == 422

    def test_execute_running_status(self, client: TestClient):
        """Test that running query returns correct status."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.RUNNING

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT * FROM table1"},
            )

        assert response.status_code == 200
        assert response.json()["status"] == "running"

    def test_execute_failed_status(self, client: TestClient):
        """Test that failed query returns correct status."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.FAILED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT * FROM table1"},
            )

        assert response.status_code == 200
        assert response.json()["status"] == "failed"

    def test_execute_returns_query_id(self, client: TestClient):
        """Test that query ID is returned for tracking."""
        query_id = uuid4()
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT * FROM table1"},
            )

        assert response.status_code == 200
        assert response.json()["query_id"] == str(query_id)

    def test_execute_explain_query(self, client: TestClient):
        """Test executing an EXPLAIN query."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "EXPLAIN SELECT * FROM table1"},
            )

        assert response.status_code == 200

    def test_execute_describe_query(self, client: TestClient):
        """Test executing a DESCRIBE query."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "DESCRIBE table1"},
            )

        assert response.status_code == 200

    def test_execute_show_query(self, client: TestClient):
        """Test executing a SHOW query."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SHOW TABLES"},
            )

        assert response.status_code == 200

    def test_execute_executor_exception(self, client: TestClient):
        """Test handling of executor exceptions."""
        mock_executor = MagicMock()
        mock_executor.execute.side_effect = RuntimeError("Database connection failed")

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT * FROM table1"},
            )

        assert response.status_code == 500
        assert "Database connection failed" in response.json()["detail"]

    def test_execute_whitespace_sql(self, client: TestClient):
        """Test executing SQL with leading/trailing whitespace."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "  SELECT * FROM table1  "},
            )

        assert response.status_code == 200

    def test_execute_multiline_sql(self, client: TestClient):
        """Test executing multi-line SQL query."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT\n  id,\n  name\nFROM\n  users"},
            )

        assert response.status_code == 200

    def test_execute_default_timeout_used(self, client: TestClient):
        """Test that default timeout is used when not specified."""
        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = uuid4()
        mock_result.state = QueryState.COMPLETED

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/query/execute",
                json={"sql": "SELECT * FROM table1"},
            )

        assert response.status_code == 200
        mock_executor.execute.assert_called_once_with("SELECT * FROM table1", None)


class TestResultsStreamingModels:
    """Tests for streaming results Pydantic models."""

    def test_results_metadata_model(self):
        """Test ResultsMetadata model."""
        from iceberg_explorer.models.query import ResultsMetadata

        metadata = ResultsMetadata(
            query_id="test-id",
            columns=[{"name": "col1", "type": "int64"}],
            total_rows=100,
        )
        assert metadata.type == "metadata"
        assert metadata.query_id == "test-id"
        assert len(metadata.columns) == 1
        assert metadata.total_rows == 100

    def test_results_batch_model(self):
        """Test ResultsBatch model."""
        from iceberg_explorer.models.query import ResultsBatch

        batch = ResultsBatch(
            rows=[[1, "a"], [2, "b"]],
            batch_index=0,
        )
        assert batch.type == "data"
        assert len(batch.rows) == 2
        assert batch.batch_index == 0

    def test_results_progress_model(self):
        """Test ResultsProgress model."""
        from iceberg_explorer.models.query import ResultsProgress

        progress = ResultsProgress(
            rows_sent=50,
            total_rows=100,
        )
        assert progress.type == "progress"
        assert progress.rows_sent == 50
        assert progress.total_rows == 100

    def test_results_complete_model(self):
        """Test ResultsComplete model."""
        from iceberg_explorer.models.query import ResultsComplete

        complete = ResultsComplete(
            query_id="test-id",
            rows_returned=100,
            duration_seconds=1.5,
        )
        assert complete.type == "complete"
        assert complete.query_id == "test-id"
        assert complete.rows_returned == 100
        assert complete.duration_seconds == 1.5


class TestGetResultsEndpoint:
    """Tests for GET /api/v1/query/{query_id}/results endpoint."""

    def _create_mock_result(
        self,
        state: QueryState = QueryState.COMPLETED,
        rows: list[list] | None = None,
        columns: list[str] | None = None,
    ) -> tuple[MagicMock, UUID]:
        """Create a mock QueryResult with Arrow batches."""
        query_id = uuid4()

        if rows is None:
            rows = [[1, "a"], [2, "b"], [3, "c"]]
        if columns is None:
            columns = ["id", "name"]

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )

        if rows:
            arrays = [
                pa.array([r[0] for r in rows]),
                pa.array([r[1] for r in rows]),
            ]
            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
            batches = [batch]
        else:
            batches = []

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = state
        mock_result.schema = schema
        mock_result.batches = batches
        mock_result.error_message = None
        mock_result.metrics = MagicMock(spec=ExecutionMetrics)
        mock_result.metrics.duration_seconds = 0.5

        return mock_result, query_id

    def test_stream_results_success(self, client: TestClient):
        """Test streaming results for a completed query."""
        mock_result, query_id = self._create_mock_result()
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"

        lines = [json.loads(line) for line in response.text.strip().split("\n")]

        assert lines[0]["type"] == "metadata"
        assert lines[0]["query_id"] == str(query_id)
        assert lines[0]["total_rows"] == 3
        assert len(lines[0]["columns"]) == 2

        assert lines[-1]["type"] == "complete"
        assert lines[-1]["rows_returned"] == 3
        assert lines[-1]["duration_seconds"] == 0.5

    def test_stream_results_with_data_batches(self, client: TestClient):
        """Test streaming includes data batches."""
        mock_result, query_id = self._create_mock_result()
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]

        data_lines = [item for item in lines if item["type"] == "data"]
        assert len(data_lines) > 0

        all_rows = []
        for batch in data_lines:
            all_rows.extend(batch["rows"])
        assert len(all_rows) == 3
        assert all_rows[0] == [1, "a"]

    def test_stream_results_query_not_found(self, client: TestClient):
        """Test streaming when query is not found."""
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = None

        query_id = uuid4()
        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        assert response.status_code == 200
        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        assert lines[0]["type"] == "error"
        assert "not found" in lines[0]["error"].lower()

    def test_stream_results_invalid_query_id(self, client: TestClient):
        """Test streaming with invalid query ID format."""
        response = client.get("/api/v1/query/invalid-uuid/results")

        assert response.status_code == 200
        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        assert lines[0]["type"] == "error"
        assert "invalid" in lines[0]["error"].lower()

    def test_stream_results_failed_query(self, client: TestClient):
        """Test streaming when query has failed."""
        mock_result, query_id = self._create_mock_result(state=QueryState.FAILED)
        mock_result.error_message = "Query timeout"
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        assert lines[0]["type"] == "error"
        assert "Query timeout" in lines[0]["error"]

    def test_stream_results_cancelled_query(self, client: TestClient):
        """Test streaming when query was cancelled."""
        mock_result, query_id = self._create_mock_result(state=QueryState.CANCELLED)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        assert lines[0]["type"] == "error"
        assert "cancelled" in lines[0]["error"].lower() or "not ready" in lines[0]["error"].lower()

    def test_stream_results_page_size_100(self, client: TestClient):
        """Test streaming with page_size=100."""
        rows = [[i, f"name{i}"] for i in range(150)]
        mock_result, query_id = self._create_mock_result(rows=rows)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results?page_size=100")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        complete = next(item for item in lines if item["type"] == "complete")
        assert complete["rows_returned"] == 100

    def test_stream_results_page_size_250(self, client: TestClient):
        """Test streaming with page_size=250."""
        rows = [[i, f"name{i}"] for i in range(300)]
        mock_result, query_id = self._create_mock_result(rows=rows)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results?page_size=250")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        complete = next(item for item in lines if item["type"] == "complete")
        assert complete["rows_returned"] == 250

    def test_stream_results_page_size_500(self, client: TestClient):
        """Test streaming with page_size=500."""
        rows = [[i, f"name{i}"] for i in range(600)]
        mock_result, query_id = self._create_mock_result(rows=rows)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results?page_size=500")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        complete = next(item for item in lines if item["type"] == "complete")
        assert complete["rows_returned"] == 500

    def test_stream_results_page_size_1000(self, client: TestClient):
        """Test streaming with page_size=1000."""
        rows = [[i, f"name{i}"] for i in range(1200)]
        mock_result, query_id = self._create_mock_result(rows=rows)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results?page_size=1000")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        complete = next(item for item in lines if item["type"] == "complete")
        assert complete["rows_returned"] == 1000

    def test_stream_results_invalid_page_size(self, client: TestClient):
        """Test streaming with invalid page_size."""
        query_id = uuid4()
        response = client.get(f"/api/v1/query/{query_id}/results?page_size=50")

        assert response.status_code == 400
        assert "Invalid page_size" in response.json()["detail"]

    def test_stream_results_with_offset(self, client: TestClient):
        """Test streaming with offset parameter."""
        rows = [[i, f"name{i}"] for i in range(200)]
        mock_result, query_id = self._create_mock_result(rows=rows)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results?page_size=100&offset=50")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        data_lines = [item for item in lines if item["type"] == "data"]

        all_rows = []
        for batch in data_lines:
            all_rows.extend(batch["rows"])

        if len(all_rows) > 0:
            assert all_rows[0][0] == 50

    def test_stream_results_offset_beyond_data(self, client: TestClient):
        """Test streaming with offset beyond data."""
        rows = [[i, f"name{i}"] for i in range(50)]
        mock_result, query_id = self._create_mock_result(rows=rows)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results?page_size=100&offset=100")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        complete = next(item for item in lines if item["type"] == "complete")
        assert complete["rows_returned"] == 0

    def test_stream_results_empty_result(self, client: TestClient):
        """Test streaming with empty result set."""
        mock_result, query_id = self._create_mock_result(rows=[])
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]

        metadata = next(item for item in lines if item["type"] == "metadata")
        assert metadata["total_rows"] == 0

        complete = next(item for item in lines if item["type"] == "complete")
        assert complete["rows_returned"] == 0

    def test_stream_results_includes_progress(self, client: TestClient):
        """Test streaming includes progress updates."""
        rows = [[i, f"name{i}"] for i in range(150)]
        mock_result, query_id = self._create_mock_result(rows=rows)
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        progress_lines = [item for item in lines if item["type"] == "progress"]
        assert len(progress_lines) > 0
        assert "rows_sent" in progress_lines[0]
        assert "total_rows" in progress_lines[0]

    def test_stream_results_includes_execution_metrics(self, client: TestClient):
        """Test streaming includes execution metrics in complete message."""
        mock_result, query_id = self._create_mock_result()
        mock_result.metrics.duration_seconds = 2.5
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        complete = next(item for item in lines if item["type"] == "complete")
        assert complete["duration_seconds"] == 2.5

    def test_stream_results_columns_in_metadata(self, client: TestClient):
        """Test that column info is included in metadata."""
        mock_result, query_id = self._create_mock_result()
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = [json.loads(line) for line in response.text.strip().split("\n")]
        metadata = next(item for item in lines if item["type"] == "metadata")

        assert len(metadata["columns"]) == 2
        assert metadata["columns"][0]["name"] == "id"
        assert "int" in metadata["columns"][0]["type"].lower()
        assert metadata["columns"][1]["name"] == "name"

    def test_stream_results_negative_offset_rejected(self, client: TestClient):
        """Test that negative offset is rejected."""
        query_id = uuid4()
        response = client.get(f"/api/v1/query/{query_id}/results?offset=-10")

        assert response.status_code == 422

    def test_stream_results_json_lines_format(self, client: TestClient):
        """Test that response is valid JSON lines format."""
        mock_result, query_id = self._create_mock_result()
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/results")

        lines = response.text.strip().split("\n")
        for line in lines:
            parsed = json.loads(line)
            assert "type" in parsed


class TestQueryStatusEndpoint:
    """Tests for GET /api/v1/query/{query_id}/status endpoint."""

    def test_status_pending_query(self, client: TestClient):
        """Test status for pending query."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.PENDING
        mock_result.error_message = None
        mock_result.metrics = MagicMock(rows_returned=0)

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/status")

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == str(query_id)
        assert data["status"] == "pending"
        assert data["rows_processed"] is None
        assert data["error_message"] is None

    def test_status_running_query(self, client: TestClient):
        """Test status for running query - rows_processed is None until complete."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.RUNNING
        mock_result.error_message = None
        mock_result.metrics = MagicMock(rows_returned=500)

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/status")

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == str(query_id)
        assert data["status"] == "running"
        # rows_processed is None during RUNNING to avoid ambiguity with 0 rows
        assert data["rows_processed"] is None
        assert data["error_message"] is None

    def test_status_completed_query(self, client: TestClient):
        """Test status for completed query."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.error_message = None
        mock_result.metrics = MagicMock(rows_returned=1000)

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/status")

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == str(query_id)
        assert data["status"] == "completed"
        assert data["rows_processed"] == 1000
        assert data["error_message"] is None

    def test_status_failed_query(self, client: TestClient):
        """Test status for failed query with error message."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.FAILED
        mock_result.error_message = "Query timeout exceeded"
        mock_result.metrics = MagicMock(rows_returned=0)

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/status")

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == str(query_id)
        assert data["status"] == "failed"
        assert data["rows_processed"] is None
        assert data["error_message"] == "Query timeout exceeded"

    def test_status_cancelled_query(self, client: TestClient):
        """Test status for cancelled query."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.CANCELLED
        mock_result.error_message = None
        mock_result.metrics = MagicMock(rows_returned=0)

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/status")

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == str(query_id)
        assert data["status"] == "cancelled"
        assert data["rows_processed"] is None
        assert data["error_message"] is None

    def test_status_query_not_found(self, client: TestClient):
        """Test status when query is not found."""
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = None

        query_id = uuid4()
        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.get(f"/api/v1/query/{query_id}/status")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_status_invalid_query_id(self, client: TestClient):
        """Test status with invalid query ID format."""
        response = client.get("/api/v1/query/invalid-uuid/status")

        assert response.status_code == 400
        assert "invalid" in response.json()["detail"].lower()

    def test_status_model_validation(self):
        """Test QueryStatusResponse model validation."""
        from iceberg_explorer.models.query import QueryStatusResponse

        response = QueryStatusResponse(
            query_id="550e8400-e29b-41d4-a716-446655440000",
            status="running",
            rows_processed=100,
            error_message=None,
        )
        assert response.query_id == "550e8400-e29b-41d4-a716-446655440000"
        assert response.status == "running"
        assert response.rows_processed == 100
        assert response.error_message is None

    def test_status_model_with_error(self):
        """Test QueryStatusResponse model with error message."""
        from iceberg_explorer.models.query import QueryStatusResponse

        response = QueryStatusResponse(
            query_id="550e8400-e29b-41d4-a716-446655440000",
            status="failed",
            rows_processed=None,
            error_message="Connection timeout",
        )
        assert response.status == "failed"
        assert response.error_message == "Connection timeout"

    def test_status_model_minimal(self):
        """Test QueryStatusResponse model with minimal fields."""
        from iceberg_explorer.models.query import QueryStatusResponse

        response = QueryStatusResponse(
            query_id="550e8400-e29b-41d4-a716-446655440000",
            status="pending",
        )
        assert response.rows_processed is None
        assert response.error_message is None


class TestCancelQueryEndpoint:
    """Tests for POST /api/v1/query/{query_id}/cancel endpoint."""

    def test_cancel_running_query(self, client: TestClient):
        """Test cancelling a running query."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.RUNNING
        mock_result.is_terminal = False

        mock_cancelled_result = MagicMock(spec=QueryResult)
        mock_cancelled_result.query_id = query_id
        mock_cancelled_result.state = QueryState.CANCELLED

        mock_executor = MagicMock()
        mock_executor.get_status.side_effect = [mock_result, mock_cancelled_result]
        mock_executor.cancel.return_value = True

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(f"/api/v1/query/{query_id}/cancel")

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == str(query_id)
        assert data["cancelled"] is True
        assert data["status"] == "cancelled"

    def test_cancel_completed_query(self, client: TestClient):
        """Test cancelling an already completed query returns success."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.is_terminal = True

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result
        mock_executor.cancel.return_value = False

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(f"/api/v1/query/{query_id}/cancel")

        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == str(query_id)
        assert data["cancelled"] is False
        assert data["status"] == "completed"

    def test_cancel_failed_query(self, client: TestClient):
        """Test cancelling an already failed query returns success."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.FAILED
        mock_result.is_terminal = True

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result
        mock_executor.cancel.return_value = False

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(f"/api/v1/query/{query_id}/cancel")

        assert response.status_code == 200
        data = response.json()
        assert data["cancelled"] is False
        assert data["status"] == "failed"

    def test_cancel_already_cancelled_query(self, client: TestClient):
        """Test cancelling an already cancelled query returns success."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.CANCELLED
        mock_result.is_terminal = True

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result
        mock_executor.cancel.return_value = False

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(f"/api/v1/query/{query_id}/cancel")

        assert response.status_code == 200
        data = response.json()
        assert data["cancelled"] is False
        assert data["status"] == "cancelled"

    def test_cancel_pending_query(self, client: TestClient):
        """Test cancelling a pending query."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.PENDING
        mock_result.is_terminal = False

        mock_cancelled_result = MagicMock(spec=QueryResult)
        mock_cancelled_result.query_id = query_id
        mock_cancelled_result.state = QueryState.CANCELLED

        mock_executor = MagicMock()
        mock_executor.get_status.side_effect = [mock_result, mock_cancelled_result]
        mock_executor.cancel.return_value = True

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(f"/api/v1/query/{query_id}/cancel")

        assert response.status_code == 200
        data = response.json()
        assert data["cancelled"] is True

    def test_cancel_query_not_found(self, client: TestClient):
        """Test cancelling a non-existent query returns 404."""
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = None

        query_id = uuid4()
        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            response = client.post(f"/api/v1/query/{query_id}/cancel")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_cancel_invalid_query_id(self, client: TestClient):
        """Test cancelling with invalid query ID format returns 400."""
        response = client.post("/api/v1/query/invalid-uuid/cancel")

        assert response.status_code == 400
        assert "invalid" in response.json()["detail"].lower()

    def test_cancel_response_model_validation(self):
        """Test CancelQueryResponse model validation."""
        from iceberg_explorer.models.query import CancelQueryResponse

        response = CancelQueryResponse(
            query_id="550e8400-e29b-41d4-a716-446655440000",
            cancelled=True,
            status="cancelled",
        )
        assert response.query_id == "550e8400-e29b-41d4-a716-446655440000"
        assert response.cancelled is True
        assert response.status == "cancelled"

    def test_cancel_response_model_already_finished(self):
        """Test CancelQueryResponse model for already finished query."""
        from iceberg_explorer.models.query import CancelQueryResponse

        response = CancelQueryResponse(
            query_id="550e8400-e29b-41d4-a716-446655440000",
            cancelled=False,
            status="completed",
        )
        assert response.cancelled is False
        assert response.status == "completed"

    def test_cancel_endpoint_calls_executor_cancel(self, client: TestClient):
        """Test that cancel endpoint calls executor.cancel with correct query_id."""
        mock_result = MagicMock(spec=QueryResult)
        query_id = uuid4()
        mock_result.query_id = query_id
        mock_result.state = QueryState.RUNNING

        mock_cancelled_result = MagicMock(spec=QueryResult)
        mock_cancelled_result.state = QueryState.CANCELLED

        mock_executor = MagicMock()
        mock_executor.get_status.side_effect = [mock_result, mock_cancelled_result]
        mock_executor.cancel.return_value = True

        with patch("iceberg_explorer.api.routes.query.get_executor", return_value=mock_executor):
            client.post(f"/api/v1/query/{query_id}/cancel")

        mock_executor.cancel.assert_called_once_with(query_id)
