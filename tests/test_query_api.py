"""Tests for query API routes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from iceberg_explorer.config import reset_settings
from iceberg_explorer.main import app
from iceberg_explorer.query.engine import reset_engine
from iceberg_explorer.query.executor import reset_executor
from iceberg_explorer.query.models import QueryResult, QueryState


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
        mock_executor.execute.assert_called_once_with("SELECT * FROM table1", timeout=60)

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
                json={"sql": "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"},
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
                json={"sql": "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users"},
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
        mock_executor.execute.assert_called_once_with("SELECT * FROM table1", timeout=None)
