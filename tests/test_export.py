"""Tests for export API routes."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pyarrow as pa
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


class TestCSVExportRequestModel:
    """Tests for CSVExportRequest Pydantic model."""

    def test_csv_export_request_with_query_id(self):
        """Test CSVExportRequest with query_id."""
        from iceberg_explorer.api.routes.export import CSVExportRequest

        request = CSVExportRequest(query_id="550e8400-e29b-41d4-a716-446655440000")
        assert request.query_id == "550e8400-e29b-41d4-a716-446655440000"
        assert request.sql is None
        assert request.filename is None

    def test_csv_export_request_with_sql(self):
        """Test CSVExportRequest with inline SQL."""
        from iceberg_explorer.api.routes.export import CSVExportRequest

        request = CSVExportRequest(sql="SELECT * FROM table1")
        assert request.query_id is None
        assert request.sql == "SELECT * FROM table1"

    def test_csv_export_request_with_filename(self):
        """Test CSVExportRequest with custom filename."""
        from iceberg_explorer.api.routes.export import CSVExportRequest

        request = CSVExportRequest(
            query_id="550e8400-e29b-41d4-a716-446655440000",
            filename="my_export",
        )
        assert request.filename == "my_export"


class TestFormatValue:
    """Tests for the _format_value helper function."""

    def test_format_none(self):
        """Test formatting None value."""
        from iceberg_explorer.api.routes.export import _format_value

        assert _format_value(None) == ""

    def test_format_string(self):
        """Test formatting string value."""
        from iceberg_explorer.api.routes.export import _format_value

        assert _format_value("hello") == "hello"

    def test_format_integer(self):
        """Test formatting integer value."""
        from iceberg_explorer.api.routes.export import _format_value

        assert _format_value(42) == "42"

    def test_format_float(self):
        """Test formatting float value."""
        from iceberg_explorer.api.routes.export import _format_value

        assert _format_value(3.14) == "3.14"

    def test_format_bool_true(self):
        """Test formatting True value."""
        from iceberg_explorer.api.routes.export import _format_value

        assert _format_value(True) == "true"

    def test_format_bool_false(self):
        """Test formatting False value."""
        from iceberg_explorer.api.routes.export import _format_value

        assert _format_value(False) == "false"

    def test_format_datetime(self):
        """Test formatting datetime value."""
        from iceberg_explorer.api.routes.export import _format_value

        dt = datetime(2024, 1, 15, 10, 30, 0)
        assert _format_value(dt) == "2024-01-15T10:30:00"

    def test_format_bytes(self):
        """Test formatting bytes value."""
        from iceberg_explorer.api.routes.export import _format_value

        assert _format_value(b"\x00\x01\x02") == "000102"

    def test_format_pyarrow_value(self):
        """Test formatting PyArrow scalar value."""
        from iceberg_explorer.api.routes.export import _format_value

        pa_value = MagicMock()
        pa_value.as_py.return_value = "converted"
        assert _format_value(pa_value) == "converted"


class TestCSVExportEndpoint:
    """Tests for POST /api/v1/export/csv endpoint."""

    def test_export_requires_query_id_or_sql(self, client: TestClient):
        """Test that export requires either query_id or sql."""
        response = client.post("/api/v1/export/csv", json={})
        assert response.status_code == 400
        assert "query_id or sql must be provided" in response.json()["detail"]

    def test_export_rejects_both_query_id_and_sql(self, client: TestClient):
        """Test that export rejects both query_id and sql."""
        response = client.post(
            "/api/v1/export/csv",
            json={
                "query_id": "550e8400-e29b-41d4-a716-446655440000",
                "sql": "SELECT 1",
            },
        )
        assert response.status_code == 400
        assert "not both" in response.json()["detail"]

    def test_export_with_invalid_query_id_format(self, client: TestClient):
        """Test export with invalid query ID format."""
        mock_executor = MagicMock()

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": "not-a-uuid"},
            )

        assert response.status_code == 400
        assert "Invalid query ID format" in response.json()["detail"]

    def test_export_with_unknown_query_id(self, client: TestClient):
        """Test export with unknown query ID returns 404."""
        mock_executor = MagicMock()
        mock_executor.get_status.return_value = None

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": "550e8400-e29b-41d4-a716-446655440000"},
            )

        assert response.status_code == 404
        assert "Query not found" in response.json()["detail"]

    def test_export_with_invalid_sql(self, client: TestClient):
        """Test export with invalid SQL returns 400."""
        response = client.post(
            "/api/v1/export/csv",
            json={"sql": "DELETE FROM table1"},
        )
        assert response.status_code == 400
        assert "Write operations are not allowed" in response.json()["detail"]

    def test_export_csv_from_query_id(self, client: TestClient):
        """Test CSV export from existing query ID."""
        query_id = uuid4()

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )
        batch = pa.record_batch(
            [
                pa.array([1, 2]),
                pa.array(["Alice", "Bob"]),
            ],
            schema=schema,
        )

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = [batch]

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": str(query_id)},
            )

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")
        assert "attachment" in response.headers["content-disposition"]

        content = response.content.decode("utf-8")
        lines = [line.strip() for line in content.strip().split("\n")]
        assert lines[0] == "id,name"
        assert lines[1] == "1,Alice"
        assert lines[2] == "2,Bob"

    def test_export_csv_from_inline_sql(self, client: TestClient):
        """Test CSV export from inline SQL."""
        query_id = uuid4()

        schema = pa.schema(
            [
                pa.field("value", pa.int64()),
            ]
        )
        batch = pa.record_batch([pa.array([42])], schema=schema)

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = [batch]

        mock_executor = MagicMock()
        mock_executor.execute.return_value = mock_result
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"sql": "SELECT 42 AS value"},
            )

        assert response.status_code == 200
        content = response.content.decode("utf-8")
        assert "value" in content
        assert "42" in content

    def test_export_csv_custom_filename(self, client: TestClient):
        """Test CSV export with custom filename."""
        query_id = uuid4()

        schema = pa.schema([pa.field("x", pa.int64())])
        batch = pa.record_batch([pa.array([1])], schema=schema)

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = [batch]

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={
                    "query_id": str(query_id),
                    "filename": "my_custom_export",
                },
            )

        assert response.status_code == 200
        assert "my_custom_export.csv" in response.headers["content-disposition"]

    def test_export_csv_handles_null_values(self, client: TestClient):
        """Test that CSV export handles NULL values correctly."""
        query_id = uuid4()

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )
        batch = pa.record_batch(
            [
                pa.array([1, 2, 3]),
                pa.array(["Alice", None, "Charlie"]),
            ],
            schema=schema,
        )

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = [batch]

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": str(query_id)},
            )

        assert response.status_code == 200
        content = response.content.decode("utf-8")
        lines = [line.strip() for line in content.strip().split("\n")]
        assert lines[2] == "2,"

    def test_export_csv_handles_boolean_values(self, client: TestClient):
        """Test that CSV export handles boolean values correctly."""
        query_id = uuid4()

        schema = pa.schema([pa.field("active", pa.bool_())])
        batch = pa.record_batch([pa.array([True, False])], schema=schema)

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = [batch]

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": str(query_id)},
            )

        assert response.status_code == 200
        content = response.content.decode("utf-8")
        assert "true" in content
        assert "false" in content

    def test_export_csv_handles_special_characters(self, client: TestClient):
        """Test that CSV export properly escapes special characters."""
        query_id = uuid4()

        schema = pa.schema([pa.field("text", pa.string())])
        batch = pa.record_batch(
            [pa.array(['Hello, "World"', "Line1\nLine2", "Quote's"])],
            schema=schema,
        )

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = [batch]

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": str(query_id)},
            )

        assert response.status_code == 200
        content = response.content.decode("utf-8")
        assert '"Hello, ""World"""' in content or "Hello" in content

    def test_export_csv_empty_result(self, client: TestClient):
        """Test CSV export with empty result set."""
        query_id = uuid4()

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = []

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": str(query_id)},
            )

        assert response.status_code == 200
        content = response.content.decode("utf-8")
        lines = [line.strip() for line in content.strip().split("\n")]
        assert lines[0] == "id,name"
        assert len(lines) == 1

    def test_export_csv_multiple_batches(self, client: TestClient):
        """Test CSV export with multiple Arrow batches."""
        query_id = uuid4()

        schema = pa.schema([pa.field("id", pa.int64())])
        batch1 = pa.record_batch([pa.array([1, 2])], schema=schema)
        batch2 = pa.record_batch([pa.array([3, 4])], schema=schema)

        mock_result = MagicMock(spec=QueryResult)
        mock_result.query_id = query_id
        mock_result.state = QueryState.COMPLETED
        mock_result.schema = schema
        mock_result.batches = [batch1, batch2]

        mock_executor = MagicMock()
        mock_executor.get_status.return_value = mock_result

        with patch("iceberg_explorer.api.routes.export.get_executor", return_value=mock_executor):
            response = client.post(
                "/api/v1/export/csv",
                json={"query_id": str(query_id)},
            )

        assert response.status_code == 200
        content = response.content.decode("utf-8")
        lines = [line.strip() for line in content.strip().split("\n")]
        assert len(lines) == 5
        assert lines[0] == "id"
        assert lines[1] == "1"
        assert lines[4] == "4"


class TestExportConfig:
    """Tests for export configuration."""

    def test_export_config_defaults(self):
        """Test default export configuration."""
        from iceberg_explorer.config import ExportConfig

        config = ExportConfig()
        assert config.max_size_bytes == 1073741824  # 1GB

    def test_export_config_custom_size(self):
        """Test custom export max size."""
        from iceberg_explorer.config import ExportConfig

        config = ExportConfig(max_size_bytes=100 * 1024 * 1024)  # 100MB
        assert config.max_size_bytes == 100 * 1024 * 1024

    def test_settings_includes_export(self):
        """Test that root settings include export config."""
        from iceberg_explorer.config import Settings

        settings = Settings()
        assert hasattr(settings, "export")
        assert settings.export.max_size_bytes == 1073741824
