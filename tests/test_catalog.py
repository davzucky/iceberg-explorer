"""Tests for catalog API routes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from iceberg_explorer.config import reset_settings
from iceberg_explorer.main import app
from iceberg_explorer.query.engine import reset_engine
from iceberg_explorer.query.executor import reset_executor


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


@pytest.fixture
def mock_engine():
    """Create a mock engine that returns namespace data."""
    engine = MagicMock()
    engine.is_initialized = True
    engine.catalog_name = "test_catalog"
    return engine


class TestListNamespacesModels:
    """Tests for namespace-related Pydantic models."""

    def test_namespace_identifier_model(self):
        """Test NamespaceIdentifier model."""
        from iceberg_explorer.models.catalog import NamespaceIdentifier

        ns = NamespaceIdentifier(namespace=["accounting", "tax"])
        assert ns.namespace == ["accounting", "tax"]

    def test_list_namespaces_response_model(self):
        """Test ListNamespacesResponse model."""
        from iceberg_explorer.models.catalog import ListNamespacesResponse

        response = ListNamespacesResponse(
            namespaces=[["accounting"], ["engineering"]],
            next_page_token=None,
        )
        assert len(response.namespaces) == 2
        assert response.namespaces[0] == ["accounting"]
        assert response.next_page_token is None

    def test_list_namespaces_response_empty(self):
        """Test ListNamespacesResponse with empty list."""
        from iceberg_explorer.models.catalog import ListNamespacesResponse

        response = ListNamespacesResponse()
        assert response.namespaces == []
        assert response.next_page_token is None


class TestListNamespacesEndpoint:
    """Tests for GET /api/v1/catalog/namespaces endpoint."""

    def test_list_top_level_namespaces(self, client: TestClient, mock_engine: MagicMock):
        """Test listing top-level namespaces."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("accounting",),
            ("engineering",),
            ("information_schema",),
            ("main",),
        ]
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces")

        assert response.status_code == 200
        data = response.json()
        assert "namespaces" in data
        assert ["accounting"] in data["namespaces"]
        assert ["engineering"] in data["namespaces"]
        assert ["information_schema"] not in data["namespaces"]
        assert ["main"] not in data["namespaces"]

    def test_list_empty_namespaces(self, client: TestClient, mock_engine: MagicMock):
        """Test listing namespaces when catalog is empty."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces")

        assert response.status_code == 200
        data = response.json()
        assert data["namespaces"] == []

    def test_list_child_namespaces(self, client: TestClient, mock_engine: MagicMock):
        """Test listing child namespaces under a parent."""
        mock_conn = MagicMock()
        call_count = [0]

        def mock_execute(sql):
            call_count[0] += 1
            result = MagicMock()
            if "LIMIT 1" in sql:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = [
                    ("tax",),
                    ("payroll",),
                ]
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces?parent=accounting")

        assert response.status_code == 200
        data = response.json()
        assert ["accounting", "tax"] in data["namespaces"]
        assert ["accounting", "payroll"] in data["namespaces"]

    def test_list_nested_namespaces_with_separator(self, client: TestClient, mock_engine: MagicMock):
        """Test listing namespaces with unit separator for multi-level parent."""
        mock_conn = MagicMock()

        def mock_execute(sql):
            result = MagicMock()
            if "LIMIT 1" in sql:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = [("2023",), ("2024",)]
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces?parent=accounting%1Ftax")

        assert response.status_code == 200
        data = response.json()
        assert ["accounting", "tax", "2023"] in data["namespaces"]
        assert ["accounting", "tax", "2024"] in data["namespaces"]

    def test_parent_namespace_not_found(self, client: TestClient, mock_engine: MagicMock):
        """Test 404 when parent namespace doesn't exist."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Catalog Error: Schema with name does not exist")
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces?parent=nonexistent")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower() or "Namespace" in data["detail"]

    def test_page_size_parameter(self, client: TestClient, mock_engine: MagicMock):
        """Test page-size query parameter validation."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces?page-size=50")

        assert response.status_code == 200

    def test_page_size_validation_min(self, client: TestClient):
        """Test page-size minimum validation."""
        response = client.get("/api/v1/catalog/namespaces?page-size=0")
        assert response.status_code == 422

    def test_page_size_validation_max(self, client: TestClient):
        """Test page-size maximum validation."""
        response = client.get("/api/v1/catalog/namespaces?page-size=10000")
        assert response.status_code == 422

    def test_engine_initialization(self, client: TestClient, mock_engine: MagicMock):
        """Test engine is initialized if not already."""
        mock_engine.is_initialized = False
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces")

        assert response.status_code == 200
        mock_engine.initialize.assert_called_once()


class TestHelperFunctions:
    """Tests for helper functions in catalog routes."""

    def test_parse_namespace_empty(self):
        """Test parsing empty namespace."""
        from iceberg_explorer.api.routes.catalog import _parse_namespace

        assert _parse_namespace(None) == []
        assert _parse_namespace("") == []

    def test_parse_namespace_single(self):
        """Test parsing single-level namespace."""
        from iceberg_explorer.api.routes.catalog import _parse_namespace

        assert _parse_namespace("accounting") == ["accounting"]

    def test_parse_namespace_multi_level(self):
        """Test parsing multi-level namespace with unit separator."""
        from iceberg_explorer.api.routes.catalog import _parse_namespace

        assert _parse_namespace("accounting\x1ftax") == ["accounting", "tax"]
        assert _parse_namespace("a\x1fb\x1fc") == ["a", "b", "c"]

    def test_quote_identifier_simple(self):
        """Test quoting simple identifier."""
        from iceberg_explorer.api.routes.catalog import _quote_identifier

        assert _quote_identifier("accounting") == '"accounting"'

    def test_quote_identifier_with_quotes(self):
        """Test quoting identifier that contains quotes."""
        from iceberg_explorer.api.routes.catalog import _quote_identifier

        assert _quote_identifier('test"name') == '"test""name"'

    def test_build_namespace_path_root(self):
        """Test building path for root catalog."""
        from iceberg_explorer.api.routes.catalog import _build_namespace_path

        assert _build_namespace_path("catalog", []) == '"catalog"'

    def test_build_namespace_path_nested(self):
        """Test building path for nested namespace."""
        from iceberg_explorer.api.routes.catalog import _build_namespace_path

        result = _build_namespace_path("catalog", ["ns1", "ns2"])
        assert result == '"catalog"."ns1"."ns2"'


class TestListTablesEndpoint:
    """Tests for GET /api/v1/catalog/namespaces/{namespace}/tables endpoint."""

    def test_list_tables_in_namespace(self, client: TestClient, mock_engine: MagicMock):
        """Test listing tables in a namespace."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 1" in sql:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = [
                    ("transactions",),
                    ("users",),
                ]
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces/accounting/tables")

        assert response.status_code == 200
        data = response.json()
        assert "identifiers" in data
        assert len(data["identifiers"]) == 2
        assert data["identifiers"][0]["namespace"] == ["accounting"]
        assert data["identifiers"][0]["name"] == "transactions"
        assert data["identifiers"][1]["namespace"] == ["accounting"]
        assert data["identifiers"][1]["name"] == "users"

    def test_list_tables_empty_namespace(self, client: TestClient, mock_engine: MagicMock):
        """Test listing tables when namespace has no tables."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 1" in sql:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces/empty_ns/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["identifiers"] == []

    def test_list_tables_nested_namespace(self, client: TestClient, mock_engine: MagicMock):
        """Test listing tables in a nested namespace using unit separator."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 1" in sql:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = [("tax_records",)]
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces/accounting%1Ftax/tables")

        assert response.status_code == 200
        data = response.json()
        assert len(data["identifiers"]) == 1
        assert data["identifiers"][0]["namespace"] == ["accounting", "tax"]
        assert data["identifiers"][0]["name"] == "tax_records"

    def test_list_tables_namespace_not_found(self, client: TestClient, mock_engine: MagicMock):
        """Test 404 when namespace doesn't exist."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Catalog Error: Schema with name does not exist")
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces/nonexistent/tables")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower() or "Namespace" in data["detail"]

    def test_list_tables_deeply_nested_namespace(self, client: TestClient, mock_engine: MagicMock):
        """Test listing tables in a deeply nested namespace."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 1" in sql:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = [("report",)]
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces/a%1Fb%1Fc/tables")

        assert response.status_code == 200
        data = response.json()
        assert len(data["identifiers"]) == 1
        assert data["identifiers"][0]["namespace"] == ["a", "b", "c"]
        assert data["identifiers"][0]["name"] == "report"

    def test_list_tables_engine_initialization(self, client: TestClient, mock_engine: MagicMock):
        """Test engine is initialized if not already."""
        mock_engine.is_initialized = False
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 1" in sql:
                result.fetchall.return_value = [(1,)]
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/namespaces/test/tables")

        assert response.status_code == 200
        mock_engine.initialize.assert_called_once()


class TestTableIdentifierModel:
    """Tests for TableIdentifier model."""

    def test_table_identifier_model(self):
        """Test TableIdentifier model."""
        from iceberg_explorer.models.catalog import TableIdentifier

        table = TableIdentifier(namespace=["accounting", "tax"], name="transactions")
        assert table.namespace == ["accounting", "tax"]
        assert table.name == "transactions"

    def test_list_tables_response_model(self):
        """Test ListTablesResponse model."""
        from iceberg_explorer.models.catalog import ListTablesResponse, TableIdentifier

        response = ListTablesResponse(
            identifiers=[
                TableIdentifier(namespace=["ns1"], name="table1"),
                TableIdentifier(namespace=["ns1"], name="table2"),
            ],
            next_page_token=None,
        )
        assert len(response.identifiers) == 2
        assert response.identifiers[0].name == "table1"
        assert response.next_page_token is None

    def test_list_tables_response_empty(self):
        """Test ListTablesResponse with empty list."""
        from iceberg_explorer.models.catalog import ListTablesResponse

        response = ListTablesResponse()
        assert response.identifiers == []
        assert response.next_page_token is None


class TestTableDetailsModels:
    """Tests for table details-related Pydantic models."""

    def test_snapshot_model(self):
        """Test Snapshot model."""
        from iceberg_explorer.models.catalog import Snapshot

        snapshot = Snapshot(
            sequence_number=1,
            snapshot_id=12345678901234567,
            timestamp_ms=1699999999000,
            manifest_list="s3://bucket/metadata/snap-123.avro",
        )
        assert snapshot.sequence_number == 1
        assert snapshot.snapshot_id == 12345678901234567
        assert snapshot.timestamp_ms == 1699999999000
        assert snapshot.manifest_list == "s3://bucket/metadata/snap-123.avro"

    def test_snapshot_model_optional_manifest(self):
        """Test Snapshot model with optional manifest_list."""
        from iceberg_explorer.models.catalog import Snapshot

        snapshot = Snapshot(
            sequence_number=1,
            snapshot_id=123,
            timestamp_ms=1699999999000,
        )
        assert snapshot.manifest_list is None

    def test_partition_field_model(self):
        """Test PartitionField model."""
        from iceberg_explorer.models.catalog import PartitionField

        field = PartitionField(
            source_id=1,
            field_id=1000,
            name="dt",
            transform="day",
        )
        assert field.source_id == 1
        assert field.field_id == 1000
        assert field.name == "dt"
        assert field.transform == "day"

    def test_partition_spec_model(self):
        """Test PartitionSpec model."""
        from iceberg_explorer.models.catalog import PartitionField, PartitionSpec

        spec = PartitionSpec(
            spec_id=0,
            fields=[
                PartitionField(source_id=1, field_id=1000, name="dt", transform="day"),
            ],
        )
        assert spec.spec_id == 0
        assert len(spec.fields) == 1
        assert spec.fields[0].name == "dt"

    def test_sort_field_model(self):
        """Test SortField model."""
        from iceberg_explorer.models.catalog import SortField

        field = SortField(
            source_id=1,
            transform="identity",
            direction="asc",
            null_order="nulls_first",
        )
        assert field.source_id == 1
        assert field.transform == "identity"
        assert field.direction == "asc"
        assert field.null_order == "nulls_first"

    def test_sort_order_model(self):
        """Test SortOrder model."""
        from iceberg_explorer.models.catalog import SortField, SortOrder

        order = SortOrder(
            order_id=0,
            fields=[
                SortField(
                    source_id=1,
                    transform="identity",
                    direction="asc",
                    null_order="nulls_first",
                ),
            ],
        )
        assert order.order_id == 0
        assert len(order.fields) == 1
        assert order.fields[0].direction == "asc"

    def test_table_details_model(self):
        """Test TableDetails model."""
        from iceberg_explorer.models.catalog import Snapshot, TableDetails

        details = TableDetails(
            namespace=["accounting", "tax"],
            name="transactions",
            location="s3://bucket/accounting/tax/transactions",
            format="ICEBERG",
            current_snapshot=Snapshot(
                sequence_number=2,
                snapshot_id=123456,
                timestamp_ms=1699999999000,
            ),
            snapshots=[
                Snapshot(sequence_number=1, snapshot_id=123455, timestamp_ms=1699999998000),
                Snapshot(sequence_number=2, snapshot_id=123456, timestamp_ms=1699999999000),
            ],
        )
        assert details.namespace == ["accounting", "tax"]
        assert details.name == "transactions"
        assert details.location == "s3://bucket/accounting/tax/transactions"
        assert details.format == "ICEBERG"
        assert details.current_snapshot is not None
        assert details.current_snapshot.sequence_number == 2
        assert len(details.snapshots) == 2

    def test_table_details_model_defaults(self):
        """Test TableDetails model with defaults."""
        from iceberg_explorer.models.catalog import TableDetails

        details = TableDetails(
            namespace=["ns1"],
            name="table1",
        )
        assert details.location is None
        assert details.format == "ICEBERG"
        assert details.partition_spec is None
        assert details.sort_order is None
        assert details.current_snapshot is None
        assert details.snapshots == []


class TestTableDetailsEndpoint:
    """Tests for GET /api/v1/catalog/tables/{table_path} endpoint."""

    def test_get_table_details_basic(self, client: TestClient, mock_engine: MagicMock):
        """Test getting table details returns correct structure."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
                result.fetchall.return_value = []
            elif "iceberg_snapshots" in sql:
                result.fetchall.return_value = [
                    (1, 12345678901234567, 1699999998000, "s3://bucket/metadata/snap-1.avro"),
                    (2, 12345678901234568, 1699999999000, "s3://bucket/metadata/snap-2.avro"),
                ]
            elif "iceberg_metadata" in sql:
                result.fetchone.return_value = (
                    "s3://bucket/accounting/transactions/metadata/manifest.avro",
                    1,
                    "DATA",
                    "ADDED",
                    "EXISTING",
                    "data/file.parquet",
                    "PARQUET",
                    100,
                )
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/accounting.transactions")

        assert response.status_code == 200
        data = response.json()
        assert data["namespace"] == ["accounting"]
        assert data["name"] == "transactions"
        assert data["format"] == "ICEBERG"
        assert "location" in data
        assert "snapshots" in data
        assert "current_snapshot" in data

    def test_get_table_details_with_snapshots(self, client: TestClient, mock_engine: MagicMock):
        """Test table details includes snapshot history."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "iceberg_snapshots" in sql:
                result.fetchall.return_value = [
                    (1, 111, 1699999998000, "s3://bucket/metadata/snap-1.avro"),
                    (2, 222, 1699999999000, "s3://bucket/metadata/snap-2.avro"),
                ]
            elif "iceberg_metadata" in sql:
                result.fetchone.return_value = (
                    "s3://bucket/ns/table/metadata/manifest.avro",
                    1,
                    "DATA",
                    "ADDED",
                    "EXISTING",
                    "data/file.parquet",
                    "PARQUET",
                    100,
                )
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.table")

        assert response.status_code == 200
        data = response.json()
        assert len(data["snapshots"]) == 2
        assert data["snapshots"][0]["sequence_number"] == 1
        assert data["snapshots"][1]["sequence_number"] == 2
        assert data["current_snapshot"]["sequence_number"] == 2

    def test_get_table_details_nested_namespace(self, client: TestClient, mock_engine: MagicMock):
        """Test table details with nested namespace using unit separator."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "iceberg_snapshots" in sql:
                result.fetchall.return_value = []
            elif "iceberg_metadata" in sql:
                result.fetchone.return_value = None
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/accounting%1Ftax.records")

        assert response.status_code == 200
        data = response.json()
        assert data["namespace"] == ["accounting", "tax"]
        assert data["name"] == "records"

    def test_get_table_details_not_found(self, client: TestClient, mock_engine: MagicMock):
        """Test 404 when table doesn't exist."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Table does not exist")
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/nonexistent.table")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower() or "Table" in data["detail"]

    def test_get_table_details_invalid_format(self, client: TestClient, mock_engine: MagicMock):
        """Test 400 when path format is invalid (no dot separator)."""
        mock_engine.get_connection.return_value.__enter__.return_value = MagicMock()
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/invalidpath")

        assert response.status_code == 400
        data = response.json()
        assert "Invalid" in data["detail"]

    def test_get_table_details_engine_initialization(self, client: TestClient, mock_engine: MagicMock):
        """Test engine is initialized if not already."""
        mock_engine.is_initialized = False
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "iceberg_snapshots" in sql:
                result.fetchall.return_value = []
            elif "iceberg_metadata" in sql:
                result.fetchone.return_value = None
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.table")

        assert response.status_code == 200
        mock_engine.initialize.assert_called_once()

    def test_get_table_details_no_snapshots(self, client: TestClient, mock_engine: MagicMock):
        """Test table details when no snapshots available."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "iceberg_snapshots" in sql:
                result.fetchall.return_value = []
            elif "iceberg_metadata" in sql:
                result.fetchone.return_value = None
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.emptytable")

        assert response.status_code == 200
        data = response.json()
        assert data["snapshots"] == []
        assert data["current_snapshot"] is None


class TestParseTablePath:
    """Tests for _parse_table_path helper function."""

    def test_parse_simple_path(self):
        """Test parsing simple namespace.table path."""
        from iceberg_explorer.api.routes.catalog import _parse_table_path

        namespace, table = _parse_table_path("accounting.transactions")
        assert namespace == ["accounting"]
        assert table == "transactions"

    def test_parse_nested_namespace_path(self):
        """Test parsing nested namespace path with unit separator."""
        from iceberg_explorer.api.routes.catalog import _parse_table_path

        namespace, table = _parse_table_path("accounting\x1ftax.records")
        assert namespace == ["accounting", "tax"]
        assert table == "records"

    def test_parse_deeply_nested_path(self):
        """Test parsing deeply nested namespace path."""
        from iceberg_explorer.api.routes.catalog import _parse_table_path

        namespace, table = _parse_table_path("a\x1fb\x1fc.table")
        assert namespace == ["a", "b", "c"]
        assert table == "table"

    def test_parse_invalid_no_dot(self):
        """Test parsing fails when no dot separator."""
        from iceberg_explorer.api.routes.catalog import _parse_table_path

        with pytest.raises(ValueError, match="Invalid table path format"):
            _parse_table_path("invalidpath")

    def test_parse_invalid_empty_namespace(self):
        """Test parsing fails when namespace is empty."""
        from iceberg_explorer.api.routes.catalog import _parse_table_path

        with pytest.raises(ValueError, match="Invalid namespace"):
            _parse_table_path(".table")


class TestSchemaModels:
    """Tests for schema-related Pydantic models."""

    def test_column_statistics_model(self):
        """Test ColumnStatistics model with all fields."""
        from iceberg_explorer.models.catalog import ColumnStatistics

        stats = ColumnStatistics(
            null_count=100,
            min_value="0",
            max_value="999",
        )
        assert stats.null_count == 100
        assert stats.min_value == "0"
        assert stats.max_value == "999"

    def test_column_statistics_defaults(self):
        """Test ColumnStatistics with default values."""
        from iceberg_explorer.models.catalog import ColumnStatistics

        stats = ColumnStatistics()
        assert stats.null_count is None
        assert stats.min_value is None
        assert stats.max_value is None

    def test_schema_field_model(self):
        """Test SchemaField model with all fields."""
        from iceberg_explorer.models.catalog import ColumnStatistics, SchemaField

        stats = ColumnStatistics(null_count=5)
        field = SchemaField(
            field_id=1,
            name="user_id",
            type="BIGINT",
            nullable=False,
            is_partition_column=True,
            statistics=stats,
        )
        assert field.field_id == 1
        assert field.name == "user_id"
        assert field.type == "BIGINT"
        assert field.nullable is False
        assert field.is_partition_column is True
        assert field.statistics.null_count == 5

    def test_schema_field_defaults(self):
        """Test SchemaField with default values."""
        from iceberg_explorer.models.catalog import SchemaField

        field = SchemaField(
            field_id=1,
            name="value",
            type="VARCHAR",
        )
        assert field.nullable is True
        assert field.is_partition_column is False
        assert field.statistics is None

    def test_table_schema_response_model(self):
        """Test TableSchemaResponse model."""
        from iceberg_explorer.models.catalog import SchemaField, TableSchemaResponse

        fields = [
            SchemaField(field_id=1, name="id", type="INTEGER"),
            SchemaField(field_id=2, name="name", type="VARCHAR"),
        ]
        response = TableSchemaResponse(
            namespace=["accounting"],
            name="users",
            schema_id=1,
            fields=fields,
        )
        assert response.namespace == ["accounting"]
        assert response.name == "users"
        assert response.schema_id == 1
        assert len(response.fields) == 2

    def test_table_schema_response_defaults(self):
        """Test TableSchemaResponse with defaults."""
        from iceberg_explorer.models.catalog import TableSchemaResponse

        response = TableSchemaResponse(
            namespace=["test"],
            name="empty_table",
        )
        assert response.schema_id == 0
        assert response.fields == []


class TestTableSchemaEndpoint:
    """Tests for GET /api/v1/catalog/tables/{table_path}/schema endpoint."""

    def test_get_table_schema_basic(self, client: TestClient, mock_engine: MagicMock):
        """Test getting table schema returns correct structure."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "information_schema.columns" in sql:
                result.fetchall.return_value = [
                    ("id", "INTEGER", 1, "NO"),
                    ("name", "VARCHAR", 2, "YES"),
                    ("created_at", "TIMESTAMP", 3, "YES"),
                ]
            elif "iceberg_metadata" in sql:
                result.fetchall.return_value = []
                result.description = []
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/accounting.transactions/schema")

        assert response.status_code == 200
        data = response.json()
        assert data["namespace"] == ["accounting"]
        assert data["name"] == "transactions"
        assert "fields" in data
        assert len(data["fields"]) == 3
        assert data["fields"][0]["name"] == "id"
        assert data["fields"][0]["type"] == "INTEGER"
        assert data["fields"][0]["nullable"] is False

    def test_get_table_schema_with_various_types(self, client: TestClient, mock_engine: MagicMock):
        """Test schema with various column types."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "information_schema.columns" in sql:
                result.fetchall.return_value = [
                    ("int_col", "INTEGER", 1, "YES"),
                    ("bigint_col", "BIGINT", 2, "YES"),
                    ("double_col", "DOUBLE", 3, "YES"),
                    ("string_col", "VARCHAR", 4, "YES"),
                    ("bool_col", "BOOLEAN", 5, "YES"),
                    ("date_col", "DATE", 6, "YES"),
                    ("ts_col", "TIMESTAMP", 7, "YES"),
                    ("decimal_col", "DECIMAL(10,2)", 8, "YES"),
                    ("binary_col", "BLOB", 9, "YES"),
                    ("list_col", "INTEGER[]", 10, "YES"),
                ]
            elif "iceberg_metadata" in sql:
                result.fetchall.return_value = []
                result.description = []
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.types_table/schema")

        assert response.status_code == 200
        data = response.json()
        assert len(data["fields"]) == 10

        type_map = {f["name"]: f["type"] for f in data["fields"]}
        assert type_map["int_col"] == "INTEGER"
        assert type_map["bigint_col"] == "BIGINT"
        assert type_map["double_col"] == "DOUBLE"
        assert type_map["string_col"] == "VARCHAR"
        assert type_map["bool_col"] == "BOOLEAN"
        assert type_map["date_col"] == "DATE"
        assert type_map["ts_col"] == "TIMESTAMP"
        assert type_map["decimal_col"] == "DECIMAL(10,2)"
        assert type_map["binary_col"] == "BLOB"
        assert type_map["list_col"] == "INTEGER[]"

    def test_get_table_schema_partition_columns(self, client: TestClient, mock_engine: MagicMock):
        """Test schema marks partition columns."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "information_schema.columns" in sql:
                result.fetchall.return_value = [
                    ("id", "INTEGER", 1, "NO"),
                    ("event_date", "DATE", 2, "YES"),
                    ("region", "VARCHAR", 3, "YES"),
                ]
            elif "iceberg_metadata" in sql:
                result.fetchall.return_value = [
                    ("path/file1.parquet", 1, "DATA", "ADDED", "EXISTING", "data/file.parquet", "PARQUET", 100, "event_date=2024-01-01, region=us-east"),
                ]
                result.description = [
                    ("manifest_path",), ("manifest_sequence_number",), ("manifest_content",),
                    ("status",), ("content",), ("file_path",), ("file_format",), ("record_count",), ("partition_value",),
                ]
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.partitioned_table/schema")

        assert response.status_code == 200
        data = response.json()

        partition_map = {f["name"]: f["is_partition_column"] for f in data["fields"]}
        assert partition_map["id"] is False
        assert partition_map["event_date"] is True
        assert partition_map["region"] is True

    def test_get_table_schema_nested_namespace(self, client: TestClient, mock_engine: MagicMock):
        """Test schema for table with nested namespace."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "information_schema.columns" in sql:
                result.fetchall.return_value = [
                    ("col1", "VARCHAR", 1, "YES"),
                ]
            elif "iceberg_metadata" in sql:
                result.fetchall.return_value = []
                result.description = []
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/accounting%1Ftax.records/schema")

        assert response.status_code == 200
        data = response.json()
        assert data["namespace"] == ["accounting", "tax"]
        assert data["name"] == "records"

    def test_get_table_schema_table_not_found(self, client: TestClient, mock_engine: MagicMock):
        """Test 404 when table doesn't exist."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Table does not exist")
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/nonexistent.table/schema")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower() or "Table" in data["detail"]

    def test_get_table_schema_invalid_path(self, client: TestClient, mock_engine: MagicMock):
        """Test 400 when path format is invalid."""
        mock_engine.get_connection.return_value.__enter__.return_value = MagicMock()
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/invalidpath/schema")

        assert response.status_code == 400
        data = response.json()
        assert "Invalid" in data["detail"]

    def test_get_table_schema_engine_initialization(self, client: TestClient, mock_engine: MagicMock):
        """Test engine is initialized if not already."""
        mock_engine.is_initialized = False
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "information_schema.columns" in sql:
                result.fetchall.return_value = [("col1", "VARCHAR", 1, "YES")]
            elif "iceberg_metadata" in sql:
                result.fetchall.return_value = []
                result.description = []
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.table/schema")

        assert response.status_code == 200
        mock_engine.initialize.assert_called_once()

    def test_get_table_schema_empty_table(self, client: TestClient, mock_engine: MagicMock):
        """Test schema for table with no columns (edge case)."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "information_schema.columns" in sql:
                result.fetchall.return_value = []
            elif "iceberg_metadata" in sql:
                result.fetchall.return_value = []
                result.description = []
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.empty_table/schema")

        assert response.status_code == 200
        data = response.json()
        assert data["fields"] == []

    def test_get_table_schema_nullable_field_types(self, client: TestClient, mock_engine: MagicMock):
        """Test nullable field detection for various is_nullable values."""
        mock_conn = MagicMock()

        def mock_execute(sql, params=None):
            result = MagicMock()
            if "LIMIT 0" in sql:
                result.fetchone.return_value = None
            elif "information_schema.columns" in sql:
                result.fetchall.return_value = [
                    ("nullable_yes", "VARCHAR", 1, "YES"),
                    ("nullable_no", "INTEGER", 2, "NO"),
                    ("nullable_null", "DOUBLE", 3, None),
                ]
            elif "iceberg_metadata" in sql:
                result.fetchall.return_value = []
                result.description = []
            else:
                result.fetchall.return_value = []
            return result

        mock_conn.execute.side_effect = mock_execute
        mock_engine.get_connection.return_value.__enter__.return_value = mock_conn
        mock_engine.get_connection.return_value.__exit__.return_value = None

        with patch("iceberg_explorer.api.routes.catalog.get_engine", return_value=mock_engine):
            response = client.get("/api/v1/catalog/tables/ns.table/schema")

        assert response.status_code == 200
        data = response.json()

        nullable_map = {f["name"]: f["nullable"] for f in data["fields"]}
        assert nullable_map["nullable_yes"] is True
        assert nullable_map["nullable_no"] is False
        assert nullable_map["nullable_null"] is True
