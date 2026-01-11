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

        def mock_execute(sql):
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

        def mock_execute(sql):
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

        def mock_execute(sql):
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

        def mock_execute(sql):
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

        def mock_execute(sql):
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
