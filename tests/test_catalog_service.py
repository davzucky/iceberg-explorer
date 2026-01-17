"""Tests for CatalogService."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from iceberg_explorer.catalog.service import CatalogService
from iceberg_explorer.config import (
    CatalogConfig,
    CatalogType,
    Settings,
    reset_settings,
)


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global state before and after each test."""
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def mock_settings() -> Settings:
    """Create mock settings for testing."""
    return Settings(
        catalog=CatalogConfig(
            type=CatalogType.REST,
            uri="http://localhost:8181",
            warehouse="demo",
            name="test_catalog",
        ),
    )


class TestCatalogServiceListNamespaces:
    """Tests for list_namespaces method."""

    def test_list_namespaces_returns_empty_list(self, mock_settings: Settings):
        """Test listing namespaces from empty catalog returns empty list."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = []

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_namespaces()
            assert result == []
            mock_catalog.list_namespaces.assert_called_once()

    def test_list_namespaces_returns_single_namespace(self, mock_settings: Settings):
        """Test listing namespaces returns single namespace."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("db",)]

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_namespaces()
            assert result == ["db"]
            mock_catalog.list_namespaces.assert_called_once()

    def test_list_namespaces_returns_multiple_namespaces(self, mock_settings: Settings):
        """Test listing namespaces returns multiple namespaces."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [
            ("db1",),
            ("db2",),
            ("analytics",),
        ]

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_namespaces()
            assert result == ["db1", "db2", "analytics"]
            mock_catalog.list_namespaces.assert_called_once()

    def test_list_namespaces_handles_multi_level(self, mock_settings: Settings):
        """Test listing multi-level namespaces."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [
            ("db",),
            ("db", "schema1"),
            ("db", "schema2"),
            ("analytics", "public"),
        ]

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_namespaces()
            assert result == ["db", "db.schema1", "db.schema2", "analytics.public"]
            mock_catalog.list_namespaces.assert_called_once()

    def test_list_namespaces_lazy_initialization(self, mock_settings: Settings):
        """Test that list_namespaces triggers lazy initialization."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("db",)]

        with patch.object(service, "_initialize_catalog", return_value=mock_catalog):
            result = service.list_namespaces()
            assert result == ["db"]
            service._initialize_catalog.assert_called_once()


class TestCatalogServiceListTables:
    """Tests for list_tables method."""

    def test_list_tables_returns_empty_list(self, mock_settings: Settings):
        """Test listing tables from empty namespace returns empty list."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = []

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_tables("db")
            assert result == []
            mock_catalog.list_tables.assert_called_once_with(("db",))

    def test_list_tables_returns_single_table(self, mock_settings: Settings):
        """Test listing tables returns single table."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = [("db", "users")]

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_tables("db")
            assert result == ["db.users"]
            mock_catalog.list_tables.assert_called_once_with(("db",))

    def test_list_tables_returns_multiple_tables(self, mock_settings: Settings):
        """Test listing tables returns multiple tables."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = [
            ("db", "users"),
            ("db", "orders"),
            ("db", "products"),
        ]

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_tables("db")
            assert result == ["db.users", "db.orders", "db.products"]
            mock_catalog.list_tables.assert_called_once_with(("db",))

    def test_list_tables_handles_multi_level_namespace(self, mock_settings: Settings):
        """Test listing tables in multi-level namespace."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = [("db", "schema", "table1")]

        with patch.object(service, "_catalog", mock_catalog):
            result = service.list_tables("db.schema")
            assert result == ["db.schema.table1"]
            mock_catalog.list_tables.assert_called_once_with(("db", "schema"))

    def test_list_tables_raises_on_nonexistent_namespace(self, mock_settings: Settings):
        """Test listing tables from non-existent namespace raises error."""
        from pyiceberg.exceptions import NoSuchNamespaceError

        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.list_tables.side_effect = NoSuchNamespaceError()

        with patch.object(service, "_catalog", mock_catalog):
            with pytest.raises(NoSuchNamespaceError):
                service.list_tables("nonexistent")
            mock_catalog.list_tables.assert_called_once_with(("nonexistent",))


class TestCatalogServiceGetTableDetails:
    """Tests for get_table_details method."""

    def test_get_table_details_returns_basic_info(self, mock_settings: Settings):
        """Test getting table details returns basic metadata."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.metadata.location = "s3://bucket/table"
        mock_table.metadata.current_snapshot_id = 12345
        mock_table.metadata.partition_specs = []
        mock_table.metadata.snapshots = []
        mock_table.metadata.spec.return_value = None
        mock_catalog.load_table.return_value = mock_table

        with patch.object(service, "_catalog", mock_catalog):
            result = service.get_table_details("db", "users")
            assert result["location"] == "s3://bucket/table"
            assert result["snapshot_id"] == 12345
            assert result["partition_spec"] is None
            assert result["num_snapshots"] == 0
            mock_catalog.load_table.assert_called_once_with(("db", "users"))

    def test_get_table_details_with_partition_spec(self, mock_settings: Settings):
        """Test getting table details includes partition spec."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.metadata.location = "s3://bucket/table"
        mock_table.metadata.current_snapshot_id = 12345
        mock_table.metadata.snapshots = []
        mock_partition_field = MagicMock()
        mock_partition_field.source_id = 1
        mock_partition_field.name = "date"
        mock_partition_field.transform = "bucket"
        mock_partition_spec = MagicMock()
        mock_partition_spec.fields = [mock_partition_field]
        mock_table.metadata.partition_specs = [mock_partition_spec]
        mock_table.metadata.spec.return_value = mock_partition_spec

        mock_catalog.load_table.return_value = mock_table

        with patch.object(service, "_catalog", mock_catalog):
            result = service.get_table_details("db", "users")
            assert result["location"] == "s3://bucket/table"
            assert result["snapshot_id"] == 12345
            assert result["partition_spec"] == [
                {"source_id": 1, "name": "date", "transform": "bucket"}
            ]
            assert result["num_snapshots"] == 0
            mock_catalog.load_table.assert_called_once_with(("db", "users"))

    def test_get_table_details_handles_multi_level_namespace(self, mock_settings: Settings):
        """Test getting table details in multi-level namespace."""
        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.metadata.location = "s3://bucket/table"
        mock_table.metadata.current_snapshot_id = 67890
        mock_table.metadata.partition_specs = []
        mock_table.metadata.snapshots = [MagicMock(), MagicMock()]
        mock_table.metadata.spec.return_value = None
        mock_catalog.load_table.return_value = mock_table

        with patch.object(service, "_catalog", mock_catalog):
            result = service.get_table_details("db.schema", "users")
            assert result["location"] == "s3://bucket/table"
            assert result["snapshot_id"] == 67890
            assert result["num_snapshots"] == 2
            mock_catalog.load_table.assert_called_once_with(("db", "schema", "users"))

    def test_get_table_details_raises_on_nonexistent_table(self, mock_settings: Settings):
        """Test getting details from non-existent table raises error."""
        from pyiceberg.exceptions import NoSuchTableError

        service = CatalogService(settings=mock_settings)
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = NoSuchTableError()

        with patch.object(service, "_catalog", mock_catalog):
            with pytest.raises(NoSuchTableError):
                service.get_table_details("db", "nonexistent")
            mock_catalog.load_table.assert_called_once_with(("db", "nonexistent"))
