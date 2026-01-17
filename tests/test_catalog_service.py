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
