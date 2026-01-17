"""Tests for DuckDB engine."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
import pytest

from iceberg_explorer.config import (
    CatalogConfig,
    CatalogType,
    DuckDBConfig,
    Settings,
    reset_settings,
)
from iceberg_explorer.query.engine import DuckDBEngine, get_engine, reset_engine


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global state before and after each test."""
    reset_settings()
    reset_engine()
    yield
    reset_settings()
    reset_engine()


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
    )


@pytest.fixture
def rest_settings() -> Settings:
    """Create settings for REST catalog."""
    return Settings(
        catalog=CatalogConfig(
            type=CatalogType.REST,
            uri="http://localhost:8181",
            name="rest_catalog",
        ),
        duckdb=DuckDBConfig(
            memory_limit="2GB",
            threads=4,
        ),
    )


class TestDuckDBEngineInit:
    """Tests for DuckDBEngine initialization."""

    def test_engine_creation(self, mock_settings: Settings):
        """Test engine can be created with settings."""
        engine = DuckDBEngine(settings=mock_settings)
        assert engine is not None
        assert engine.catalog_name == "test_catalog"
        assert not engine.is_initialized

    def test_engine_uses_default_settings(self):
        """Test engine uses get_settings() when no settings provided."""
        engine = DuckDBEngine()
        assert engine.catalog_name == "default"


class TestDuckDBEngineConnection:
    """Tests for DuckDB connection management."""

    def test_create_connection_applies_memory_limit(self, mock_settings: Settings):
        """Test memory limit is applied to connection."""
        engine = DuckDBEngine(settings=mock_settings)
        conn = engine._create_connection()
        try:
            result = conn.execute("SELECT current_setting('memory_limit')").fetchone()
            assert result is not None
            mem_str = result[0].lower()
            assert "mib" in mem_str or "gib" in mem_str or "gb" in mem_str
        finally:
            conn.close()

    def test_create_connection_applies_threads(self, mock_settings: Settings):
        """Test thread count is applied to connection."""
        engine = DuckDBEngine(settings=mock_settings)
        conn = engine._create_connection()
        try:
            result = conn.execute("SELECT current_setting('threads')").fetchone()
            assert result is not None
            assert int(result[0]) == 2
        finally:
            conn.close()

    def test_get_connection_without_init_raises(self, mock_settings: Settings):
        """Test get_connection raises if not initialized."""
        engine = DuckDBEngine(settings=mock_settings)

        with pytest.raises(RuntimeError, match="Engine not initialized"), engine.get_connection():
            pass

    def test_close_engine(self, mock_settings: Settings):
        """Test engine can be closed."""
        engine = DuckDBEngine(settings=mock_settings)
        engine._connection = duckdb.connect(":memory:")
        engine._catalog_attached = True

        assert engine.is_initialized
        engine.close()
        assert not engine.is_initialized


class TestDuckDBEngineCatalogAttachment:
    """Tests for catalog attachment logic."""

    def test_attach_local_catalog_sql(self, mock_settings: Settings):
        """Test local catalog generates correct ATTACH SQL."""
        engine = DuckDBEngine(settings=mock_settings)
        conn = MagicMock()

        with patch.object(conn, "execute") as mock_execute:
            engine._attach_catalog(conn)

            calls = [str(call) for call in mock_execute.call_args_list]
            assert any("INSTALL iceberg" in c for c in calls)
            assert any("LOAD iceberg" in c for c in calls)
            assert any("TYPE ICEBERG" in c and "/tmp/test_warehouse" in c for c in calls)

    def test_attach_rest_catalog_sql(self, rest_settings: Settings):
        """Test REST catalog generates correct ATTACH SQL."""
        engine = DuckDBEngine(settings=rest_settings)
        conn = MagicMock()

        with patch.object(conn, "execute") as mock_execute:
            engine._attach_catalog(conn)

            calls = [str(call) for call in mock_execute.call_args_list]
            assert any("INSTALL iceberg" in c for c in calls)
            assert any("LOAD iceberg" in c for c in calls)
            assert any("ENDPOINT" in c for c in calls)
            assert any("AUTHORIZATION_TYPE 'none'" in c for c in calls)
            assert any("http://localhost:8181" in c for c in calls)


class TestDuckDBEngineHealthCheck:
    """Tests for health check functionality."""

    def test_health_check_not_initialized(self, mock_settings: Settings):
        """Test health check returns error when not initialized."""
        engine = DuckDBEngine(settings=mock_settings)

        result = engine.health_check()

        assert result["healthy"] is False
        assert result["duckdb"] is False
        assert result["catalog"] is False
        error = str(result.get("error", ""))
        assert "not initialized" in error

    def test_health_check_duckdb_ok(self, mock_settings: Settings):
        """Test health check passes for DuckDB when connection works."""
        engine = DuckDBEngine(settings=mock_settings)
        engine._connection = duckdb.connect(":memory:")
        engine._catalog_attached = True

        mock_conn = engine._connection

        result = engine.health_check()

        assert result["duckdb"] is True
        mock_conn.close()

    def test_health_check_duckdb_failure(self, mock_settings: Settings):
        """Test health check fails when DuckDB query fails."""
        engine = DuckDBEngine(settings=mock_settings)
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Connection lost")
        engine._connection = mock_conn

        result = engine.health_check()

        assert result["healthy"] is False
        assert result["duckdb"] is False
        error = str(result.get("error", ""))
        assert "DuckDB error" in error


class TestGlobalEngine:
    """Tests for global engine singleton."""

    def test_get_engine_returns_same_instance(self):
        """Test get_engine returns cached instance."""
        engine1 = get_engine()
        engine2 = get_engine()
        assert engine1 is engine2

    def test_reset_engine_clears_cache(self):
        """Test reset_engine clears the cached engine."""
        engine1 = get_engine()
        reset_engine()
        engine2 = get_engine()
        assert engine1 is not engine2

    def test_reset_engine_closes_connection(self):
        """Test reset_engine closes the connection."""
        engine = get_engine()
        engine._connection = duckdb.connect(":memory:")
        engine._catalog_attached = True

        reset_engine()

        assert engine._connection is None or not engine.is_initialized


class TestDuckDBEngineIntegration:
    """Integration tests for DuckDB engine with real DuckDB (no Iceberg)."""

    def test_connection_context_manager(self, mock_settings: Settings):
        """Test connection context manager works correctly."""
        engine = DuckDBEngine(settings=mock_settings)
        engine._connection = duckdb.connect(":memory:")
        engine._catalog_attached = True

        with engine.get_connection() as conn:
            result = conn.execute("SELECT 42").fetchone()
            assert result == (42,)

    def test_multiple_connections_sequential(self, mock_settings: Settings):
        """Test multiple sequential connection uses work."""
        engine = DuckDBEngine(settings=mock_settings)
        engine._connection = duckdb.connect(":memory:")
        engine._catalog_attached = True

        for i in range(3):
            with engine.get_connection() as conn:
                result = conn.execute(f"SELECT {i}").fetchone()
                assert result == (i,)

    def test_initialize_creates_connection(self, mock_settings: Settings):
        """Test initialize creates a connection (mocked catalog)."""
        engine = DuckDBEngine(settings=mock_settings)

        with patch.object(engine, "_attach_catalog"):
            engine.initialize()

        assert engine._connection is not None
        engine.close()

    def test_initialize_idempotent(self, mock_settings: Settings):
        """Test calling initialize multiple times is safe."""
        engine = DuckDBEngine(settings=mock_settings)

        with patch.object(engine, "_attach_catalog"):
            engine.initialize()
            conn1 = engine._connection
            engine.initialize()
            conn2 = engine._connection

        assert conn1 is conn2
        engine.close()
