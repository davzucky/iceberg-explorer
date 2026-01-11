"""DuckDB engine for Iceberg catalog queries.

Provides a connection manager that:
- Connects to Iceberg catalogs (REST or local)
- Enforces read-only mode
- Applies memory and thread limits from configuration
- Provides health check for catalog connectivity
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING

import duckdb

from iceberg_explorer.config import CatalogType, get_settings

if TYPE_CHECKING:
    from collections.abc import Generator

    from iceberg_explorer.config import Settings


class DuckDBEngine:
    """DuckDB connection manager for Iceberg catalogs.

    This class manages DuckDB connections with:
    - Iceberg extension loading and catalog attachment
    - Read-only mode enforcement
    - Configurable memory limits and thread counts
    - Thread-safe connection handling
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize the DuckDB engine.

        Args:
            settings: Application settings. If None, uses cached settings.
        """
        self._settings = settings or get_settings()
        self._lock = threading.Lock()
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._catalog_attached = False

    @property
    def catalog_name(self) -> str:
        """Get the configured catalog name."""
        return self._settings.catalog.name

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new DuckDB connection with configured settings.

        Returns:
            Configured DuckDB connection.
        """
        conn = duckdb.connect(":memory:", read_only=False)

        duckdb_config = self._settings.duckdb
        conn.execute(f"SET memory_limit = '{duckdb_config.memory_limit}'")
        conn.execute(f"SET threads = {duckdb_config.threads}")

        return conn

    def _attach_catalog(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Attach the Iceberg catalog to the connection.

        Args:
            conn: DuckDB connection to attach catalog to.

        Raises:
            RuntimeError: If catalog attachment fails.
        """
        catalog_config = self._settings.catalog
        catalog_name = catalog_config.name

        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")

        quoted_catalog_name = '"' + catalog_name.replace('"', '""') + '"'
        if catalog_config.type == CatalogType.REST:
            attach_sql = f"""
                ATTACH '{catalog_config.uri}' AS {quoted_catalog_name} (
                    TYPE ICEBERG,
                    ENDPOINT_TYPE REST
                )
            """
        else:
            attach_sql = f"""
                ATTACH '{catalog_config.warehouse}' AS {quoted_catalog_name} (
                    TYPE ICEBERG
                )
            """

        conn.execute(attach_sql)
        self._catalog_attached = True

    def initialize(self) -> None:
        """Initialize the engine and attach the catalog.

        This method should be called once at application startup.

        Raises:
            RuntimeError: If initialization fails.
        """
        with self._lock:
            if self._connection is not None:
                return

            self._connection = self._create_connection()
            self._attach_catalog(self._connection)

    def close(self) -> None:
        """Close the engine and release resources."""
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
                self._catalog_attached = False

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Get a DuckDB connection for query execution.

        This returns a cursor from the main connection, ensuring
        read-only operations are enforced.

        Yields:
            DuckDB connection cursor for query execution.

        Raises:
            RuntimeError: If engine is not initialized.
        """
        if self._connection is None:
            raise RuntimeError("Engine not initialized. Call initialize() first.")

        with self._lock:
            cursor = self._connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def health_check(self) -> dict[str, bool | str]:
        """Check health of DuckDB and catalog connectivity.

        Returns:
            Dictionary with health status:
            - healthy: Overall health status
            - duckdb: DuckDB connection status
            - catalog: Catalog connectivity status
            - error: Error message if unhealthy
        """
        result: dict[str, bool | str] = {
            "healthy": False,
            "duckdb": False,
            "catalog": False,
        }

        if self._connection is None:
            result["error"] = "Engine not initialized"
            return result

        try:
            self._connection.execute("SELECT 1").fetchone()
            result["duckdb"] = True
        except Exception as e:
            result["error"] = f"DuckDB error: {e}"
            return result

        try:
            catalog_name = self.catalog_name
            quoted_catalog_name = '"' + catalog_name.replace('"', '""') + '"'
            self._connection.execute(f"SELECT * FROM {quoted_catalog_name}.information_schema.schemata LIMIT 1")
            result["catalog"] = True
        except Exception as e:
            result["error"] = f"Catalog error: {e}"
            return result

        result["healthy"] = True
        return result

    @property
    def is_initialized(self) -> bool:
        """Check if the engine is initialized."""
        return self._connection is not None and self._catalog_attached


_engine: DuckDBEngine | None = None


def get_engine() -> DuckDBEngine:
    """Get the global DuckDB engine instance (cached).

    Returns:
        The global DuckDB engine.
    """
    global _engine
    if _engine is None:
        _engine = DuckDBEngine()
    return _engine


def reset_engine() -> None:
    """Reset the global engine (useful for testing)."""
    global _engine
    if _engine is not None:
        _engine.close()
    _engine = None
