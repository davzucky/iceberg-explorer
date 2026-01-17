"""Catalog service for Iceberg metadata operations.

This module provides a service wrapper around PyIceberg catalog operations,
handling connection management and lazy initialization.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import load_catalog

from iceberg_explorer.config import CatalogType, get_settings

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

    from iceberg_explorer.config import Settings


class CatalogService:
    """Service for Iceberg catalog operations using PyIceberg.

    This class manages PyIceberg catalog connections with:
    - Lazy initialization on first use
    - Configuration-driven catalog setup (REST or local)
    - S3/credential passthrough support

    The service is designed to be created once and reused throughout
    the application lifecycle (typically via dependency injection).
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize the catalog service.

        Args:
            settings: Application settings. If None, uses cached settings.
        """
        self._settings = settings or get_settings()
        self._catalog: Catalog | None = None

    def _build_catalog_properties(self) -> dict[str, str]:
        """Build catalog properties from configuration.

        Returns:
            Dictionary of catalog properties for PyIceberg.
        """
        props = {
            "uri": self._settings.catalog.uri,
            "warehouse": self._settings.catalog.warehouse,
        }

        if self._settings.catalog.credential:
            props["credential"] = self._settings.catalog.credential

        s3_config = self._settings.catalog.s3
        if s3_config.endpoint:
            props["s3.endpoint"] = s3_config.endpoint
        if s3_config.access_key_id:
            props["s3.access-key-id"] = s3_config.access_key_id
        if s3_config.secret_access_key:
            props["s3.secret-access-key"] = s3_config.secret_access_key
        if s3_config.region:
            props["s3.region"] = s3_config.region

        return props

    @property
    def catalog(self) -> Catalog:
        """Get the PyIceberg catalog instance (lazy initialization).

        Returns:
            PyIceberg Catalog instance.

        Raises:
            RuntimeError: If catalog type is not supported.
        """
        if self._catalog is None:
            self._catalog = self._initialize_catalog()
        return self._catalog

    def _initialize_catalog(self) -> Catalog:
        """Initialize the PyIceberg catalog.

        Returns:
            Initialized PyIceberg catalog instance.

        Raises:
            RuntimeError: If catalog type is not supported.
        """
        catalog_type = self._settings.catalog.type

        if catalog_type == CatalogType.REST:
            props = self._build_catalog_properties()
            catalog_name = self._settings.catalog.name
            return load_catalog(catalog_name, **props)
        else:
            raise RuntimeError(f"Unsupported catalog type: {catalog_type}")

    def list_namespaces(self) -> list[str]:
        """List all namespaces in the catalog.

        Returns:
            List of namespace identifiers as strings. Multi-level namespaces
            are represented with dot separators (e.g., "db.schema").
            Returns an empty list if the catalog has no namespaces.

        Raises:
            RuntimeError: If catalog type is not supported.
        """
        namespaces = self.catalog.list_namespaces()
        return [".".join(ns) for ns in namespaces]

    def close(self) -> None:
        """Close the catalog connection.

        This method cleans up resources and should be called when
        the service is no longer needed.
        """
        if self._catalog is not None:
            self._catalog = None
