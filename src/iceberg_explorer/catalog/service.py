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

    def list_namespaces(self, parent: str | None = None) -> list[str]:
        """List namespaces in the catalog.

        Args:
            parent: Optional parent namespace identifier (e.g., "db" or "db.schema").
                    If not provided, lists top-level namespaces.

        Returns:
            List of namespace identifiers as strings. Multi-level namespaces
            are represented with dot separators (e.g., "db.schema").
            Returns an empty list if the catalog has no namespaces.

        Raises:
            RuntimeError: If the catalog type is not supported.
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """
        if parent:
            parent_tuple = tuple(parent.split("."))
            namespaces = self.catalog.list_namespaces(namespace=parent_tuple)
        else:
            namespaces = self.catalog.list_namespaces()
        return [".".join(ns) for ns in namespaces]

    def list_tables(self, namespace: str) -> list[str]:
        """List tables in a namespace.

        Args:
            namespace: Namespace identifier (e.g., "db" or "db.schema").

        Returns:
            List of table identifiers as strings.

        Raises:
            RuntimeError: If catalog type is not supported.
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """
        namespace_tuple = tuple(namespace.split("."))
        tables = self.catalog.list_tables(namespace_tuple)
        return [".".join(table) for table in tables]

    def get_table_details(self, namespace: str, table_name: str) -> dict:
        """Get details for a table.

        Args:
            namespace: Namespace identifier (e.g., "db" or "db.schema").
            table_name: Name of the table.

        Returns:
            Dictionary containing table metadata including:
            - location: Table location (URI)
            - snapshot_id: Current snapshot ID
            - partition_spec: Partition specification
            - snapshots: List of snapshot dictionaries with sequence_number,
              snapshot_id, timestamp_ms, and manifest_list

        Raises:
            RuntimeError: If the catalog type is not supported.
            NoSuchTableError: If a table does not exist.
        """
        table_identifier = (*namespace.split("."), table_name)
        table = self.catalog.load_table(table_identifier)

        partition_spec_info = None
        if table.metadata.partition_specs:
            spec = table.metadata.spec()
            if spec:
                partition_spec_info = [
                    {
                        "source_id": field.source_id,
                        "name": field.name,
                        "transform": str(field.transform),
                    }
                    for field in spec.fields
                ]

        snapshots = []
        for snap in table.metadata.snapshots:
            snapshot_dict = {
                "sequence_number": snap.sequence_number,
                "snapshot_id": snap.snapshot_id,
                "timestamp_ms": int(snap.timestamp_ms),
                "manifest_list": snap.manifest_list,
            }
            snapshots.append(snapshot_dict)

        return {
            "location": table.metadata.location,
            "snapshot_id": table.metadata.current_snapshot_id,
            "partition_spec": partition_spec_info,
            "snapshots": snapshots,
        }

    def get_table_schema(self, namespace: str, table_name: str) -> dict:
        """Get schema for a table.

        Args:
            namespace: Namespace identifier (e.g., "db" or "db.schema").
            table_name: Name of the table.

        Returns:
            Dictionary containing schema fields with names, types, and nullability.

        Raises:
            RuntimeError: If catalog type is not supported.
            NoSuchTableError: If a table does not exist.
        """
        table_identifier = (*namespace.split("."), table_name)
        table = self.catalog.load_table(table_identifier)
        schema = table.schema()

        fields = []
        for field in schema.fields:
            fields.append(
                {
                    "name": field.name,
                    "type": str(field.field_type),
                    "nullable": field.optional,
                }
            )

        return {
            "schema_id": schema.schema_id,
            "fields": fields,
        }

    def close(self) -> None:
        """Close the catalog connection.

        This method cleans up resources and should be called when
        the service is no longer needed.
        """
        if self._catalog is not None:
            self._catalog = None


_catalog_service: CatalogService | None = None


def get_catalog_service() -> CatalogService:
    """Get the global catalog service instance (cached).

    Returns:
        The global catalog service.
    """
    global _catalog_service
    if _catalog_service is None:
        _catalog_service = CatalogService()
    return _catalog_service


def reset_catalog_service() -> None:
    """Reset global catalog service (useful for testing)."""
    global _catalog_service
    if _catalog_service is not None:
        _catalog_service.close()
    _catalog_service = None
