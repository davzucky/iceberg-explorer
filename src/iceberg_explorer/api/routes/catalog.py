"""Catalog API routes for browsing namespaces and tables.

Provides endpoints for:
- Listing namespaces (top-level and nested)
- Listing tables in a namespace
- Getting table details and schema
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from iceberg_explorer.api.routes.utils import (
    _build_namespace_path,
    _parse_namespace,
    _quote_identifier,
)
from iceberg_explorer.catalog.service import get_catalog_service
from iceberg_explorer.models.catalog import (
    ColumnStatistics,
    ListNamespacesResponse,
    ListTablesResponse,
    PartitionField,
    PartitionSpec,
    SchemaField,
    Snapshot,
    TableDetails,
    TableIdentifier,
    TableSchemaResponse,
)
from iceberg_explorer.query.engine import get_engine

# Backward compatibility for tests and imports that access these helpers here.
_COMPAT_EXPORTS = (_build_namespace_path, _quote_identifier, get_engine)

router = APIRouter(prefix="/api/v1/catalog", tags=["catalog"])


@router.get("/namespaces", response_model=ListNamespacesResponse)
async def list_namespaces(
    parent: Annotated[
        str | None,
        Query(
            description="Parent namespace path using unit separator (\\x1f) for multi-level. "
            "Example: accounting%1Ftax for accounting.tax",
        ),
    ] = None,
    page_token: Annotated[  # noqa: ARG001
        str | None,
        Query(
            alias="page-token",
            description="Token for pagination (not yet implemented)",
        ),
    ] = None,
    page_size: Annotated[  # noqa: ARG001
        int,
        Query(
            alias="page-size",
            ge=1,
            le=1000,
            description="Maximum number of namespaces to return",
        ),
    ] = 100,
) -> ListNamespacesResponse:
    """List namespaces in the catalog.

    Returns top-level namespaces if no parent is specified, or child namespaces
    of the specified parent.

    Args:
        parent: Optional parent namespace path (unit separator delimited).
        page_token: Optional pagination token.
        page_size: Maximum results per page.

    Returns:
        ListNamespacesResponse with namespace identifiers.

    Raises:
        HTTPException: 404 if parent namespace doesn't exist.
    """
    from pyiceberg.exceptions import NoSuchNamespaceError

    catalog_service = get_catalog_service()

    parent_parts = _parse_namespace(parent)

    parent_ns_str = ".".join(parent_parts) if parent_parts else None

    try:
        namespaces = catalog_service.list_namespaces(parent=parent_ns_str)
    except NoSuchNamespaceError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Namespace not found: {'.'.join(parent_parts) if parent_parts else 'root'}",
        ) from e

    namespace_lists: list[list[str]] = []
    for ns in namespaces:
        namespace_lists.append(ns.split("."))

    return ListNamespacesResponse(namespaces=namespace_lists)


@router.get("/namespaces/{namespace}/tables", response_model=ListTablesResponse)
async def list_tables(
    namespace: str,
) -> ListTablesResponse:
    """List tables in a namespace.

    Args:
        namespace: Namespace path using unit separator (\\x1f) for multi-level.
                  Example: accounting%1Ftax for accounting.tax

    Returns:
        ListTablesResponse with table identifiers.

    Raises:
        HTTPException: 404 if namespace doesn't exist.
    """
    from pyiceberg.exceptions import NoSuchNamespaceError

    catalog_service = get_catalog_service()

    namespace_parts = _parse_namespace(namespace)
    if not namespace_parts:
        raise HTTPException(
            status_code=400,
            detail="Namespace cannot be empty",
        )

    namespace_str = ".".join(namespace_parts)

    try:
        tables = catalog_service.list_tables(namespace_str)
    except NoSuchNamespaceError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Namespace not found: {'.'.join(namespace_parts)}",
        ) from e

    identifiers: list[TableIdentifier] = []
    for table_identifier in tables:
        table_parts = table_identifier.split(".")
        table_name = table_parts[-1]
        identifiers.append(TableIdentifier(namespace=namespace_parts, name=table_name))

    return ListTablesResponse(identifiers=identifiers)


def _parse_table_path(table_path: str) -> tuple[list[str], str]:
    """Parse a table path in format 'namespace.table' into components.

    The namespace uses unit separator (\\x1f) for multi-level, and the table
    name is the last segment after the final dot.

    Args:
        table_path: Table path like 'accounting.transactions' or 'accounting\\x1ftax.records'

    Returns:
        Tuple of (namespace_parts, table_name)

    Raises:
        ValueError: If the path format is invalid.
    """
    if "." not in table_path:
        raise ValueError(f"Invalid table path format: {table_path}. Expected 'namespace.table'")

    last_dot = table_path.rfind(".")
    namespace_str = table_path[:last_dot]
    table_name = table_path[last_dot + 1 :]

    if not table_name or "/" in table_name or "/" in namespace_str:
        raise ValueError(f"Invalid table path: {table_path}")

    namespace_parts = _parse_namespace(namespace_str)
    if not namespace_parts:
        raise ValueError(f"Invalid namespace in table path: {table_path}")

    return namespace_parts, table_name


@router.get("/tables/{table_path:path}/schema", response_model=TableSchemaResponse)
async def get_table_schema(
    table_path: str,
) -> TableSchemaResponse:
    """Get schema for a table.

    Args:
        table_path: Table path in format 'namespace.table' where namespace uses
                   unit separator (\\x1f) for multi-level. Example: 'accounting.transactions'
                   or 'accounting%1Ftax.records'

    Returns:
        TableSchemaResponse with column definitions, types, and partition indicators.

    Raises:
        HTTPException: 404 if table doesn't exist, 400 if path format is invalid.
    """
    from pyiceberg.exceptions import NoSuchTableError

    catalog_service = get_catalog_service()

    try:
        namespace_parts, table_name = _parse_table_path(table_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    namespace_str = ".".join(namespace_parts)
    try:
        schema_info = catalog_service.get_table_schema(namespace_str, table_name)
        details = catalog_service.get_table_details(namespace_str, table_name)
    except NoSuchTableError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Table not found: {namespace_str}.{table_name}",
        ) from e

    partition_source_ids: set[int] = set()
    partition_names: set[str] = set()
    partition_spec_info = details.get("partition_spec")
    if partition_spec_info:
        for partition_field in partition_spec_info.get("fields", []):
            source_id = partition_field.get("source_id")
            if isinstance(source_id, int):
                partition_source_ids.add(source_id)
            field_name = partition_field.get("name")
            if isinstance(field_name, str):
                partition_names.add(field_name)

    column_stats: dict[str, ColumnStatistics] = {}
    fields: list[SchemaField] = []
    for field_info in schema_info["fields"]:
        field_id = int(field_info.get("field_id", 0))
        field_name = str(field_info["name"])
        stats = column_stats.get(field_name)
        fields.append(
            SchemaField(
                field_id=field_id,
                name=field_name,
                type=str(field_info["type"]),
                nullable=bool(field_info.get("nullable", True)),
                is_partition_column=(field_id in partition_source_ids)
                or (field_name in partition_names),
                statistics=stats,
            )
        )

    return TableSchemaResponse(
        namespace=namespace_parts,
        name=table_name,
        schema_id=int(schema_info.get("schema_id", 0)),
        fields=fields,
    )


@router.get("/tables/{table_path:path}", response_model=TableDetails)
async def get_table_details(
    table_path: str,
) -> TableDetails:
    """Get detailed metadata for a table.

    Args:
        table_path: Table path in format 'namespace.table' where namespace uses
                   unit separator (\\x1f) for multi-level. Example: 'accounting.transactions'
                   or 'accounting%1Ftax.records'

    Returns:
        TableDetails with location, format, partition spec, sort order, and snapshots.

    Raises:
        HTTPException: 404 if table doesn't exist, 400 if path format is invalid.
    """
    from pyiceberg.exceptions import NoSuchTableError

    catalog_service = get_catalog_service()

    try:
        namespace_parts, table_name = _parse_table_path(table_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    namespace_str = ".".join(namespace_parts)

    try:
        details = catalog_service.get_table_details(namespace_str, table_name)
    except NoSuchTableError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Table not found: {namespace_str}.{table_name}",
        ) from e

    snapshots: list[Snapshot] = []
    current_snapshot: Snapshot | None = None
    for snap_dict in details["snapshots"]:
        snapshot = Snapshot(
            sequence_number=snap_dict["sequence_number"],
            snapshot_id=snap_dict["snapshot_id"],
            timestamp_ms=snap_dict["timestamp_ms"],
            manifest_list=snap_dict["manifest_list"],
        )
        snapshots.append(snapshot)

    # Use the catalog-provided current snapshot ID if available
    current_snapshot_id = details.get("snapshot_id")
    if current_snapshot_id is not None:
        current_snapshot = next(
            (s for s in snapshots if s.snapshot_id == current_snapshot_id),
            None,
        )
    # Fallback to max sequence number if no current snapshot ID or no match found
    if current_snapshot is None and snapshots:
        current_snapshot = max(snapshots, key=lambda s: s.sequence_number)

    partition_spec: PartitionSpec | None = None
    partition_spec_info = details["partition_spec"]
    if partition_spec_info:
        spec_id = partition_spec_info.get("spec_id", 0)
        fields = []
        for field_dict in partition_spec_info.get("fields", []):
            field = PartitionField(
                source_id=field_dict["source_id"],
                field_id=field_dict.get("field_id", 1000 + len(fields)),
                name=field_dict["name"],
                transform=field_dict["transform"],
            )
            fields.append(field)
        partition_spec = PartitionSpec(spec_id=spec_id, fields=fields)

    return TableDetails(
        namespace=namespace_parts,
        name=table_name,
        location=details["location"],
        format="ICEBERG",
        partition_spec=partition_spec,
        sort_order=None,
        current_snapshot=current_snapshot,
        snapshots=snapshots,
    )
