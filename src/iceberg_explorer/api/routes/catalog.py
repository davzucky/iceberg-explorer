"""Catalog API routes for browsing namespaces and tables.

Provides endpoints for:
- Listing namespaces (top-level and nested)
- Listing tables in a namespace
- Getting table details and schema
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)

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
    engine = get_engine()

    if not engine.is_initialized:
        engine.initialize()

    try:
        namespace_parts, table_name = _parse_table_path(table_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    catalog_name = engine.catalog_name

    with engine.get_connection() as conn:
        namespace_path = _build_namespace_path(catalog_name, namespace_parts)
        quoted_table = _quote_identifier(table_name)
        full_table_path = f"{namespace_path}.{quoted_table}"

        try:
            conn.execute(f"SELECT * FROM {full_table_path} LIMIT 0")
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail=f"Table not found: {'.'.join(namespace_parts)}.{table_name}",
            ) from e

        columns_sql = f"""
            SELECT
                column_name,
                data_type,
                ordinal_position,
                is_nullable
            FROM {namespace_path}.information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
        """
        column_result = conn.execute(columns_sql, [table_name]).fetchall()

        partition_columns: set[str] = set()
        unquoted_path = f"{catalog_name}.{'.'.join(namespace_parts)}.{table_name}"
        try:
            metadata_sql = "SELECT * FROM iceberg_metadata(?) LIMIT 100"
            cur = conn.execute(metadata_sql, [unquoted_path])
            metadata_result = cur.fetchall()
            if metadata_result:
                description = cur.description
                if description:
                    col_names = [col[0].lower() for col in description]
                    if "partition_value" in col_names or "partition" in col_names:
                        part_idx = (
                            col_names.index("partition_value")
                            if "partition_value" in col_names
                            else col_names.index("partition")
                        )
                        for row in metadata_result:
                            if row[part_idx]:
                                for part in str(row[part_idx]).split(","):
                                    if "=" in part:
                                        partition_columns.add(part.split("=")[0].strip())
        except Exception as e:
            logger.debug("Failed to extract partition columns from metadata: %s", e)

        column_stats: dict[str, ColumnStatistics] = {}

        fields: list[SchemaField] = []
        for i, row in enumerate(column_result):
            col_name = row[0]
            col_type = row[1]
            ordinal = row[2] if len(row) > 2 else i + 1
            is_nullable = row[3].upper() == "YES" if len(row) > 3 and row[3] else True

            stats = column_stats.get(col_name)

            field = SchemaField(
                field_id=ordinal,
                name=col_name,
                type=col_type,
                nullable=is_nullable,
                is_partition_column=col_name in partition_columns,
                statistics=stats,
            )
            fields.append(field)

    return TableSchemaResponse(
        namespace=namespace_parts,
        name=table_name,
        schema_id=0,
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

    if snapshots:
        current_snapshot = max(snapshots, key=lambda s: s.sequence_number)

    partition_spec: PartitionSpec | None = None
    partition_spec_info = details["partition_spec"]
    if partition_spec_info:
        spec_id = 0
        fields = []
        for field_dict in partition_spec_info:
            field = PartitionField(
                source_id=field_dict["source_id"],
                field_id=1000 + len(fields),
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
