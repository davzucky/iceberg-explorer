"""Catalog API routes for browsing namespaces and tables.

Provides endpoints for:
- Listing namespaces (top-level and nested)
- Listing tables in a namespace
- Getting table details and schema
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from iceberg_explorer.models.catalog import (
    ListNamespacesResponse,
    ListTablesResponse,
    PartitionSpec,
    Snapshot,
    SortOrder,
    TableDetails,
    TableIdentifier,
)
from iceberg_explorer.query.engine import get_engine

router = APIRouter(prefix="/api/v1/catalog", tags=["catalog"])

UNIT_SEPARATOR = "\x1f"


def _parse_namespace(namespace_str: str | None) -> list[str]:
    """Parse a namespace string into its components.

    Args:
        namespace_str: Namespace string with unit separator (\\x1f) delimiter,
                      or None for top-level.

    Returns:
        List of namespace components.
    """
    if not namespace_str:
        return []
    return namespace_str.split(UNIT_SEPARATOR)


def _quote_identifier(identifier: str) -> str:
    """Quote an identifier for use in SQL.

    Args:
        identifier: The identifier to quote.

    Returns:
        Quoted identifier safe for SQL.
    """
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _build_namespace_path(catalog_name: str, namespace: list[str]) -> str:
    """Build a fully qualified namespace path for SQL.

    Args:
        catalog_name: The catalog name.
        namespace: List of namespace components.

    Returns:
        Fully qualified path like catalog.ns1.ns2
    """
    parts = [_quote_identifier(catalog_name)]
    parts.extend(_quote_identifier(ns) for ns in namespace)
    return ".".join(parts)


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
    engine = get_engine()

    if not engine.is_initialized:
        engine.initialize()

    parent_parts = _parse_namespace(parent)
    catalog_name = engine.catalog_name

    with engine.get_connection() as conn:
        if parent_parts:
            parent_path = _build_namespace_path(catalog_name, parent_parts)
            try:
                conn.execute(f"SELECT * FROM {parent_path}.information_schema.schemata LIMIT 1")
            except Exception as e:
                raise HTTPException(
                    status_code=404,
                    detail=f"Namespace not found: {'.'.join(parent_parts)}",
                ) from e

        schema_path = _build_namespace_path(catalog_name, parent_parts) if parent_parts else _quote_identifier(catalog_name)
        sql = f"SELECT schema_name FROM {schema_path}.information_schema.schemata"

        try:
            result = conn.execute(sql).fetchall()
        except Exception as e:
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                raise HTTPException(
                    status_code=404,
                    detail=f"Namespace not found: {'.'.join(parent_parts) if parent_parts else 'root'}",
                ) from e
            raise

        namespaces: list[list[str]] = []
        for row in result:
            schema_name = row[0]
            if schema_name not in ("main", "information_schema", "pg_catalog"):
                full_namespace = parent_parts + [schema_name] if parent_parts else [schema_name]
                namespaces.append(full_namespace)

    return ListNamespacesResponse(namespaces=namespaces)


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
    engine = get_engine()

    if not engine.is_initialized:
        engine.initialize()

    namespace_parts = _parse_namespace(namespace)
    if not namespace_parts:
        raise HTTPException(
            status_code=400,
            detail="Namespace cannot be empty",
        )

    catalog_name = engine.catalog_name

    with engine.get_connection() as conn:
        namespace_path = _build_namespace_path(catalog_name, namespace_parts)
        try:
            conn.execute(f"SELECT * FROM {namespace_path}.information_schema.schemata LIMIT 1")
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail=f"Namespace not found: {'.'.join(namespace_parts)}",
            ) from e

        sql = f"SELECT table_name FROM {namespace_path}.information_schema.tables WHERE table_schema = '{namespace_parts[-1]}'"

        try:
            result = conn.execute(sql).fetchall()
        except Exception as e:
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                raise HTTPException(
                    status_code=404,
                    detail=f"Namespace not found: {'.'.join(namespace_parts)}",
                ) from e
            raise

        identifiers: list[TableIdentifier] = []
        for row in result:
            table_name = row[0]
            identifiers.append(
                TableIdentifier(namespace=namespace_parts, name=table_name)
            )

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

    namespace_parts = _parse_namespace(namespace_str)
    if not namespace_parts:
        raise ValueError(f"Invalid namespace in table path: {table_path}")

    return namespace_parts, table_name


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

        snapshots: list[Snapshot] = []
        current_snapshot: Snapshot | None = None
        try:
            unquoted_path = f"{catalog_name}.{'.'.join(namespace_parts)}.{table_name}"
            snapshot_result = conn.execute(
                f"SELECT sequence_number, snapshot_id, timestamp_ms, manifest_list FROM iceberg_snapshots('{unquoted_path}')"
            ).fetchall()

            for row in snapshot_result:
                timestamp_ms = row[2]
                if hasattr(timestamp_ms, "timestamp"):
                    timestamp_ms = int(timestamp_ms.timestamp() * 1000)
                elif isinstance(timestamp_ms, str):
                    from datetime import datetime

                    dt = datetime.fromisoformat(timestamp_ms.replace("Z", "+00:00"))
                    timestamp_ms = int(dt.timestamp() * 1000)
                else:
                    timestamp_ms = int(timestamp_ms)

                snapshot = Snapshot(
                    sequence_number=int(row[0]),
                    snapshot_id=int(row[1]),
                    timestamp_ms=timestamp_ms,
                    manifest_list=row[3] if row[3] else None,
                )
                snapshots.append(snapshot)

            if snapshots:
                current_snapshot = max(snapshots, key=lambda s: s.sequence_number)
        except Exception:
            pass

        partition_spec: PartitionSpec | None = None
        sort_order: SortOrder | None = None
        location: str | None = None

        try:
            metadata_result = conn.execute(
                f"SELECT * FROM iceberg_metadata('{unquoted_path}') LIMIT 1"
            ).fetchone()
            if metadata_result:
                manifest_path = metadata_result[0]
                if manifest_path:
                    path_parts = manifest_path.split("/metadata/")
                    if len(path_parts) > 1:
                        location = path_parts[0]
        except Exception:
            pass

        if location is None:
            location = f"s3://{catalog_name}/{'.'.join(namespace_parts)}/{table_name}"

    return TableDetails(
        namespace=namespace_parts,
        name=table_name,
        location=location,
        format="ICEBERG",
        partition_spec=partition_spec,
        sort_order=sort_order,
        current_snapshot=current_snapshot,
        snapshots=snapshots,
    )
