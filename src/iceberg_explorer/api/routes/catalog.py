"""Catalog API routes for browsing namespaces and tables.

Provides endpoints for:
- Listing namespaces (top-level and nested)
- Listing tables in a namespace
- Getting table details and schema
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from iceberg_explorer.models.catalog import ListNamespacesResponse
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
