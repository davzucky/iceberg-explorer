"""Web UI routes for Iceberg Explorer."""

import hashlib
import logging
from pathlib import Path
from typing import Annotated
from urllib.parse import quote

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from iceberg_explorer.query.engine import get_engine

TEMPLATES_DIR = Path(__file__).parent.parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(tags=["ui"])

logger = logging.getLogger(__name__)

UNIT_SEPARATOR = "\x1f"


def _parse_namespace(namespace_str: str | None) -> list[str]:
    """Parse a namespace string into its components."""
    if not namespace_str:
        return []
    return namespace_str.split(UNIT_SEPARATOR)


def _quote_identifier(identifier: str) -> str:
    """Quote an identifier for use in SQL."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _build_namespace_path(catalog_name: str, namespace: list[str]) -> str:
    """Build a fully qualified namespace path for SQL."""
    parts = [_quote_identifier(catalog_name)]
    parts.extend(_quote_identifier(ns) for ns in namespace)
    return ".".join(parts)


def _encode_namespace(namespace_parts: list[str]) -> str:
    """Encode namespace parts for URL parameter."""
    return quote(UNIT_SEPARATOR.join(namespace_parts), safe="")


def _generate_id(namespace_parts: list[str]) -> str:
    """Generate a stable ID for a namespace."""
    path = UNIT_SEPARATOR.join(namespace_parts)
    return hashlib.md5(path.encode(), usedforsecurity=False).hexdigest()[:8]


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Render the main catalog browser page."""
    return templates.TemplateResponse(request, "index.html")


@router.get("/query", response_class=HTMLResponse)
async def query_page(request: Request) -> HTMLResponse:
    """Render the query editor page."""
    return templates.TemplateResponse(request, "query.html")


@router.get("/ui/partials/namespace-tree", response_class=HTMLResponse)
async def namespace_tree_partial(request: Request) -> HTMLResponse:
    """Render the namespace tree with top-level namespaces from the catalog."""
    namespaces: list[dict] = []

    try:
        engine = get_engine()
        if not engine.is_initialized:
            engine.initialize()

        catalog_name = engine.catalog_name

        with engine.get_connection() as conn:
            sql = f"SELECT schema_name FROM {_quote_identifier(catalog_name)}.information_schema.schemata"
            result = conn.execute(sql).fetchall()

            for row in result:
                schema_name = row[0]
                if schema_name not in ("main", "information_schema", "pg_catalog"):
                    namespace_parts = [schema_name]
                    namespaces.append(
                        {
                            "name": schema_name,
                            "path": UNIT_SEPARATOR.join(namespace_parts),
                            "encoded_path": _encode_namespace(namespace_parts),
                            "id": _generate_id(namespace_parts),
                        }
                    )
    except Exception as e:
        logger.warning("Failed to load namespaces: %s", e)

    return templates.TemplateResponse(
        request,
        "partials/namespace_tree.html",
        {"namespaces": namespaces, "level": 0},
    )


@router.get("/ui/partials/namespace-children", response_class=HTMLResponse)
async def namespace_children_partial(
    request: Request,
    parent: Annotated[
        str,
        Query(description="Parent namespace path using unit separator"),
    ],
) -> HTMLResponse:
    """Render children (child namespaces and tables) for a namespace."""
    namespaces: list[dict] = []
    tables: list[dict] = []

    parent_parts = _parse_namespace(parent)

    try:
        engine = get_engine()
        if not engine.is_initialized:
            engine.initialize()

        catalog_name = engine.catalog_name

        with engine.get_connection() as conn:
            namespace_path = _build_namespace_path(catalog_name, parent_parts)

            try:
                sql = f"SELECT schema_name FROM {namespace_path}.information_schema.schemata"
                result = conn.execute(sql).fetchall()

                for row in result:
                    schema_name = row[0]
                    if schema_name not in ("main", "information_schema", "pg_catalog"):
                        namespace_parts = parent_parts + [schema_name]
                        namespaces.append(
                            {
                                "name": schema_name,
                                "path": UNIT_SEPARATOR.join(namespace_parts),
                                "encoded_path": _encode_namespace(namespace_parts),
                                "id": _generate_id(namespace_parts),
                            }
                        )
            except Exception as e:
                logger.debug("No child namespaces found: %s", e)

            try:
                sql = f"SELECT table_name FROM {namespace_path}.information_schema.tables WHERE table_schema = ?"
                result = conn.execute(sql, [parent_parts[-1]]).fetchall()

                for row in result:
                    table_name = row[0]
                    table_path = f"{_encode_namespace(parent_parts)}.{table_name}"
                    tables.append(
                        {
                            "name": table_name,
                            "namespace": parent_parts,
                            "table_path": table_path,
                            "id": _generate_id(parent_parts + [table_name]),
                        }
                    )
            except Exception as e:
                logger.debug("Failed to load tables: %s", e)

    except Exception as e:
        logger.warning("Failed to load namespace children: %s", e)

    return templates.TemplateResponse(
        request,
        "partials/namespace_children.html",
        {"namespaces": namespaces, "tables": tables, "level": len(parent_parts)},
    )


@router.get("/ui/partials/table-details", response_class=HTMLResponse)
async def table_details_partial(
    request: Request,
    table_path: Annotated[
        str,
        Query(description="Table path in format namespace.table"),
    ],
) -> HTMLResponse:
    """Render table details for the main content area."""
    table_info: dict = {}
    error: str | None = None

    try:
        if "." not in table_path:
            error = "Invalid table path format"
        else:
            last_dot = table_path.rfind(".")
            namespace_str = table_path[:last_dot]
            table_name = table_path[last_dot + 1 :]
            namespace_parts = _parse_namespace(namespace_str)

            engine = get_engine()
            if not engine.is_initialized:
                engine.initialize()

            catalog_name = engine.catalog_name

            with engine.get_connection() as conn:
                namespace_path = _build_namespace_path(catalog_name, namespace_parts)
                quoted_table = _quote_identifier(table_name)
                full_table_path = f"{namespace_path}.{quoted_table}"

                conn.execute(f"SELECT * FROM {full_table_path} LIMIT 0")

                columns_sql = f"""
                    SELECT column_name, data_type, is_nullable
                    FROM {namespace_path}.information_schema.columns
                    WHERE table_name = ?
                    ORDER BY ordinal_position
                """
                columns = conn.execute(columns_sql, [table_name]).fetchall()

                partition_columns: set[str] = set()
                try:
                    unquoted_path = f"{catalog_name}.{'.'.join(namespace_parts)}.{table_name}"
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
                    logger.debug("Failed to extract partition columns: %s", e)

                snapshots = []
                location = None
                try:
                    unquoted_path = f"{catalog_name}.{'.'.join(namespace_parts)}.{table_name}"
                    snapshot_result = conn.execute(
                        "SELECT sequence_number, snapshot_id, timestamp_ms FROM iceberg_snapshots(?)",
                        [unquoted_path],
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

                        snapshots.append(
                            {
                                "sequence_number": int(row[0]),
                                "snapshot_id": int(row[1]),
                                "timestamp_ms": timestamp_ms,
                            }
                        )

                    metadata_result = conn.execute(
                        "SELECT * FROM iceberg_metadata(?) LIMIT 1",
                        [unquoted_path],
                    ).fetchone()
                    if metadata_result and metadata_result[0]:
                        path_parts = metadata_result[0].split("/metadata/")
                        if len(path_parts) > 1:
                            location = path_parts[0]
                except Exception as e:
                    logger.debug("Failed to fetch metadata: %s", e)

                partition_column_list = sorted(partition_columns) if partition_columns else []
                current_snapshot = (
                    max(snapshots, key=lambda s: s["sequence_number"]) if snapshots else None
                )
                table_info = {
                    "namespace": namespace_parts,
                    "name": table_name,
                    "location": location
                    or f"s3://{catalog_name}/{'.'.join(namespace_parts)}/{table_name}",
                    "format": "ICEBERG",
                    "partition_columns": partition_column_list,
                    "columns": [
                        {
                            "name": col[0],
                            "type": col[1],
                            "nullable": col[2].upper() == "YES" if col[2] else True,
                            "is_partition": col[0] in partition_columns,
                        }
                        for col in columns
                    ],
                    "snapshots": snapshots,
                    "current_snapshot": current_snapshot,
                }
    except Exception as e:
        logger.warning("Failed to load table details: %s", e)
        error = str(e)

    return templates.TemplateResponse(
        request,
        "partials/table_details.html",
        {"table": table_info, "error": error},
    )
