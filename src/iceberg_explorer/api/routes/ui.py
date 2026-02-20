"""Web UI routes for Iceberg Explorer."""

import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Annotated
from urllib.parse import quote, unquote

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from iceberg_explorer.api.routes.utils import (
    UNIT_SEPARATOR,
    _parse_namespace,
)
from iceberg_explorer.catalog.service import get_catalog_service

TEMPLATES_DIR = Path(__file__).parent.parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(tags=["ui"])

logger = logging.getLogger(__name__)
CATALOG_CALL_TIMEOUT_SECONDS = 5.0


async def _run_catalog_call(callable_obj, *args):
    """Run blocking catalog calls in a thread with a timeout."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(callable_obj, *args),
            timeout=CATALOG_CALL_TIMEOUT_SECONDS,
        )
    except TimeoutError as e:
        raise RuntimeError("Catalog request timed out") from e


def _encode_namespace(namespace_parts: list[str]) -> str:
    """Encode namespace parts for URL parameter."""
    return quote(UNIT_SEPARATOR.join(namespace_parts), safe="")


def _generate_id(namespace_parts: list[str]) -> str:
    """Generate a stable ID for a namespace."""
    path = UNIT_SEPARATOR.join(namespace_parts)
    return hashlib.md5(path.encode(), usedforsecurity=False).hexdigest()[:8]


def _normalize_identifier(identifier: str) -> str:
    """Normalize a table/namespace identifier from URL input."""
    normalized = unquote(identifier).strip()
    quote_pairs = {'"': '"', "'": "'", "`": "`", "[": "]"}
    if (
        len(normalized) >= 2
        and normalized[0] in quote_pairs
        and normalized[-1] == quote_pairs[normalized[0]]
    ):
        opening_quote = normalized[0]
        normalized = normalized[1:-1].strip()
        escaped_quote = "]]" if opening_quote == "[" else opening_quote * 2
        normalized = normalized.replace(
            escaped_quote, "]" if opening_quote == "[" else opening_quote
        )
    return normalized


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Render the main catalog browser page."""
    return templates.TemplateResponse(request, "index.html")


@router.get("/ui/partials/namespace-tree", response_class=HTMLResponse)
async def namespace_tree_partial(request: Request) -> HTMLResponse:
    """Render the namespace tree with top-level namespaces from the catalog."""
    namespaces: list[dict] = []

    try:
        catalog_service = get_catalog_service()
        discovered = await _run_catalog_call(catalog_service.list_namespaces)
        top_level_names: set[str] = set()
        for namespace in discovered:
            parts = namespace.split(".")
            if parts and parts[0] and parts[0] not in top_level_names:
                top_level_names.add(parts[0])
                namespace_parts = [parts[0]]
                namespaces.append(
                    {
                        "name": parts[0],
                        "path": UNIT_SEPARATOR.join(namespace_parts),
                        "encoded_path": _encode_namespace(namespace_parts),
                        "id": _generate_id(namespace_parts),
                    }
                )
        namespaces.sort(key=lambda ns: ns["name"])
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

    # Handle empty parent namespace - return empty result
    if not parent_parts:
        return templates.TemplateResponse(
            request,
            "partials/namespace_children.html",
            {"namespaces": namespaces, "tables": tables, "level": 0},
        )

    try:
        catalog_service = get_catalog_service()
        parent_namespace = ".".join(parent_parts)

        child_namespaces = await _run_catalog_call(
            catalog_service.list_namespaces, parent_namespace
        )
        seen_children: set[str] = set()
        for child in child_namespaces:
            child_parts = child.split(".")
            if child_parts[: len(parent_parts)] == parent_parts:
                full_parts = child_parts
            elif len(child_parts) == 1:
                full_parts = [*parent_parts, child_parts[0]]
            else:
                continue

            if len(full_parts) <= len(parent_parts):
                continue

            immediate_parts = full_parts[: len(parent_parts) + 1]
            child_name = immediate_parts[-1]
            if child_name in seen_children:
                continue
            seen_children.add(child_name)
            namespaces.append(
                {
                    "name": child_name,
                    "path": UNIT_SEPARATOR.join(immediate_parts),
                    "encoded_path": _encode_namespace(immediate_parts),
                    "id": _generate_id(immediate_parts),
                }
            )

        table_identifiers = await _run_catalog_call(catalog_service.list_tables, parent_namespace)
        for table_identifier in table_identifiers:
            table_name = table_identifier.split(".")[-1]
            table_path = f"{_encode_namespace(parent_parts)}.{quote(table_name, safe='')}"
            tables.append(
                {
                    "name": table_name,
                    "namespace": parent_parts,
                    "table_path": table_path,
                    "id": _generate_id([*parent_parts, table_name]),
                }
            )

        namespaces.sort(key=lambda ns: ns["name"])
        tables.sort(key=lambda table: table["name"])

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
    from pyiceberg.exceptions import NoSuchTableError

    table_info: dict = {}
    error: str | None = None
    prefill_sql: str = ""

    try:
        if "." not in table_path:
            error = "Invalid table path format"
        else:
            last_dot = table_path.rfind(".")
            namespace_str = unquote(table_path[:last_dot])
            table_name = _normalize_identifier(table_path[last_dot + 1 :])
            namespace_parts = [
                _normalize_identifier(part) for part in _parse_namespace(namespace_str)
            ]

            # Validate namespace and table name are not empty
            if not namespace_parts:
                error = "Invalid namespace in table path"
            elif not table_name:
                error = "Invalid table name in table path"
            else:
                catalog_service = get_catalog_service()
                namespace = ".".join(namespace_parts)
                details = await _run_catalog_call(
                    catalog_service.get_table_details,
                    namespace,
                    table_name,
                )
                schema_info = await _run_catalog_call(
                    catalog_service.get_table_schema,
                    namespace,
                    table_name,
                )

                partition_spec = details.get("partition_spec") or {}
                partition_field_entries = partition_spec.get("fields", [])
                partition_columns = sorted(
                    field["name"]
                    for field in partition_field_entries
                    if isinstance(field.get("name"), str)
                )
                partition_source_ids = {
                    field["source_id"]
                    for field in partition_field_entries
                    if isinstance(field.get("source_id"), int)
                }

                snapshots = [
                    {
                        "sequence_number": int(snap["sequence_number"]),
                        "snapshot_id": int(snap["snapshot_id"]),
                        "timestamp_ms": int(snap["timestamp_ms"]),
                    }
                    for snap in details.get("snapshots", [])
                ]

                current_snapshot_id = details.get("snapshot_id")
                current_snapshot = None
                if current_snapshot_id is not None:
                    current_snapshot = next(
                        (snap for snap in snapshots if snap["snapshot_id"] == current_snapshot_id),
                        None,
                    )
                if current_snapshot is None and snapshots:
                    current_snapshot = max(snapshots, key=lambda s: s["sequence_number"])

                table_info = {
                    "namespace": namespace_parts,
                    "name": table_name,
                    "location": details.get("location"),
                    "format": "ICEBERG",
                    "partition_columns": partition_columns,
                    "columns": [
                        {
                            "name": field["name"],
                            "type": field["type"],
                            "nullable": field["nullable"],
                            "is_partition": field.get("field_id") in partition_source_ids,
                        }
                        for field in schema_info.get("fields", [])
                    ],
                    "snapshots": snapshots,
                    "current_snapshot": current_snapshot,
                }
                qualified_identifier = ".".join([*namespace_parts, table_name])
                prefill_sql = f"SELECT * FROM {qualified_identifier} LIMIT 100"
    except NoSuchTableError:
        error = "Table not found"
    except Exception:
        logger.exception("Failed to load table details")
        error = "An unexpected error occurred while loading table details."

    return templates.TemplateResponse(
        request,
        "partials/table_details.html",
        {"table": table_info, "error": error, "prefill_sql": prefill_sql},
    )
