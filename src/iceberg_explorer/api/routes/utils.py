"""Shared utility functions for API routes.

Provides common helpers for:
- Namespace parsing and encoding
- SQL identifier quoting
- Namespace path construction
"""

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
