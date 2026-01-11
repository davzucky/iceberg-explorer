"""Catalog data models for namespace and table browsing.

Provides Pydantic models for:
- Namespace representation (as list of strings)
- Table identifiers
- Pagination support
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class NamespaceIdentifier(BaseModel):
    """Represents a namespace as a list of string components.

    Example: ["accounting", "tax"] for nested namespace accounting.tax
    """

    namespace: list[str] = Field(
        ...,
        description="Namespace path components",
        examples=[["accounting"], ["accounting", "tax"]],
    )


class ListNamespacesResponse(BaseModel):
    """Response for listing namespaces."""

    namespaces: list[list[str]] = Field(
        default_factory=list,
        description="List of namespace identifiers (each is a list of strings)",
        examples=[[[["accounting"], ["engineering"]]]],
    )
    next_page_token: str | None = Field(
        default=None,
        description="Token for fetching the next page of results",
    )


class TableIdentifier(BaseModel):
    """Represents a table with its namespace and name."""

    namespace: list[str] = Field(
        ...,
        description="Namespace path components",
        examples=[["accounting", "tax"]],
    )
    name: str = Field(
        ...,
        description="Table name",
        examples=["transactions"],
    )


class ListTablesResponse(BaseModel):
    """Response for listing tables in a namespace."""

    identifiers: list[TableIdentifier] = Field(
        default_factory=list,
        description="List of table identifiers",
    )
    next_page_token: str | None = Field(
        default=None,
        description="Token for fetching the next page of results",
    )
