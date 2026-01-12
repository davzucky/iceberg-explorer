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
        examples=[[["accounting"], ["engineering"]]],
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


class Snapshot(BaseModel):
    """Represents an Iceberg table snapshot."""

    sequence_number: int = Field(
        ...,
        description="Sequence number of the snapshot",
    )
    snapshot_id: int = Field(
        ...,
        description="Unique identifier for the snapshot",
    )
    timestamp_ms: int = Field(
        ...,
        description="Timestamp when snapshot was created (milliseconds since epoch)",
    )
    manifest_list: str | None = Field(
        default=None,
        description="Path to the manifest list for this snapshot",
    )


class PartitionField(BaseModel):
    """Represents a field in the partition spec."""

    source_id: int = Field(
        ...,
        description="ID of the source column",
    )
    field_id: int = Field(
        ...,
        description="ID of the partition field",
    )
    name: str = Field(
        ...,
        description="Name of the partition field",
    )
    transform: str = Field(
        ...,
        description="Transform applied to the source column (e.g., identity, bucket, truncate)",
    )


class PartitionSpec(BaseModel):
    """Represents the partition specification for an Iceberg table."""

    spec_id: int = Field(
        ...,
        description="ID of the partition spec",
    )
    fields: list[PartitionField] = Field(
        default_factory=list,
        description="List of partition fields",
    )


class SortField(BaseModel):
    """Represents a field in the sort order."""

    source_id: int = Field(
        ...,
        description="ID of the source column",
    )
    transform: str = Field(
        ...,
        description="Transform applied to the source column",
    )
    direction: str = Field(
        ...,
        description="Sort direction (asc or desc)",
    )
    null_order: str = Field(
        ...,
        description="Null ordering (nulls_first or nulls_last)",
    )


class SortOrder(BaseModel):
    """Represents the sort order for an Iceberg table."""

    order_id: int = Field(
        ...,
        description="ID of the sort order",
    )
    fields: list[SortField] = Field(
        default_factory=list,
        description="List of sort fields",
    )


class TableDetails(BaseModel):
    """Detailed metadata for an Iceberg table."""

    namespace: list[str] = Field(
        ...,
        description="Namespace path components",
    )
    name: str = Field(
        ...,
        description="Table name",
    )
    location: str | None = Field(
        default=None,
        description="Storage location of the table",
    )
    format: str = Field(
        default="ICEBERG",
        description="Table format (always ICEBERG)",
    )
    partition_spec: PartitionSpec | None = Field(
        default=None,
        description="Current partition specification",
    )
    sort_order: SortOrder | None = Field(
        default=None,
        description="Current sort order",
    )
    current_snapshot: Snapshot | None = Field(
        default=None,
        description="Current (latest) snapshot",
    )
    snapshots: list[Snapshot] = Field(
        default_factory=list,
        description="List of all snapshots (history)",
    )


class ColumnStatistics(BaseModel):
    """Column-level statistics from Iceberg table metadata."""

    null_count: int | None = Field(
        default=None,
        description="Number of null values in the column",
    )
    min_value: str | None = Field(
        default=None,
        description="Minimum value (as string representation)",
    )
    max_value: str | None = Field(
        default=None,
        description="Maximum value (as string representation)",
    )


class SchemaField(BaseModel):
    """Represents a column in an Iceberg table schema."""

    field_id: int = Field(
        ...,
        description="Unique field identifier in the schema",
    )
    name: str = Field(
        ...,
        description="Column name",
    )
    type: str = Field(
        ...,
        description="Column data type",
    )
    nullable: bool = Field(
        default=True,
        description="Whether the column allows null values",
    )
    is_partition_column: bool = Field(
        default=False,
        description="Whether this column is used for partitioning",
    )
    statistics: ColumnStatistics | None = Field(
        default=None,
        description="Column statistics if available",
    )


class TableSchemaResponse(BaseModel):
    """Response for table schema endpoint."""

    namespace: list[str] = Field(
        ...,
        description="Namespace path components",
    )
    name: str = Field(
        ...,
        description="Table name",
    )
    schema_id: int = Field(
        default=0,
        description="ID of the current schema",
    )
    fields: list[SchemaField] = Field(
        default_factory=list,
        description="List of columns in the schema",
    )
