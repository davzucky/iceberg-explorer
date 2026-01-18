# PRD: PyIceberg Catalog Integration

## Introduction

Refactor the Iceberg Explorer application to use PyIceberg for catalog operations (namespace exploration and table metadata retrieval) instead of DuckDB's Iceberg extension. This creates a cleaner separation of concerns: PyIceberg handles catalog metadata operations while DuckDB remains dedicated to SQL query execution.

Currently, the application uses DuckDB's `duckdb_schemas()`, `duckdb_tables()`, `iceberg_snapshots()`, and `iceberg_metadata()` functions for all catalog interactions. This approach conflates query execution with metadata operations and limits catalog type support to what DuckDB's Iceberg extension provides.

## Goals

- Use PyIceberg as the primary interface for all catalog metadata operations
- Support REST catalog type through PyIceberg's native connector
- Provide richer, more accurate Iceberg metadata (schema, partitions, snapshots, location)
- Maintain DuckDB exclusively for SQL query execution
- Achieve cleaner architectural separation between catalog operations and query execution
- Improve compatibility with standard Iceberg catalog implementations

## User Stories

### US-001: Add PyIceberg dependency
**Description:** As a developer, I need PyIceberg installed as a project dependency so that I can use it for catalog operations.

**Acceptance Criteria:**
- [ ] `pyiceberg` added to `pyproject.toml` with REST catalog support (`pyiceberg[pyarrow]`)
- [ ] Dependencies install successfully with `uv sync --all-extras`
- [ ] PyIceberg can be imported in Python: `from pyiceberg.catalog import load_catalog`
- [ ] Typecheck/lint passes (`uv run ruff check src/`)

---

### US-002: Create PyIceberg catalog service module
**Description:** As a developer, I need a new service module that wraps PyIceberg catalog operations so that catalog access is centralized and testable.

**Acceptance Criteria:**
- [ ] New file `src/iceberg_explorer/catalog/service.py` created
- [ ] `CatalogService` class implemented with methods:
  - `load_catalog()` - Initialize PyIceberg catalog from configuration
  - `list_namespaces(parent: tuple[str, ...] | None)` - List namespaces
  - `list_tables(namespace: tuple[str, ...])` - List tables in namespace
  - `load_table(namespace: tuple[str, ...], name: str)` - Load table object
- [ ] Service is a singleton or uses dependency injection pattern
- [ ] Proper error handling with custom exceptions
- [ ] Typecheck/lint passes

---

### US-003: Update catalog configuration for PyIceberg
**Description:** As a developer, I need the configuration to support PyIceberg catalog settings so that users can configure REST catalog connections.

**Acceptance Criteria:**
- [ ] `CatalogConfig` in `config.py` supports PyIceberg REST catalog options:
  - `uri` - REST catalog endpoint URL
  - `warehouse` - Warehouse identifier (optional)
  - `credential` - Authentication credential (optional)
  - `token` - Bearer token (optional)
- [ ] Configuration can be loaded from environment variables with prefix `ICEBERG_EXPLORER_CATALOG__`
- [ ] Configuration can be loaded from JSON config file
- [ ] Backward compatibility maintained for existing config options
- [ ] Typecheck/lint passes

---

### US-004: Implement namespace listing with PyIceberg
**Description:** As a user, I want to browse namespaces in the catalog so that I can explore the data lake structure.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/namespaces` uses PyIceberg `catalog.list_namespaces()`
- [ ] Nested namespace support via `parent` query parameter
- [ ] Response format unchanged: `{"namespaces": [["ns1"], ["ns2"]]}`
- [ ] System schemas filtered out (if any)
- [ ] 404 returned for non-existent parent namespace
- [ ] Unit tests pass
- [ ] Typecheck/lint passes

---

### US-005: Implement table listing with PyIceberg
**Description:** As a user, I want to see all tables within a namespace so that I can find the data I need.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/namespaces/{namespace}/tables` uses PyIceberg `catalog.list_tables()`
- [ ] Multi-level namespace paths decoded correctly (unit separator `%1F`)
- [ ] Response format unchanged: `{"identifiers": [{"namespace": ["ns"], "name": "table"}]}`
- [ ] 404 returned for non-existent namespace
- [ ] Unit tests pass
- [ ] Typecheck/lint passes

---

### US-006: Implement table details with PyIceberg
**Description:** As a user, I want to view table metadata (location, partitioning, snapshots) so that I can understand the table structure.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/tables/{table_path}` uses PyIceberg `catalog.load_table()`
- [ ] Table details extracted from PyIceberg `Table` object:
  - `location` from `table.location()`
  - `partition_spec` from `table.spec()`
  - `current_snapshot` from `table.current_snapshot()`
  - `snapshots` list from `table.snapshots()`
- [ ] Response format unchanged (matches existing `TableDetails` model)
- [ ] 404 returned for non-existent table
- [ ] Unit tests pass
- [ ] Typecheck/lint passes

---

### US-007: Implement table schema retrieval with PyIceberg
**Description:** As a user, I want to view the column schema of a table so that I can understand the data structure.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/tables/{table_path}/schema` uses PyIceberg `table.schema()`
- [ ] Schema fields extracted correctly:
  - `field_id` from Iceberg field ID
  - `name` from field name
  - `type` formatted as string (e.g., "string", "long", "timestamp")
  - `nullable` from field's `optional` property
  - `is_partition_column` determined from partition spec
- [ ] Response format unchanged (matches existing `TableSchemaResponse` model)
- [ ] Nested types (struct, list, map) represented appropriately
- [ ] Unit tests pass
- [ ] Typecheck/lint passes

---

### US-008: Update DuckDB engine for query-only operations
**Description:** As a developer, I need to ensure DuckDB is used only for SQL query execution, not catalog metadata operations.

**Acceptance Criteria:**
- [ ] `engine.py` catalog attachment still works (required for queries)
- [ ] Remove or deprecate `duckdb_schemas()` and `duckdb_tables()` usage from catalog routes
- [ ] Query execution endpoints (`/api/v1/query/*`) unchanged
- [ ] Export endpoint (`/api/v1/export/csv`) unchanged
- [ ] Health check updated to verify both PyIceberg catalog and DuckDB connectivity
- [ ] Unit tests pass
- [ ] Typecheck/lint passes

---

### US-009: Update UI namespace tree to use new endpoints
**Description:** As a user, I want the namespace tree in the UI to work correctly with the new PyIceberg backend.

**Acceptance Criteria:**
- [ ] `/ui/partials/namespace-tree` renders correctly
- [ ] `/ui/partials/namespace-children` HTMX partial loads child namespaces
- [ ] `/ui/partials/table-details` displays table metadata from PyIceberg
- [ ] Lazy loading of namespaces and tables works
- [ ] No UI regressions
- [ ] Typecheck/lint passes
- [ ] Verify in browser using dev-browser skill

---

### US-010: Update health endpoint for PyIceberg
**Description:** As an operator, I want the health endpoint to verify PyIceberg catalog connectivity so that I can monitor application health.

**Acceptance Criteria:**
- [ ] `GET /health` checks PyIceberg catalog connectivity
- [ ] Health response includes `"catalog": "healthy"` or `"catalog": "unhealthy"`
- [ ] DuckDB health check retained for query engine status
- [ ] Appropriate error messages when catalog is unreachable
- [ ] Unit tests pass
- [ ] Typecheck/lint passes

---

### US-011: Add integration tests for PyIceberg catalog
**Description:** As a developer, I need integration tests to verify PyIceberg catalog operations work correctly.

**Acceptance Criteria:**
- [ ] New test file `tests/test_catalog_service.py` created
- [ ] Tests cover:
  - Catalog initialization with valid/invalid config
  - Namespace listing (top-level and nested)
  - Table listing within namespaces
  - Table loading and metadata extraction
  - Schema retrieval
  - Error handling for missing namespaces/tables
- [ ] Tests use mocked PyIceberg catalog or test fixtures
- [ ] All tests pass with `uv run pytest`
- [ ] Typecheck/lint passes

---

### US-012: Update documentation and configuration examples
**Description:** As a user, I need updated documentation so that I can configure PyIceberg catalog connections.

**Acceptance Criteria:**
- [ ] `AGENTS.md` updated with new catalog configuration options
- [ ] Example configuration for REST catalog added
- [ ] Environment variable documentation updated
- [ ] Any breaking changes documented

## Functional Requirements

- FR-1: The system must use PyIceberg's `load_catalog()` to initialize catalog connections
- FR-2: The system must support REST catalog type via PyIceberg's REST catalog implementation
- FR-3: The system must use `catalog.list_namespaces()` for namespace enumeration
- FR-4: The system must use `catalog.list_tables()` for table enumeration within namespaces
- FR-5: The system must use `catalog.load_table()` to retrieve table objects
- FR-6: The system must extract table schema using `table.schema()` from PyIceberg
- FR-7: The system must extract partition spec using `table.spec()` from PyIceberg
- FR-8: The system must extract snapshot information using `table.current_snapshot()` and `table.snapshots()`
- FR-9: The system must maintain existing API response formats for backward compatibility
- FR-10: The system must continue using DuckDB for SQL query execution
- FR-11: The system must provide appropriate error responses (404, 500) for catalog failures
- FR-12: The system must support catalog configuration via environment variables and config files

## Non-Goals (Out of Scope)

- Write operations (INSERT, UPDATE, DELETE) - application is read-only
- Support for catalog types other than REST (Hive, Glue, DynamoDB) - can be added later
- Time-travel query support via PyIceberg - out of scope for this PRD
- Migration tooling - no data migration required, this is read-only integration
- Authentication/authorization - not in scope per project requirements
- Advanced metadata inspection (manifests, entries, files) - basic metadata only
- PyIceberg's scan/read capabilities - DuckDB handles all query execution

## Technical Considerations

### PyIceberg Integration
- PyIceberg Catalog instances are **not guaranteed to be thread-safe**. When sharing a Catalog instance across threads (e.g., in async web frameworks), either protect shared Catalog/Table objects with synchronization primitives (e.g., `threading.Lock`) or create separate Catalog instances per thread/request.
- Use `load_catalog()` with configuration dict for flexibility
- REST catalog requires `uri` and optionally `warehouse`, `credential`, `token`

### Catalog Configuration Example
```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "default",
    **{
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "my_warehouse",  # optional
    }
)
```

### Type Mapping
PyIceberg types need conversion to string format for API responses:
- `StringType()` -> `"string"`
- `LongType()` -> `"long"`
- `TimestampType()` -> `"timestamp"`
- `StructType(...)` -> `"struct<field1: type1, ...>"`

### Error Handling
- `NoSuchNamespaceError` -> HTTP 404
- `NoSuchTableError` -> HTTP 404
- `CatalogException` -> HTTP 500 with error details

### Existing Components to Reuse
- `models/catalog.py` - Response models remain unchanged
- `api/routes/utils.py` - Namespace parsing utilities
- `config.py` - Extend existing configuration pattern
- Templates in `templates/partials/` - No changes needed

### DuckDB Coexistence
- DuckDB still attaches to Iceberg catalog for query execution
- Both PyIceberg and DuckDB read from the same underlying catalog
- No conflict as both are read-only operations

## Success Metrics

- All existing API endpoints return identical response formats
- Catalog operations complete within acceptable latency (< 500ms for listings)
- All existing tests pass after migration
- New PyIceberg-specific tests achieve >80% coverage of catalog service
- No regressions in UI functionality
- Health checks accurately reflect catalog and query engine status

## Open Questions

- Should we expose additional PyIceberg metadata (e.g., table properties) in future iterations?
- Should we add caching for frequently accessed catalog metadata?
- How should we handle catalog connection failures during application startup?
