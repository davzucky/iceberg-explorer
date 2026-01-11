# PRD: Iceberg Explorer - Data Lake Exploration Platform

## Introduction

Iceberg Explorer is a high-performance web application for interactive exploration of Apache Iceberg data lakes. Built with FastAPI, HTMX, Alpine.js, and DuckDB, it provides a unified interface for data analysts to discover, inspect, and query Iceberg tables across REST catalogs with minimal infrastructure overhead.

This PRD covers the **MVP phase**, focusing on core catalog exploration, schema inspection, and data querying capabilities. Authentication is deferred to a later phase.

## Goals

- Provide intuitive catalog browsing with namespace tree navigation
- Enable interactive schema inspection with column statistics
- Support read-only SQL queries against Iceberg tables via DuckDB
- Deliver streaming query results with pagination and sorting
- Export query results to CSV format
- Run as standalone Python application (containers later)
- Achieve 90%+ test coverage with balanced unit/integration/E2E tests

## Architecture Overview

```
┌─────────────────┐      ┌──────────────────────────┐
│   Web Browser   │─────▶│   FastAPI + HTMX         │
│   (HTMX +       │      │   Backend                │
│    Alpine.js)   │      │   (Granian ASGI)         │
└─────────────────┘      │                          │
                         │   DuckDB + Iceberg       │─────▶ Iceberg REST Catalog
                         │   Extension              │       (or local filesystem)
                         └──────────────────────────┘
                                   │
                                   ▼
                         ┌──────────────────┐
                         │   Object Storage │
                         │   (S3, GCS, etc) │
                         └──────────────────┘
```

## User Stories

### Phase 1: Project Foundation

#### US-001: Project Setup and Configuration
**Description:** As a developer, I need a properly structured Python project so that I can begin development with modern tooling.

**Acceptance Criteria:**
- [ ] Project uses `uv` for dependency management
- [ ] `pyproject.toml` configured with FastAPI, DuckDB, Granian dependencies
- [ ] Source code in `src/iceberg_explorer/` structure
- [ ] Pre-commit hooks configured (ruff, pyupgrade)
- [ ] Configuration loading from JSON file with env var overrides
- [ ] `uv run pytest` runs successfully (empty test suite)
- [ ] `uv run ruff check src/` passes

#### US-002: Configuration System
**Description:** As a developer, I need a flexible configuration system so that the app works in different environments.

**Acceptance Criteria:**
- [ ] Load config from JSON file via `ICEBERG_EXPLORER_CONFIG` env var
- [ ] Support env var overrides (e.g., `ICEBERG_EXPLORER_QUERY__MAX_ROWS`)
- [ ] Pydantic models for config validation
- [ ] Defaults for all optional settings
- [ ] Support both REST and local catalog types
- [ ] Configuration for DuckDB memory/threads limits
- [ ] Tests cover config loading and validation

---

### Phase 2: DuckDB Integration

#### US-003: DuckDB Engine Initialization
**Description:** As a developer, I need a DuckDB engine that connects to Iceberg catalogs so queries can access table data.

**Acceptance Criteria:**
- [ ] DuckDB connection pool/manager class
- [ ] Attach Iceberg catalog (REST or local) on initialization
- [ ] Read-only mode enforced at DuckDB level
- [ ] Memory limit and thread count configurable
- [ ] Health check method to verify catalog connectivity
- [ ] Unit tests with local Iceberg catalog fixture

#### US-004: Query Execution Engine
**Description:** As a developer, I need a query executor that runs SQL and streams results so the API can return data efficiently.

**Acceptance Criteria:**
- [ ] Execute SQL query with configurable timeout (10s - 3600s)
- [ ] Return results as Arrow batches for streaming
- [ ] Reject non-SELECT statements (INSERT, UPDATE, DELETE, DROP)
- [ ] Track execution metrics (duration, rows scanned)
- [ ] Query cancellation via interrupt
- [ ] Integration tests with sample Iceberg tables

---

### Phase 3: Catalog API

#### US-005: List Namespaces Endpoint
**Description:** As a user, I want to list namespaces in the catalog so I can browse available data.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/namespaces` returns top-level namespace list
- [ ] `GET /api/v1/catalog/namespaces?parent={ns}` returns child namespaces
- [ ] Parent parameter uses unit separator (`%1F`) for multi-level: `parent=accounting%1Ftax`
- [ ] Namespaces returned as arrays: `[["accounting"], ["engineering"]]`
- [ ] Support pagination with `page-token` and `page-size` params
- [ ] Return namespace metadata/properties if available
- [ ] Empty list for catalogs with no namespaces
- [ ] 404 if parent namespace doesn't exist
- [ ] Integration test with nested namespace hierarchy

**Technical Note:** Iceberg REST API represents namespaces as arrays of strings. For example, `accounting.tax.paid` is represented as `["accounting", "tax", "paid"]`. The `parent` query parameter filters to show only direct children of that namespace.

#### US-006: List Tables Endpoint
**Description:** As a user, I want to list tables in a namespace so I can find the data I need.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/namespaces/{namespace}/tables` returns table list
- [ ] Namespace path uses unit separator for multi-level: `accounting%1Ftax`
- [ ] Return table identifiers as `{namespace: [...], name: "table_name"}`
- [ ] Return table names with basic metadata
- [ ] 404 for non-existent namespace
- [ ] Integration test with sample tables

#### US-007: Table Details Endpoint
**Description:** As a user, I want to view table metadata so I understand the table structure.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/tables/{namespace}.{table}` returns details
- [ ] Include: location, format, partition spec, sort order, current snapshot
- [ ] Return snapshot history (list of snapshots with timestamps)
- [ ] 404 for non-existent table
- [ ] Integration test verifying all metadata fields

#### US-008: Table Schema Endpoint
**Description:** As a user, I want to view a table's schema so I know what columns are available.

**Acceptance Criteria:**
- [ ] `GET /api/v1/catalog/tables/{namespace}.{table}/schema` returns schema
- [ ] Include column name, type, field ID, nullable flag
- [ ] Mark partition columns with indicator
- [ ] Include column statistics if available (min/max/null count)
- [ ] Integration test with various column types

---

### Phase 4: Query API

#### US-009: Execute Query Endpoint
**Description:** As a user, I want to execute SQL queries so I can explore table data.

**Acceptance Criteria:**
- [ ] `POST /api/v1/query/execute` accepts SQL and optional timeout
- [ ] Return query_id for tracking
- [ ] Validate SQL is SELECT-only before execution
- [ ] Default timeout 300s, user can specify 10s-3600s
- [ ] Return error with message for invalid queries
- [ ] Unit tests for SQL validation, integration tests for execution

#### US-010: Query Results Endpoint (Streaming)
**Description:** As a user, I want to retrieve query results with pagination so I can view large datasets.

**Acceptance Criteria:**
- [ ] `GET /api/v1/query/{query_id}/results` streams JSON lines
- [ ] Stream format: metadata → data batches → progress → complete
- [ ] Support `page_size` param (100, 250, 500, 1000)
- [ ] Support `offset` param for pagination
- [ ] Include execution metrics in complete message
- [ ] Integration test verifying streaming behavior

#### US-011: Query Status Endpoint
**Description:** As a user, I want to check query status so I know when results are ready.

**Acceptance Criteria:**
- [ ] `GET /api/v1/query/{query_id}/status` returns current state
- [ ] States: pending, running, completed, failed, cancelled
- [ ] Include progress info for running queries (rows processed)
- [ ] Include error message for failed queries
- [ ] Unit tests for state transitions

#### US-012: Cancel Query Endpoint
**Description:** As a user, I want to cancel a running query so I don't wait for unwanted results.

**Acceptance Criteria:**
- [ ] `POST /api/v1/query/{query_id}/cancel` stops execution
- [ ] DuckDB query interrupted cleanly
- [ ] Status updates to "cancelled"
- [ ] Return success even if query already finished
- [ ] Integration test verifying cancellation works

---

### Phase 5: Export API *(Lower Priority)*

#### US-013: CSV Export Endpoint *(Lower Priority)*
**Description:** As a user, I want to export query results to CSV so I can use data in other tools.

**Priority Note:** This feature is lower priority and can be implemented after core functionality is complete.

**Acceptance Criteria:**
- [ ] `POST /api/v1/export/csv` accepts query_id or inline SQL
- [ ] Stream CSV as chunked download
- [ ] Configurable max size (default 1GB)
- [ ] Proper Content-Disposition header with filename
- [ ] Handle large exports without memory issues
- [ ] Integration test with various data types

---

### Phase 6: Web UI (HTMX + Alpine.js)

#### US-014: Base Layout and Navigation
**Description:** As a user, I want a clean UI layout so I can navigate the application easily.

**Acceptance Criteria:**
- [ ] Base HTML template with header, sidebar, main content
- [ ] Tailwind CSS for styling (compiled, not CDN in prod)
- [ ] HTMX loaded for dynamic updates
- [ ] Alpine.js loaded for client-side interactivity
- [ ] Responsive design (works on tablet+)
- [ ] Verify in browser using dev-browser skill

#### US-015: Namespace Tree Browser
**Description:** As a user, I want to browse namespaces in a tree view so I can find tables quickly.

**Acceptance Criteria:**
- [ ] Sidebar shows namespace tree
- [ ] Click namespace to expand/collapse (lazy load via HTMX)
- [ ] Tables shown as leaf nodes under namespaces
- [ ] Click table to load details in main panel
- [ ] Loading indicator during fetch
- [ ] Verify in browser using dev-browser skill

#### US-016: Table Details View
**Description:** As a user, I want to view table details so I understand the data structure.

**Acceptance Criteria:**
- [ ] Main panel shows selected table metadata
- [ ] Tabs for: Overview, Schema, Snapshots
- [ ] Overview: location, format, partitioning, current snapshot
- [ ] Schema: searchable column list with type badges
- [ ] Snapshots: list with timestamps, click to time-travel (future)
- [ ] HTMX partial updates when switching tabs
- [ ] Verify in browser using dev-browser skill

#### US-017: SQL Query Editor
**Description:** As a user, I want a SQL editor so I can write and execute queries.

**Acceptance Criteria:**
- [ ] Monaco editor with SQL syntax highlighting
- [ ] Auto-complete for table/column names (bonus, can defer)
- [ ] Run button executes query via HTMX
- [ ] Timeout selector (dropdown: 30s, 60s, 5m, 15m, 1h)
- [ ] Cancel button visible during execution
- [ ] Error messages displayed clearly
- [ ] Verify in browser using dev-browser skill

#### US-018: Query Results Table
**Description:** As a user, I want to view query results in a table so I can analyze the data.

**Acceptance Criteria:**
- [ ] Results displayed in scrollable table
- [ ] Column headers clickable for sorting (Alpine.js)
- [ ] Per-column filter inputs (text, numeric range)
- [ ] Pagination controls (page size selector, prev/next)
- [ ] Row count and execution time displayed
- [ ] Streaming indicator during data fetch
- [ ] Verify in browser using dev-browser skill

#### US-019: CSV Export Button *(Lower Priority)*
**Description:** As a user, I want to export results to CSV so I can use data externally.

**Priority Note:** Depends on US-013. Lower priority, implement after core UI is complete.

**Acceptance Criteria:**
- [ ] Export button in results toolbar
- [ ] Shows progress for large exports
- [ ] Downloads file with appropriate name
- [ ] Disabled when no results available
- [ ] Verify in browser using dev-browser skill

---

### Phase 7: Health & Observability

#### US-020: Health and Readiness Endpoints
**Description:** As an operator, I need health check endpoints so I can monitor application status.

**Acceptance Criteria:**
- [ ] `GET /health` returns status, version, component health
- [ ] `GET /ready` returns readiness for traffic
- [ ] Check DuckDB connectivity in health
- [ ] Check catalog connectivity in health
- [ ] Return 503 if unhealthy
- [ ] Unit tests for health logic

#### US-021: OpenTelemetry Instrumentation
**Description:** As an operator, I need observability via OpenTelemetry so I can monitor the application with standard tooling.

**Acceptance Criteria:**
- [ ] OpenTelemetry SDK integrated with auto-instrumentation
- [ ] FastAPI routes automatically traced
- [ ] DuckDB query spans with duration and row count attributes
- [ ] Metrics exported: `query_duration_seconds` (histogram), `query_rows_returned` (counter), `active_queries` (gauge)
- [ ] Configurable OTLP exporter endpoint via `OTEL_EXPORTER_OTLP_ENDPOINT`
- [ ] Service name set to `iceberg-explorer`
- [ ] Structured JSON logs with trace context (trace_id, span_id)
- [ ] Log query execution with: query_id, table, rows, duration
- [ ] Log errors with stack traces
- [ ] Configurable log level via config
- [ ] No sensitive data in logs (no SQL content by default)
- [ ] Integration test verifying traces and metrics are emitted

---

## Functional Requirements

### Catalog Operations
- FR-1: Support REST catalog type with configurable endpoint URL
- FR-2: Support local filesystem catalog for testing
- FR-3: Single catalog configured at deployment time (no multi-catalog)
- FR-4: Lazy-load namespace children for performance

### Query Operations
- FR-5: Execute read-only SQL queries only (reject DML/DDL)
- FR-6: User-settable query timeout (10s - 3600s, default 300s)
- FR-7: Query cancellation via UI and API
- FR-8: Stream results as JSON lines for large datasets
- FR-9: Pagination with configurable page size (100-1000 rows)

### Export Operations
- FR-10: Stream CSV export (chunked transfer)
- FR-11: Configurable max export size (default 1GB)
- FR-12: Filename includes table name and timestamp

### UI Operations
- FR-13: HTMX for server-driven UI updates
- FR-14: Alpine.js for client-side sorting/filtering
- FR-15: Monaco editor for SQL input
- FR-16: Responsive layout (minimum 768px width)

### Observability
- FR-17: OpenTelemetry auto-instrumentation for traces and metrics
- FR-18: Health and readiness probes
- FR-19: Structured JSON logging with trace context

## Non-Goals (Out of Scope for MVP)

- **Authentication/Authorization**: Keycloak integration deferred to Phase 2
- **Query History**: No persistent storage of past queries
- **Query Bookmarks**: No saved queries feature
- **Parquet Export**: CSV only for MVP
- **Multi-Catalog**: Single catalog per deployment
- **Write Operations**: Read-only access only
- **Time-Travel Queries**: View snapshots but not query them
- **Data Profiling**: No automatic statistics generation
- **Query Scheduling**: No async/scheduled queries
- **Docker/Kubernetes**: Standalone Python first, containers later
- **Query Plan Visualization**: EXPLAIN output deferred to future version

## Technical Considerations

### Dependencies
- **FastAPI**: REST API framework
- **HTMX**: Server-driven UI updates
- **Alpine.js**: Lightweight client-side reactivity
- **DuckDB**: In-process SQL engine with Iceberg extension
- **Granian**: Rust-based ASGI server
- **Pydantic**: Configuration and model validation
- **Jinja2**: Server-side template rendering
- **opentelemetry-instrumentation-fastapi**: Auto-instrumentation for FastAPI
- **opentelemetry-exporter-otlp**: OTLP exporter for traces and metrics
- **structlog**: Structured logging with OpenTelemetry trace context

### Project Structure
```
iceberg-explorer/
├── src/
│   └── iceberg_explorer/
│       ├── __init__.py
│       ├── main.py              # FastAPI app factory
│       ├── config.py            # Configuration management
│       ├── catalog/
│       │   ├── __init__.py
│       │   ├── manager.py       # Catalog abstraction
│       │   ├── rest.py          # REST catalog
│       │   └── local.py         # Local filesystem
│       ├── query/
│       │   ├── __init__.py
│       │   ├── engine.py        # DuckDB engine
│       │   ├── executor.py      # Async execution
│       │   └── models.py        # Query state
│       ├── api/
│       │   ├── __init__.py
│       │   └── routes/
│       │       ├── catalog.py
│       │       ├── query.py
│       │       ├── export.py
│       │       └── health.py
│       ├── models/
│       │   ├── catalog.py       # Pydantic models
│       │   └── query.py
│       └── templates/
│           ├── base.html
│           ├── catalog.html
│           ├── query.html
│           └── partials/
│               ├── namespace_tree.html
│               ├── table_details.html
│               └── results_table.html
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── e2e/
│   ├── fixtures/
│   └── conftest.py
├── static/
│   ├── css/
│   └── js/
├── pyproject.toml
├── uv.lock
└── .pre-commit-config.yaml
```

### Testing Strategy
- **Unit Tests (70%)**: Config, models, SQL validation, query state
- **Integration Tests (20%)**: DuckDB queries, catalog operations, API endpoints
- **E2E Tests (10%)**: Full user workflows with browser automation
- **Fixtures**: Local Iceberg catalog with sample data

### Testing Infrastructure (docker-compose)

For integration and E2E testing, a `docker/docker-compose.test.yml` file provides:
- **Lakekeeper**: Iceberg REST catalog (port 8181)
- **MinIO**: S3-compatible object storage (port 9000, console 9001)
- **PostgreSQL**: Backend for Lakekeeper catalog metadata

```yaml
# docker/docker-compose.test.yml
services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 5s
      retries: 3
    networks:
      - iceberg-net

  createbuckets:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set myminio http://minio:9000 minioadmin minioadmin;
      mc mb myminio/warehouse --ignore-existing;
      exit 0;
      "
    networks:
      - iceberg-net

  postgres:
    image: postgres:17
    environment:
      POSTGRES_USER: lakekeeper
      POSTGRES_PASSWORD: lakekeeper
      POSTGRES_DB: lakekeeper
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U lakekeeper -d lakekeeper"]
      interval: 2s
      timeout: 5s
      retries: 5
    volumes:
      - postgres-data:/var/lib/postgresql/data
    networks:
      - iceberg-net

  lakekeeper-migrate:
    image: quay.io/lakekeeper/catalog:latest
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      LAKEKEEPER__PG_ENCRYPTION_KEY: "test-encryption-key-not-for-prod"
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://lakekeeper:lakekeeper@postgres:5432/lakekeeper
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://lakekeeper:lakekeeper@postgres:5432/lakekeeper
    command: ["migrate"]
    networks:
      - iceberg-net

  lakekeeper:
    image: quay.io/lakekeeper/catalog:latest
    depends_on:
      lakekeeper-migrate:
        condition: service_completed_successfully
      minio:
        condition: service_healthy
    ports:
      - "8181:8181"
    environment:
      LAKEKEEPER__PG_ENCRYPTION_KEY: "test-encryption-key-not-for-prod"
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://lakekeeper:lakekeeper@postgres:5432/lakekeeper
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://lakekeeper:lakekeeper@postgres:5432/lakekeeper
      LAKEKEEPER__AUTHZ_BACKEND: allowall
    command: ["serve"]
    healthcheck:
      test: ["CMD", "/home/nonroot/lakekeeper", "healthcheck"]
      interval: 2s
      timeout: 10s
      retries: 5
    networks:
      - iceberg-net

networks:
  iceberg-net:
    driver: bridge

volumes:
  minio-data:
  postgres-data:
```

### Sample Test Data

Sample datasets are stored in `tests/fixtures/data/` with source attribution. Data sourced from [data.europa.eu](https://data.europa.eu/en):

| Dataset | Description | Source | Columns |
|---------|-------------|--------|---------|
| `eu_energy_consumption.csv` | EU country energy consumption by year | [Eurostat Energy Statistics](https://data.europa.eu/data/datasets/eu-energy-statistics) | country, year, energy_type, consumption_gwh, population |
| `eu_air_quality.csv` | Air quality measurements by city | [European Environment Agency](https://data.europa.eu/data/datasets/air-quality) | city, country, date, pm25, pm10, no2, o3, station_id |
| `eu_transport_statistics.csv` | Transport infrastructure data | [Eurostat Transport](https://data.europa.eu/data/datasets/transport-statistics) | country, year, mode, passengers_millions, freight_tonnes |

These datasets provide:
- Various data types (strings, integers, floats, dates)
- Multiple tables for namespace/table browsing tests
- Realistic data for query and filter testing
- Partitionable columns (country, year) for Iceberg features

### Performance Targets
- Sub-second metadata operations
- Query execution within 5s for typical GB-scale queries
- Streaming results start within 500ms of query completion
- UI interactions feel responsive (<100ms feedback)

## Success Metrics

- All user stories completed with acceptance criteria met
- 90%+ test coverage
- Pre-commit hooks passing (ruff, pyupgrade)
- Application starts and serves requests successfully
- Can browse catalog, view schemas, execute queries, export CSV
- Health endpoints return correct status

## Open Questions

*All initial questions have been resolved:*

1. ~~Should we support query plan visualization (EXPLAIN) in MVP?~~ → **No, deferred**
2. ~~What sample data should we include for testing/demos?~~ → **EU open data from data.europa.eu (see Testing section)**
3. ~~Should the Monaco editor auto-complete be in MVP or deferred?~~ → **Deferred to post-MVP**
4. ~~Do we need WebSocket for real-time query progress, or is polling sufficient?~~ → **Polling for v1**

## References

- [DuckDB Iceberg Extension](https://duckdb.org/docs/stable/core_extensions/iceberg/overview.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [HTMX Documentation](https://htmx.org/)
- [Alpine.js Documentation](https://alpinejs.dev/)
- [Granian ASGI Server](https://granian.dev/)
- [FastHX Library](https://volfpeter.github.io/fasthx/)
- [Lakekeeper Iceberg REST Catalog](https://docs.lakekeeper.io/)
- [Lakekeeper GitHub](https://github.com/lakekeeper/lakekeeper)
- [Iceberg REST Catalog API - Namespaces](https://medium.com/data-engineering-with-dremio/iceberg-rest-catalog-overview-managing-namespaces-ffebfdcf1998)
- [OpenTelemetry Python](https://opentelemetry.io/docs/languages/python/)
- [EU Open Data Portal](https://data.europa.eu/en)
