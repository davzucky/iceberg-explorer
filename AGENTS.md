# AGENTS.md - Iceberg Explorer

## Project Overview

Iceberg Explorer is a high-performance web application for interactive exploration of Apache Iceberg data lakes. It provides catalog browsing, SQL query execution, and CSV export capabilities.

**Tech Stack:**
- Backend: FastAPI, DuckDB (with Iceberg extension), Granian
- Frontend: HTMX, Alpine.js, Tailwind CSS
- Observability: OpenTelemetry, structlog

## Commands

### Development

```bash
# Install dependencies
uv sync --all-extras

# Start development server
uv run iceberg-explorer

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov

# Linting
uv run ruff check src/

# Linting with auto-fix
uv run ruff check src/ --fix

# Format code
uv run ruff format src/ tests/

# Install pre-commit hooks
uv run pre-commit install
```

### Verification (run after changes)

```bash
uv run ruff check src/ tests/
uv run pytest
```

## Project Structure

```
src/iceberg_explorer/
├── __init__.py
├── main.py              # Application entry point, FastAPI app setup
├── config.py            # Pydantic configuration models, env var loading
├── observability.py     # OpenTelemetry setup, structured logging
├── api/
│   ├── __init__.py
│   └── routes/
│       ├── __init__.py
│       ├── catalog.py   # Namespace/table browsing endpoints
│       ├── export.py    # CSV export endpoint
│       ├── health.py    # Health and readiness endpoints
│       ├── query.py     # Query execution endpoints
│       ├── ui.py        # HTML template routes
│       └── utils.py     # Shared route utilities
├── models/
│   ├── __init__.py
│   ├── catalog.py       # Catalog response models
│   └── query.py         # Query request/response models
├── query/
│   ├── __init__.py
│   ├── engine.py        # DuckDB connection/catalog management
│   ├── executor.py      # Query execution logic
│   └── models.py        # Internal query state models
└── templates/
    ├── base.html        # Base layout template
    ├── index.html       # Catalog browser page
    ├── query.html       # SQL query editor page
    └── partials/
        ├── namespace_tree.html     # Sidebar namespace tree
        ├── namespace_children.html # HTMX partial for lazy loading
        └── table_details.html      # Table metadata view

tests/
├── test_catalog.py      # Catalog API tests
├── test_config.py       # Configuration tests
├── test_engine.py       # DuckDB engine tests
├── test_executor.py     # Query executor tests
├── test_export.py       # CSV export tests
├── test_health.py       # Health endpoint tests
├── test_main.py         # Application startup tests
├── test_observability.py # Observability tests
├── test_query_api.py    # Query API tests
└── test_ui.py           # UI route tests
```

## Code Conventions

### Python Style

- Python 3.11+ features are encouraged (type hints, `match` statements, etc.)
- Use `ruff` for linting and formatting (line length: 100)
- Imports sorted with `isort` rules via ruff
- Pydantic v2 for all data models
- Async/await patterns for FastAPI routes
- structlog for logging (no print statements)

### Testing

- pytest with pytest-asyncio for async tests
- httpx for API testing
- Test files mirror source structure with `test_` prefix
- Use fixtures for DuckDB connections and test catalogs
- Target: 70% unit, 20% integration, 10% e2e

### API Design

- REST endpoints under `/api/v1/`
- Namespace paths use unit separator (`%1F`) for multi-level
- Streaming responses use JSON lines format
- All queries are SELECT-only (read-only mode enforced)

### Frontend

- HTMX for server-driven interactivity
- Alpine.js for client-side state
- Templates in Jinja2 format
- Partials for HTMX swaps in `templates/partials/`

## Configuration

Configuration via JSON file (`ICEBERG_EXPLORER_CONFIG` env var) with env var overrides:

- `ICEBERG_EXPLORER_QUERY__MAX_ROWS` - Max rows per query
- `ICEBERG_EXPLORER_QUERY__TIMEOUT` - Default query timeout (seconds)
- `ICEBERG_EXPLORER_DUCKDB__MEMORY_LIMIT` - DuckDB memory limit
- `ICEBERG_EXPLORER_DUCKDB__THREADS` - DuckDB thread count

## Key APIs

### Catalog Endpoints

- `GET /api/v1/catalog/namespaces` - List namespaces
- `GET /api/v1/catalog/namespaces/{namespace}/tables` - List tables
- `GET /api/v1/catalog/tables/{namespace}.{table}` - Table details
- `GET /api/v1/catalog/tables/{namespace}.{table}/schema` - Table schema

### Query Endpoints

- `POST /api/v1/query/execute` - Execute SQL query
- `GET /api/v1/query/{query_id}/results` - Stream results (JSON lines)
- `GET /api/v1/query/{query_id}/status` - Query status
- `POST /api/v1/query/{query_id}/cancel` - Cancel query

### Export Endpoints

- `POST /api/v1/export/csv` - Export to CSV

### Health Endpoints

- `GET /health` - Health check with component status
- `GET /ready` - Readiness probe

## Non-Goals (Out of Scope)

- Authentication/Authorization
- Query history persistence
- Write operations (INSERT, UPDATE, DELETE)
- Multi-catalog support
- Docker/Kubernetes deployment
- Time-travel queries

## PRD Reference

Full product requirements in `prd.json` with 21 user stories across 7 phases.
