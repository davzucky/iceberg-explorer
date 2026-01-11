# Iceberg Explorer

High-performance web application for interactive exploration of Apache Iceberg data lakes.

## Features

- Browse Iceberg catalog namespaces and tables
- View table schema, partitioning, and snapshot history
- Execute SQL queries against Iceberg tables using DuckDB
- Export query results to CSV
- Real-time streaming of query results

## Tech Stack

- **Backend**: FastAPI, DuckDB (with Iceberg extension), Granian
- **Frontend**: HTMX, Alpine.js, Tailwind CSS
- **Observability**: OpenTelemetry, structlog

## Development

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

### Setup

```bash
# Install dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Run linting
uv run ruff check src/

# Start development server
uv run iceberg-explorer
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
uv run pre-commit install
```

## License

MIT
