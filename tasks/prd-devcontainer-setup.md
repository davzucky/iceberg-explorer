# PRD: DevContainer Setup with Lakekeeper and MinIO

## Introduction

Create a devcontainer configuration for Iceberg Explorer that enables local development and testing against a real Apache Iceberg environment. The setup includes Lakekeeper (REST catalog) and MinIO (S3-compatible storage), allowing developers to work with the full stack without external dependencies. Sample data will be automatically seeded on container startup.

## Goals

- Enable one-command local development environment setup via VS Code or `devcontainer up`
- Provide a working Lakekeeper REST catalog connected to MinIO storage
- Auto-seed demo namespaces, tables, and sample data for immediate testing
- Ensure all services are healthy and interconnected before development starts
- Keep the setup minimal and focused on core functionality

## User Stories

### US-001: Create devcontainer directory structure
**Description:** As a developer, I want a proper `.devcontainer` directory structure so that VS Code and the devcontainer CLI can recognize and build the environment.

**Acceptance Criteria:**
- [ ] `.devcontainer/` directory exists at repository root
- [ ] `devcontainer.json` file exists with valid JSON schema
- [ ] `docker-compose.yml` file exists for multi-container orchestration
- [ ] `devcontainer templates apply` or manual creation succeeds
- [ ] Lint check passes (valid JSON/YAML)

### US-002: Configure Python development container
**Description:** As a developer, I want the main development container to have Python 3.12 and uv package manager so I can run the Iceberg Explorer application.

**Acceptance Criteria:**
- [ ] Dev container uses Python 3.12 base image (mcr.microsoft.com/devcontainers/python:3.12)
- [ ] uv package manager is installed and available in PATH
- [ ] Project dependencies can be installed via `uv sync --all-extras`
- [ ] `uv run iceberg-explorer` starts the application successfully
- [ ] `uv run pytest` runs tests successfully
- [ ] Container has `/workspace` mounted to project root

### US-003: Configure MinIO S3 storage service
**Description:** As a developer, I want MinIO running as S3-compatible storage so that Iceberg tables have a place to store data files.

**Acceptance Criteria:**
- [ ] MinIO service defined in docker-compose.yml
- [ ] MinIO accessible on port 9000 (S3 API) within container network
- [ ] MinIO console accessible on port 9001 from host browser
- [ ] Default credentials configured (minioadmin/minioadmin for dev)
- [ ] `iceberg-warehouse` bucket automatically created on startup
- [ ] MinIO healthcheck passes before dependent services start

### US-004: Configure Lakekeeper REST catalog service
**Description:** As a developer, I want Lakekeeper running as an Iceberg REST catalog so I can browse namespaces and tables through the Iceberg Explorer.

**Acceptance Criteria:**
- [ ] Lakekeeper service defined in docker-compose.yml
- [ ] PostgreSQL service defined for Lakekeeper metadata storage
- [ ] Lakekeeper REST API accessible at `http://lakekeeper:8181/catalog` within container network
- [ ] Lakekeeper configured to use MinIO as storage backend
- [ ] Lakekeeper healthcheck passes before app container starts
- [ ] A default warehouse named "demo" is configured

### US-005: Configure environment variables for Iceberg Explorer
**Description:** As a developer, I want environment variables pre-configured so that Iceberg Explorer connects to Lakekeeper and MinIO automatically.

**Acceptance Criteria:**
- [ ] `ICEBERG_EXPLORER_CATALOG__TYPE=rest` is set
- [ ] `ICEBERG_EXPLORER_CATALOG__URI=http://lakekeeper:8181/catalog` is set
- [ ] `ICEBERG_EXPLORER_CATALOG__WAREHOUSE=demo` is set
- [ ] AWS credentials for MinIO access are configured
- [ ] `AWS_ENDPOINT_URL=http://minio:9000` is set
- [ ] Application starts without manual configuration

### US-006: Create data seeding script
**Description:** As a developer, I want sample data automatically seeded so I can immediately explore and test the application.

**Acceptance Criteria:**
- [ ] Seed script creates at least 2 namespaces (e.g., `nyc`, `sales`)
- [ ] Seed script creates at least 2 tables per namespace with different schemas
- [ ] Tables contain sample rows (at least 100 rows per table)
- [ ] Seeding runs automatically on first container startup
- [ ] Seeding is idempotent (safe to run multiple times)
- [ ] Seeding completes before VS Code attaches or returns success

### US-007: Verify end-to-end connectivity
**Description:** As a developer, I want to verify the entire setup works so I can confidently start developing.

**Acceptance Criteria:**
- [ ] `devcontainer up --workspace-folder .` completes successfully
- [ ] `uv run iceberg-explorer` starts and serves on port 8000
- [ ] Iceberg Explorer UI shows seeded namespaces in sidebar
- [ ] SQL query execution returns results from seeded tables
- [ ] `devcontainer exec` can run commands in the container
- [ ] Container can be stopped and restarted without data loss (within session)

## Functional Requirements

- FR-1: The devcontainer must use Docker Compose for multi-container orchestration
- FR-2: The MinIO service must create the `iceberg-warehouse` bucket on startup using an init container or entrypoint script
- FR-3: The Lakekeeper service must wait for PostgreSQL to be healthy before starting
- FR-4: The development container must wait for Lakekeeper to be healthy before running post-create commands
- FR-5: Environment variables must be set via `containerEnv` in devcontainer.json
- FR-6: The seed script must use PyIceberg or DuckDB Iceberg extension to create tables
- FR-7: Port 8000 (app), 9001 (MinIO console) must be forwarded to host
- FR-8: The devcontainer must work with both VS Code Dev Containers extension and `devcontainer` CLI

## Non-Goals

- Authentication/authorization for Lakekeeper or MinIO (dev-only credentials are fine)
- Persistent storage volumes (ephemeral storage acceptable for dev)
- Production-grade security configuration
- Spark or other heavy query engines
- CI/CD pipeline configuration (separate task)
- Remote devcontainer hosting (Codespaces, etc.)
- Time-travel or snapshot features in seed data

## Technical Considerations

- Use official `lakekeeper/lakekeeper` Docker image
- Use official `minio/minio` Docker image
- Use `postgres:15` or later for Lakekeeper metadata
- Consider using `ghcr.io/astral-sh/uv` feature for uv installation
- Network mode should allow inter-container communication via service names
- The existing `.env` file has partial configuration that can be referenced
- DuckDB with `iceberg` extension can create/query Iceberg tables via REST catalog

## Success Metrics

- Developer can go from `git clone` to running app in under 5 minutes
- No manual configuration steps required after opening in VS Code
- All seeded data visible and queryable through Iceberg Explorer UI
- `devcontainer up` completes without errors on Linux/macOS/Windows (via WSL2)

## Open Questions

- Should we include a VS Code extensions list (Python, Ruff, etc.) in devcontainer.json?
- What sample data schemas would be most useful for testing? (NYC taxi data style, or generic sales data?)
- Should the seed script be Python-based (using PyIceberg) or SQL-based (using DuckDB)?
