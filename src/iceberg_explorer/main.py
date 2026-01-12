"""Main entry point for Iceberg Explorer."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from iceberg_explorer import __version__
from iceberg_explorer.api.routes.catalog import router as catalog_router
from iceberg_explorer.api.routes.export import router as export_router
from iceberg_explorer.api.routes.health import router as health_router
from iceberg_explorer.api.routes.query import router as query_router
from iceberg_explorer.api.routes.ui import router as ui_router
from iceberg_explorer.observability import setup_opentelemetry, shutdown_opentelemetry


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan - setup and shutdown."""
    setup_opentelemetry(app)
    yield
    shutdown_opentelemetry()


app = FastAPI(
    title="Iceberg Explorer",
    description="High-performance web application for interactive exploration of Apache Iceberg data lakes",
    version=__version__,
    lifespan=lifespan,
)

app.include_router(catalog_router)
app.include_router(export_router)
app.include_router(health_router)
app.include_router(query_router)
app.include_router(ui_router)


def main() -> None:
    """Run the application server."""
    import uvicorn

    from iceberg_explorer.config import get_settings

    settings = get_settings()
    uvicorn.run(app, host=settings.server.host, port=settings.server.port)


if __name__ == "__main__":
    main()
