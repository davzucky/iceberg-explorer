"""Main entry point for Iceberg Explorer."""

from fastapi import FastAPI

from iceberg_explorer import __version__
from iceberg_explorer.api.routes.catalog import router as catalog_router
from iceberg_explorer.api.routes.health import router as health_router
from iceberg_explorer.api.routes.query import router as query_router
from iceberg_explorer.api.routes.ui import router as ui_router

app = FastAPI(
    title="Iceberg Explorer",
    description="High-performance web application for interactive exploration of Apache Iceberg data lakes",
    version=__version__,
)

app.include_router(catalog_router)
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
