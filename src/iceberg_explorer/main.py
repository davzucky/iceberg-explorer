"""Main entry point for Iceberg Explorer."""

from fastapi import FastAPI

from iceberg_explorer.api.routes.catalog import router as catalog_router
from iceberg_explorer.api.routes.health import router as health_router
from iceberg_explorer.api.routes.query import router as query_router

app = FastAPI(
    title="Iceberg Explorer",
    description="High-performance web application for interactive exploration of Apache Iceberg data lakes",
    version="0.1.0",
)

app.include_router(catalog_router)
app.include_router(health_router)
app.include_router(query_router)


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": "Iceberg Explorer"}


def main() -> None:
    """Run the application server."""
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
