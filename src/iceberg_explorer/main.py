"""Main entry point for Iceberg Explorer."""

from fastapi import FastAPI

app = FastAPI(
    title="Iceberg Explorer",
    description="High-performance web application for interactive exploration of Apache Iceberg data lakes",
    version="0.1.0",
)


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
