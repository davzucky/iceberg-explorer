"""Health and readiness endpoints for monitoring.

Provides:
- GET /health - Returns application health status
- GET /ready - Returns readiness for traffic
"""

from __future__ import annotations

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field

from iceberg_explorer import __version__
from iceberg_explorer.query.engine import get_engine

router = APIRouter(tags=["health"])


class ComponentHealth(BaseModel):
    """Health status of a component."""

    healthy: bool = Field(..., description="Whether the component is healthy.")
    error: str | None = Field(default=None, description="Error message if unhealthy.")


class HealthResponse(BaseModel):
    """Response for health check endpoint."""

    status: str = Field(..., description="Overall status: healthy, degraded, or unhealthy.")
    version: str = Field(..., description="Application version.")
    components: dict[str, ComponentHealth] = Field(
        ...,
        description="Health status of individual components.",
    )


class ReadyResponse(BaseModel):
    """Response for readiness check endpoint."""

    ready: bool = Field(..., description="Whether the application is ready for traffic.")
    reason: str | None = Field(default=None, description="Reason if not ready.")


@router.get("/health", response_model=HealthResponse)
async def health_check(response: Response) -> HealthResponse:
    """Check application health.

    Returns the overall health status along with component-level health.
    Checks DuckDB connectivity and catalog accessibility.

    Returns:
        HealthResponse with status, version, and component health.
        Returns 503 status code if unhealthy.
    """
    components: dict[str, ComponentHealth] = {}

    try:
        engine = get_engine()
        if not engine.is_initialized:
            engine.initialize()

        engine_health = engine.health_check()

        components["duckdb"] = ComponentHealth(
            healthy=engine_health.get("duckdb", False),
            error=None if engine_health.get("duckdb") else engine_health.get("error"),
        )
        components["catalog"] = ComponentHealth(
            healthy=engine_health.get("catalog", False),
            error=None if engine_health.get("catalog") else engine_health.get("error"),
        )

        overall_healthy = engine_health.get("healthy", False)

    except Exception as e:
        components["duckdb"] = ComponentHealth(healthy=False, error=str(e))
        components["catalog"] = ComponentHealth(healthy=False, error=str(e))
        overall_healthy = False

    if overall_healthy:
        status = "healthy"
    elif any(c.healthy for c in components.values()):
        status = "degraded"
        response.status_code = 503
    else:
        status = "unhealthy"
        response.status_code = 503

    return HealthResponse(
        status=status,
        version=__version__,
        components=components,
    )


@router.get("/ready", response_model=ReadyResponse)
async def readiness_check(response: Response) -> ReadyResponse:
    """Check application readiness for traffic.

    Returns whether the application is ready to receive traffic.
    Requires both DuckDB and catalog to be healthy.

    Returns:
        ReadyResponse with ready status.
        Returns 503 status code if not ready.
    """
    try:
        engine = get_engine()
        if not engine.is_initialized:
            response.status_code = 503
            return ReadyResponse(ready=False, reason="Engine not initialized")

        engine_health = engine.health_check()

        if not engine_health.get("healthy", False):
            response.status_code = 503
            return ReadyResponse(
                ready=False,
                reason=engine_health.get("error", "Health check failed"),
            )

        return ReadyResponse(ready=True)

    except Exception as e:
        response.status_code = 503
        return ReadyResponse(ready=False, reason=str(e))
