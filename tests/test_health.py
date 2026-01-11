"""Tests for health and readiness endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from iceberg_explorer import __version__
from iceberg_explorer.config import reset_settings
from iceberg_explorer.main import app
from iceberg_explorer.query.engine import reset_engine
from iceberg_explorer.query.executor import reset_executor


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global state before and after each test."""
    reset_settings()
    reset_engine()
    reset_executor()
    yield
    reset_settings()
    reset_engine()
    reset_executor()


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


class TestHealthModels:
    """Tests for health-related Pydantic models."""

    def test_component_health_model(self):
        """Test ComponentHealth model."""
        from iceberg_explorer.api.routes.health import ComponentHealth

        component = ComponentHealth(healthy=True)
        assert component.healthy is True
        assert component.error is None

    def test_component_health_with_error(self):
        """Test ComponentHealth model with error."""
        from iceberg_explorer.api.routes.health import ComponentHealth

        component = ComponentHealth(healthy=False, error="Connection failed")
        assert component.healthy is False
        assert component.error == "Connection failed"

    def test_health_response_model(self):
        """Test HealthResponse model."""
        from iceberg_explorer.api.routes.health import ComponentHealth, HealthResponse

        response = HealthResponse(
            status="healthy",
            version="0.1.0",
            components={
                "duckdb": ComponentHealth(healthy=True),
                "catalog": ComponentHealth(healthy=True),
            },
        )
        assert response.status == "healthy"
        assert response.version == "0.1.0"
        assert len(response.components) == 2

    def test_ready_response_model(self):
        """Test ReadyResponse model."""
        from iceberg_explorer.api.routes.health import ReadyResponse

        response = ReadyResponse(ready=True)
        assert response.ready is True
        assert response.reason is None

    def test_ready_response_not_ready(self):
        """Test ReadyResponse model when not ready."""
        from iceberg_explorer.api.routes.health import ReadyResponse

        response = ReadyResponse(ready=False, reason="Catalog not available")
        assert response.ready is False
        assert response.reason == "Catalog not available"


class TestHealthEndpoint:
    """Tests for GET /health endpoint."""

    def test_health_all_healthy(self, client: TestClient):
        """Test health endpoint when everything is healthy."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": True,
            "duckdb": True,
            "catalog": True,
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["version"] == __version__
        assert data["components"]["duckdb"]["healthy"] is True
        assert data["components"]["catalog"]["healthy"] is True

    def test_health_duckdb_unhealthy(self, client: TestClient):
        """Test health endpoint when DuckDB is unhealthy."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": False,
            "duckdb": False,
            "catalog": False,
            "error": "DuckDB connection failed",
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/health")

        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["components"]["duckdb"]["healthy"] is False

    def test_health_catalog_unhealthy(self, client: TestClient):
        """Test health endpoint when catalog is unhealthy."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": False,
            "duckdb": True,
            "catalog": False,
            "error": "Catalog connection failed",
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/health")

        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["components"]["duckdb"]["healthy"] is True
        assert data["components"]["catalog"]["healthy"] is False

    def test_health_engine_not_initialized(self, client: TestClient):
        """Test health endpoint when engine needs initialization."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = False
        mock_engine.health_check.return_value = {
            "healthy": True,
            "duckdb": True,
            "catalog": True,
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/health")

        assert response.status_code == 200
        mock_engine.initialize.assert_called_once()

    def test_health_engine_exception(self, client: TestClient):
        """Test health endpoint when engine throws exception."""
        with patch(
            "iceberg_explorer.api.routes.health.get_engine",
            side_effect=RuntimeError("Engine initialization failed"),
        ):
            response = client.get("/health")

        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["components"]["duckdb"]["healthy"] is False
        assert data["components"]["catalog"]["healthy"] is False

    def test_health_includes_version(self, client: TestClient):
        """Test health endpoint includes version."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": True,
            "duckdb": True,
            "catalog": True,
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["version"] == __version__

    def test_health_degraded_status(self, client: TestClient):
        """Test health returns degraded when partially healthy."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": False,
            "duckdb": True,
            "catalog": False,
            "error": "Catalog timeout",
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/health")

        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"


class TestReadyEndpoint:
    """Tests for GET /ready endpoint."""

    def test_ready_when_healthy(self, client: TestClient):
        """Test ready endpoint when everything is healthy."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": True,
            "duckdb": True,
            "catalog": True,
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/ready")

        assert response.status_code == 200
        data = response.json()
        assert data["ready"] is True
        assert data["reason"] is None

    def test_ready_engine_not_initialized(self, client: TestClient):
        """Test ready endpoint when engine not initialized."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = False

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/ready")

        assert response.status_code == 503
        data = response.json()
        assert data["ready"] is False
        assert "not initialized" in data["reason"].lower()

    def test_ready_health_check_failed(self, client: TestClient):
        """Test ready endpoint when health check fails."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": False,
            "duckdb": True,
            "catalog": False,
            "error": "Catalog connection refused",
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/ready")

        assert response.status_code == 503
        data = response.json()
        assert data["ready"] is False
        assert "Catalog connection refused" in data["reason"]

    def test_ready_engine_exception(self, client: TestClient):
        """Test ready endpoint when engine throws exception."""
        with patch(
            "iceberg_explorer.api.routes.health.get_engine",
            side_effect=RuntimeError("Engine crashed"),
        ):
            response = client.get("/ready")

        assert response.status_code == 503
        data = response.json()
        assert data["ready"] is False
        assert "Engine crashed" in data["reason"]

    def test_ready_duckdb_unhealthy(self, client: TestClient):
        """Test ready endpoint when DuckDB is unhealthy."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": False,
            "duckdb": False,
            "catalog": True,
            "error": "DuckDB out of memory",
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/ready")

        assert response.status_code == 503
        data = response.json()
        assert data["ready"] is False

    def test_ready_catalog_unhealthy(self, client: TestClient):
        """Test ready endpoint when catalog is unhealthy."""
        mock_engine = MagicMock()
        mock_engine.is_initialized = True
        mock_engine.health_check.return_value = {
            "healthy": False,
            "duckdb": True,
            "catalog": False,
            "error": "Catalog timeout",
        }

        with patch(
            "iceberg_explorer.api.routes.health.get_engine", return_value=mock_engine
        ):
            response = client.get("/ready")

        assert response.status_code == 503
        data = response.json()
        assert data["ready"] is False
        assert "Catalog timeout" in data["reason"]
