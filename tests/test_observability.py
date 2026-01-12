"""Tests for OpenTelemetry instrumentation and observability."""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from iceberg_explorer.config import reset_settings
from iceberg_explorer.observability import (
    configure_logging,
    decrement_active_queries,
    get_logger,
    get_meter,
    get_tracer,
    increment_active_queries,
    record_query_duration,
    record_query_rows,
    reset_observability,
    setup_opentelemetry,
)
from iceberg_explorer.query.engine import reset_engine
from iceberg_explorer.query.executor import reset_executor


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global state before and after each test."""
    reset_settings()
    reset_engine()
    reset_executor()
    reset_observability()
    yield
    reset_settings()
    reset_engine()
    reset_executor()
    reset_observability()


@pytest.fixture
def test_app():
    """Create a test FastAPI app."""
    return FastAPI()


@pytest.fixture
def span_exporter():
    """Create an in-memory span exporter for testing."""
    return InMemorySpanExporter()


@pytest.fixture
def metric_reader():
    """Create an in-memory metric reader for testing."""
    return InMemoryMetricReader()


@pytest.fixture
def test_tracer_provider(span_exporter):
    """Set up a test tracer provider with in-memory exporter."""
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    trace.set_tracer_provider(provider)
    return provider


@pytest.fixture
def test_meter_provider(metric_reader):
    """Set up a test meter provider with in-memory reader."""
    provider = MeterProvider(metric_readers=[metric_reader])
    metrics.set_meter_provider(provider)
    return provider


class TestOpenTelemetrySetup:
    """Tests for OpenTelemetry setup and initialization."""

    def test_setup_without_otel_enabled(self, test_app):
        """Test setup when OpenTelemetry is disabled."""
        with patch("iceberg_explorer.observability.get_settings") as mock_settings:
            mock_settings.return_value.otel.enabled = False
            setup_opentelemetry(test_app)

    def test_setup_multiple_times_is_idempotent(self, test_app):
        """Test that calling setup multiple times doesn't fail."""
        with patch("iceberg_explorer.observability.get_settings") as mock_settings:
            mock_settings.return_value.otel.enabled = False
            setup_opentelemetry(test_app)
            setup_opentelemetry(test_app)

    def test_reset_observability(self):
        """Test reset_observability clears state."""
        reset_observability()


class TestTracer:
    """Tests for tracer functionality."""

    def test_get_tracer_returns_tracer(self):
        """Test get_tracer returns a valid tracer."""
        tracer = get_tracer()
        assert tracer is not None

    def test_get_tracer_cached(self):
        """Test get_tracer returns same instance."""
        tracer1 = get_tracer()
        tracer2 = get_tracer()
        assert tracer1 is tracer2


class TestMeter:
    """Tests for meter functionality."""

    def test_get_meter_returns_meter(self):
        """Test get_meter returns a valid meter."""
        meter = get_meter()
        assert meter is not None

    def test_get_meter_cached(self):
        """Test get_meter returns same instance."""
        meter1 = get_meter()
        meter2 = get_meter()
        assert meter1 is meter2


class TestLogging:
    """Tests for structured logging."""

    def test_configure_logging(self):
        """Test logging configuration."""
        configure_logging()

    def test_get_logger_default_name(self):
        """Test get_logger with default name."""
        logger = get_logger()
        assert logger is not None

    def test_get_logger_custom_name(self):
        """Test get_logger with custom name."""
        logger = get_logger("custom_module")
        assert logger is not None


class TestMetricRecording:
    """Tests for metric recording functions."""

    def test_record_query_duration_no_histogram(self):
        """Test record_query_duration when histogram not initialized."""
        record_query_duration(1.5, "completed")

    def test_record_query_rows_no_counter(self):
        """Test record_query_rows when counter not initialized."""
        record_query_rows(100)

    def test_increment_active_queries_no_gauge(self):
        """Test increment_active_queries when gauge not initialized."""
        increment_active_queries()

    def test_decrement_active_queries_no_gauge(self):
        """Test decrement_active_queries when gauge not initialized."""
        decrement_active_queries()


class TestTracingIntegration:
    """Tests for tracing integration with query executor."""

    def test_query_execution_creates_span(
        self,
        test_tracer_provider,  # noqa: ARG002
        span_exporter,
    ):
        """Test that query execution creates a DuckDB span."""
        from iceberg_explorer.query.executor import QueryExecutor

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.get_connection.return_value.__exit__ = MagicMock(return_value=None)

        mock_arrow = MagicMock()
        mock_arrow.to_batches.return_value = []
        mock_arrow.schema = MagicMock()
        mock_conn.execute.return_value.fetch_arrow_table.return_value = mock_arrow

        executor = QueryExecutor(engine=mock_engine)

        with contextlib.suppress(Exception):
            executor.execute("SELECT 1", timeout=30)

        spans = span_exporter.get_finished_spans()
        query_spans = [s for s in spans if s.name == "duckdb.query"]
        assert len(query_spans) >= 1

        span = query_spans[0]
        assert span.attributes.get("db.system") == "duckdb"
        assert span.attributes.get("db.operation") == "SELECT"
        assert "query.id" in span.attributes

    def test_failed_query_sets_result_failed(self):
        """Test that failed queries set failed state on result."""
        from iceberg_explorer.query.executor import QueryExecutor
        from iceberg_explorer.query.models import QueryState

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.get_connection.return_value.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.side_effect = Exception("Database error")

        executor = QueryExecutor(engine=mock_engine)

        with pytest.raises(Exception, match="Database error"):
            executor.execute("SELECT * FROM non_existent", timeout=30)

        result = list(executor._active_queries.values())
        assert len(result) >= 1
        assert result[0].state == QueryState.FAILED


class TestMetricsIntegration:
    """Tests for metrics integration."""

    def test_metrics_created_when_otel_enabled(
        self,
        test_app,
        test_meter_provider,  # noqa: ARG002
        metric_reader,  # noqa: ARG002
    ):
        """Test that metrics are created when OTel is enabled."""
        with (
            patch("iceberg_explorer.observability.get_settings") as mock_settings,
            patch("iceberg_explorer.observability.OTLPSpanExporter"),
            patch("iceberg_explorer.observability.OTLPMetricExporter"),
            patch("iceberg_explorer.observability.BatchSpanProcessor"),
            patch("iceberg_explorer.observability.PeriodicExportingMetricReader"),
            patch("iceberg_explorer.observability.TracerProvider"),
            patch("iceberg_explorer.observability.MeterProvider"),
            patch("iceberg_explorer.observability.FastAPIInstrumentor"),
        ):
            mock_settings.return_value.otel.enabled = True
            mock_settings.return_value.otel.endpoint = "http://localhost:4317"
            mock_settings.return_value.otel.service_name = "test-service"

            reset_observability()
            setup_opentelemetry(test_app)


class TestFastAPIIntegration:
    """Tests for FastAPI instrumentation integration."""

    def test_app_instrumented(
        self,
        test_tracer_provider,  # noqa: ARG002
        span_exporter,  # noqa: ARG002
    ):
        """Test that the FastAPI app is properly instrumented."""
        from iceberg_explorer.main import app

        with patch("iceberg_explorer.api.routes.health.get_engine") as mock_get_engine:
            mock_engine = MagicMock()
            mock_engine.is_initialized = True
            mock_engine.health_check.return_value = {
                "healthy": True,
                "duckdb": True,
                "catalog": True,
            }
            mock_get_engine.return_value = mock_engine

            client = TestClient(app)
            response = client.get("/health")
            assert response.status_code == 200


class TestTraceContext:
    """Tests for trace context in logs."""

    def test_trace_context_added_to_logs(
        self,
        test_tracer_provider,  # noqa: ARG002
    ):
        """Test that trace context is added to log events."""
        from iceberg_explorer.observability import _add_trace_context

        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("test-span"):
            event_dict = {"event": "test"}
            result = _add_trace_context(None, "info", event_dict)

            assert "trace_id" in result
            assert "span_id" in result
            assert len(result["trace_id"]) == 32
            assert len(result["span_id"]) == 16

    def test_trace_context_not_added_without_span(self):
        """Test that trace context is not added when no active span."""
        from iceberg_explorer.observability import _add_trace_context

        reset_observability()
        event_dict = {"event": "test"}
        result = _add_trace_context(None, "info", event_dict)

        assert "trace_id" not in result or result.get("trace_id") is None


class TestNoSensitiveData:
    """Tests to ensure no sensitive data in telemetry."""

    def test_sql_not_in_span_attributes(
        self,
        test_tracer_provider,  # noqa: ARG002
        span_exporter,
    ):
        """Test that SQL content is not recorded in span attributes by default."""
        from iceberg_explorer.query.executor import QueryExecutor

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.get_connection.return_value.__exit__ = MagicMock(return_value=None)

        mock_arrow = MagicMock()
        mock_arrow.to_batches.return_value = []
        mock_arrow.schema = MagicMock()
        mock_conn.execute.return_value.fetch_arrow_table.return_value = mock_arrow

        executor = QueryExecutor(engine=mock_engine)

        sensitive_sql = "SELECT password FROM users WHERE email = 'test@example.com'"
        with contextlib.suppress(Exception):
            executor.execute(sensitive_sql, timeout=30)

        spans = span_exporter.get_finished_spans()
        query_spans = [s for s in spans if s.name == "duckdb.query"]

        for span in query_spans:
            attrs = dict(span.attributes)
            assert "db.statement" not in attrs
            for _key, value in attrs.items():
                if isinstance(value, str):
                    assert sensitive_sql not in value
                    assert "password" not in value.lower()
                    assert "test@example.com" not in value


class TestActiveQueriesMetric:
    """Tests for active queries gauge."""

    def test_increment_and_decrement_active_queries(self):
        """Test incrementing and decrementing active queries."""
        increment_active_queries()
        decrement_active_queries()
