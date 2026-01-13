"""OpenTelemetry instrumentation for Iceberg Explorer.

Provides:
- OpenTelemetry SDK initialization with auto-instrumentation
- Tracer for creating spans with trace context
- Metrics for query monitoring (duration, rows, active queries)
- Structured JSON logging with trace correlation
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import structlog
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from iceberg_explorer.config import get_settings

if TYPE_CHECKING:
    from fastapi import FastAPI

_initialized = False

_tracer: trace.Tracer | None = None
_meter: metrics.Meter | None = None
_tracer_provider: TracerProvider | None = None
_meter_provider: MeterProvider | None = None

_query_duration_histogram: metrics.Histogram | None = None
_query_rows_counter: metrics.Counter | None = None
_active_queries_gauge: metrics.UpDownCounter | None = None


def get_tracer() -> trace.Tracer:
    """Get the application tracer.

    Returns:
        The OpenTelemetry tracer for creating spans.
    """
    global _tracer
    if _tracer is None:
        _tracer = trace.get_tracer("iceberg_explorer")
    return _tracer


def get_meter() -> metrics.Meter:
    """Get the application meter.

    Returns:
        The OpenTelemetry meter for creating metrics.
    """
    global _meter
    if _meter is None:
        _meter = metrics.get_meter("iceberg_explorer")
    return _meter


def record_query_duration(duration_seconds: float, status: str = "completed") -> None:
    """Record query execution duration.

    Args:
        duration_seconds: Query duration in seconds.
        status: Query status (completed, failed, cancelled, timeout).
    """
    global _query_duration_histogram
    if _query_duration_histogram is not None:
        _query_duration_histogram.record(duration_seconds, {"status": status})


def record_query_rows(row_count: int) -> None:
    """Record number of rows returned from a query.

    Args:
        row_count: Number of rows returned.
    """
    global _query_rows_counter
    if _query_rows_counter is not None:
        _query_rows_counter.add(row_count)


def increment_active_queries() -> None:
    """Increment the active queries counter."""
    global _active_queries_gauge
    if _active_queries_gauge is not None:
        _active_queries_gauge.add(1)


def decrement_active_queries() -> None:
    """Decrement the active queries counter."""
    global _active_queries_gauge
    if _active_queries_gauge is not None:
        _active_queries_gauge.add(-1)


def _add_trace_context(
    logger: logging.Logger,  # noqa: ARG001
    method_name: str,  # noqa: ARG001
    event_dict: dict,
) -> dict:
    """Add OpenTelemetry trace context to log records.

    Args:
        logger: The logger instance (unused but required by structlog).
        method_name: The logging method name (unused but required by structlog).
        event_dict: The event dictionary to enhance.

    Returns:
        Event dictionary with trace context added.
    """
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def configure_logging() -> None:
    """Configure structured JSON logging with trace context."""
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.UnicodeDecoder(),
            _add_trace_context,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        level=logging.INFO,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a structured logger.

    Args:
        name: Logger name. Defaults to "iceberg_explorer".

    Returns:
        A structured logger with trace context support.
    """
    return structlog.get_logger(name or "iceberg_explorer")


def setup_opentelemetry(app: FastAPI) -> None:
    """Initialize OpenTelemetry instrumentation.

    Sets up:
    - Tracer provider with OTLP exporter
    - Meter provider with OTLP exporter
    - FastAPI auto-instrumentation
    - Metrics for query monitoring

    Args:
        app: FastAPI application to instrument.
    """
    global _initialized, _tracer, _meter, _tracer_provider, _meter_provider
    global _query_duration_histogram, _query_rows_counter, _active_queries_gauge

    if _initialized:
        return

    settings = get_settings()

    configure_logging()

    if not settings.otel.enabled:
        _initialized = True
        return

    resource = Resource.create({SERVICE_NAME: settings.otel.service_name})

    tracer_provider = TracerProvider(resource=resource)
    span_exporter = OTLPSpanExporter(
        endpoint=settings.otel.endpoint, insecure=settings.otel.insecure
    )
    span_processor = BatchSpanProcessor(span_exporter)
    tracer_provider.add_span_processor(span_processor)
    trace.set_tracer_provider(tracer_provider)
    _tracer_provider = tracer_provider

    metric_exporter = OTLPMetricExporter(
        endpoint=settings.otel.endpoint, insecure=settings.otel.insecure
    )
    metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=10000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)
    _meter_provider = meter_provider

    _tracer = trace.get_tracer("iceberg_explorer")
    _meter = metrics.get_meter("iceberg_explorer")

    _query_duration_histogram = _meter.create_histogram(
        name="query_duration_seconds",
        description="Duration of SQL query execution in seconds",
        unit="s",
    )

    _query_rows_counter = _meter.create_counter(
        name="query_rows_returned",
        description="Total number of rows returned from queries",
        unit="rows",
    )

    _active_queries_gauge = _meter.create_up_down_counter(
        name="active_queries",
        description="Number of currently executing queries",
        unit="queries",
    )

    FastAPIInstrumentor.instrument_app(app)

    _initialized = True


def shutdown_opentelemetry() -> None:
    """Shutdown OpenTelemetry providers to flush pending telemetry."""
    import contextlib

    global _tracer_provider, _meter_provider
    if _tracer_provider is not None:
        with contextlib.suppress(Exception):
            _tracer_provider.force_flush(timeout_millis=5000)
            _tracer_provider.shutdown()
        _tracer_provider = None
    if _meter_provider is not None:
        with contextlib.suppress(Exception):
            _meter_provider.force_flush(timeout_millis=5000)
            _meter_provider.shutdown()
        _meter_provider = None


def reset_observability() -> None:
    """Reset observability state (useful for testing)."""
    import contextlib

    global _initialized, _tracer, _meter, _tracer_provider, _meter_provider
    global _query_duration_histogram, _query_rows_counter, _active_queries_gauge

    # Uninstrument FastAPI to avoid double-instrumentation on re-setup
    with contextlib.suppress(Exception):
        FastAPIInstrumentor.uninstrument()

    _initialized = False
    _tracer = None
    _meter = None
    _tracer_provider = None
    _meter_provider = None
    _query_duration_histogram = None
    _query_rows_counter = None
    _active_queries_gauge = None
