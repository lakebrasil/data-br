"""
Observability setup: structlog + OpenTelemetry + Rich.

Local: structlog renders pretty to stderr, OTel exports to stdout (or local
collector if OTEL_EXPORTER_OTLP_ENDPOINT is set), Rich renders progress bars
to stdout.

Lambda: structlog renders JSON to CloudWatch Logs, OTel exports OTLP to ADOT
collector (preinstalled layer), Rich falls back to plain logs (no TTY).

Standardize:
  - Logger:  structlog.get_logger(__name__)
  - Tracer:  observability.tracer.start_as_current_span("my.op")
  - Metrics: meter.create_counter("data_br_files_fetched") etc.
"""

from __future__ import annotations

import logging
import os
import sys

import structlog
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

SERVICE_NAME = "data-br-fetch"
SERVICE_VERSION = os.environ.get("APP_VERSION", "0.1.0")
ENVIRONMENT = os.environ.get("APP_ENV", "local")

_tracer: trace.Tracer | None = None
_meter: metrics.Meter | None = None
_initialized = False


def init() -> None:
    """Configure structlog + OTel SDK exactly once. Idempotent."""
    global _tracer, _meter, _initialized
    if _initialized:
        return

    is_tty = sys.stderr.isatty()
    log_level = logging.DEBUG if os.environ.get("LOG_LEVEL") == "DEBUG" else logging.INFO

    # ── structlog ──
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    if is_tty:
        # Local dev: pretty colored output to stderr.
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        # Lambda / CI: JSON for CloudWatch parse.
        renderer = structlog.processors.JSONRenderer()
    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # ── OTel resource ──
    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
            "service.version": SERVICE_VERSION,
            "deployment.environment": ENVIRONMENT,
        }
    )

    # ── Tracing ──
    tracer_provider = TracerProvider(resource=resource)
    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        # Production / Lambda with ADOT collector at sidecar.
        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    elif os.environ.get("OTEL_TRACES_CONSOLE") == "1":
        # Local debug: dump spans to stderr.
        tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    # else: spans recorded but not exported (dev default — no spam).
    trace.set_tracer_provider(tracer_provider)
    _tracer = trace.get_tracer(SERVICE_NAME, SERVICE_VERSION)

    # ── Metrics ──
    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=15000)
        meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    else:
        meter_provider = MeterProvider(resource=resource)
    metrics.set_meter_provider(meter_provider)
    _meter = metrics.get_meter(SERVICE_NAME, SERVICE_VERSION)

    _initialized = True


def get_tracer() -> trace.Tracer:
    if not _initialized:
        init()
    assert _tracer is not None
    return _tracer


def get_meter() -> metrics.Meter:
    if not _initialized:
        init()
    assert _meter is not None
    return _meter


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    if not _initialized:
        init()
    return structlog.get_logger(name)


# Pre-defined metrics — single source of truth.
class Metrics:
    """Lazy-bound metrics. Call after init()."""

    _files_fetched: metrics.Counter | None = None
    _bytes_downloaded: metrics.Counter | None = None
    _fetch_errors: metrics.Counter | None = None
    _fetch_duration_ms: metrics.Histogram | None = None

    @classmethod
    def files_fetched(cls) -> metrics.Counter:
        if cls._files_fetched is None:
            cls._files_fetched = get_meter().create_counter(
                "data_br_files_fetched", unit="1", description="Files successfully fetched"
            )
        return cls._files_fetched

    @classmethod
    def bytes_downloaded(cls) -> metrics.Counter:
        if cls._bytes_downloaded is None:
            cls._bytes_downloaded = get_meter().create_counter(
                "data_br_bytes_downloaded", unit="By", description="Bytes downloaded"
            )
        return cls._bytes_downloaded

    @classmethod
    def fetch_errors(cls) -> metrics.Counter:
        if cls._fetch_errors is None:
            cls._fetch_errors = get_meter().create_counter(
                "data_br_fetch_errors", unit="1", description="Fetch failures (after retries)"
            )
        return cls._fetch_errors

    @classmethod
    def fetch_duration_ms(cls) -> metrics.Histogram:
        if cls._fetch_duration_ms is None:
            cls._fetch_duration_ms = get_meter().create_histogram(
                "data_br_fetch_duration_ms", unit="ms", description="Per-file fetch duration"
            )
        return cls._fetch_duration_ms
