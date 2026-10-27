"""OpenTelemetry wiring + Prometheus metrics + SQLAlchemy engine instrumentation.

Span model for a booking request::

    POST /v1/events/{id}/seats/{n}/hold        (FastAPI auto-instrumented)
      └─ tx.begin                              (session events)
         ├─ seat.lock.acquire                  ← time blocked on FOR UPDATE
         ├─ seat.hold.mutate                   ← business logic
         ├─ outbox.append                      ← durable enqueue (same tx)
         └─ tx.commit                          ← fsync latency visible
    webhook.enqueue                            (XADD hop, linked by traceparent)

The lock-wait span is the money chart: p99 of `seat.lock.acquire` under a
ticket drop tells you whether SKIP LOCKED is doing its job.
"""
from __future__ import annotations

import logging

from opentelemetry import metrics, trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Status, StatusCode
from sqlalchemy import event
from sqlalchemy.engine import Engine

from app.config import settings

logger = logging.getLogger("app.observability")
_instrumented = False


def setup_telemetry(engine: Engine | None = None) -> None:
    """Idempotent bootstrap: OTLP exporter when configured, console fallback in dev."""
    global _instrumented
    if _instrumented:
        return
    _instrumented = True

    resource = Resource(attributes={SERVICE_NAME: settings.otel_service_name})
    provider = TracerProvider(resource=resource)

    if settings.otlp_endpoint:
        # Lazy import keeps the console-only dev path dependency-light.
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{settings.otlp_endpoint}/v1/traces")))
    else:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)

    if engine is not None:
        SQLAlchemyInstrumentor().instrument(enable_commenter=True, commenter_options={}, engine=engine)

    _install_engine_event_spans()
    _setup_metrics()


def get_tracer(name: str = "booking-engine") -> trace.Tracer:
    return trace.get_tracer(name)


def propagate_traceparent() -> str | None:
    """Serialise current span context for embedding into an outbox row."""
    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier.get("traceparent")


def link_consumer_span(name: str, traceparent: str | None):  # noqa: ANN201
    """Start a span that is a CHILD of the original producer request's span."""
    tracer = get_tracer("app.consumer")
    ctx = extract({"traceparent": traceparent}) if traceparent else None
    return tracer.start_as_current_span(name, context=ctx)


# --------------------------------------------------------------------------
# Fine-grained DB spans: we want the *lock acquisition* separated from commit.
# --------------------------------------------------------------------------
def _install_engine_event_spans() -> None:
    tracer = get_tracer("app.database")

    @event.listens_for(Engine, "before_cursor_execute")
    def _before(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        context._otel_span_start = tracer.start_span(
            "db.execute",
            attributes={"db.statement": statement[:200], "db.system": conn.engine.dialect.name},
        )

    @event.listens_for(Engine, "after_cursor_execute")
    def _after(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        span = getattr(context, "_otel_span_start", None)
        if span is not None:
            span.set_status(Status(StatusCode.OK))
            span.end()

    @event.listens_for(Engine, "handle_error")
    def _error(exception_context):  # noqa: ANN001
        span = getattr(exception_context.context, "_otel_span_start", None) if exception_context.context else None
        if span is not None:
            span.record_exception(exception_context.original_exception)
            span.set_status(Status(StatusCode.ERROR))
            span.end()


# --------------------------------------------------------------------------
# Prometheus counters/histograms (exposed at GET /metrics).
# --------------------------------------------------------------------------
class Metrics:
    def __init__(self) -> None:
        meter = metrics.get_meter("booking-engine")
        self.requests_total = meter.create_counter("http_requests_total", description="Requests by route/status")
        self.holds_total = meter.create_counter("booking_holds_total", description="Seat holds by outcome")
        self.bookings_total = meter.create_counter("booking_confirmed_total", description="Webhook-driven confirmations")
        self.lock_wait_seconds = meter.create_histogram(
            "pg_lock_wait_seconds",
            description="Time blocked acquiring the seat row lock (FOR UPDATE)",
            unit="s",
        )
        self.webhook_signature_failures = meter.create_counter(
            "webhook_signature_failures_total",
            description="Rejected webhook signatures (security signal — alert on spikes)",
        )
        self.rate_limited_total = meter.create_counter("rate_limited_requests_total", description="429 responses issued")
        self.outbox_published = meter.create_counter("outbox_messages_published_total", description="Messages relayed to Redis")
        self.outbox_dead = meter.create_counter("outbox_messages_dead_total", description="Messages parked in DLQ state")


metrics_registry: Metrics | None = None


def _setup_metrics() -> None:
    global metrics_registry
    if not settings.metrics_enabled:
        return
    metrics_registry = Metrics()


def get_metrics() -> Metrics | None:
    return metrics_registry
