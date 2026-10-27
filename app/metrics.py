"""Prometheus exposition layer (text format at GET /metrics).

Deliberately uses prometheus_client directly for the HTTP-facing metrics
(counters/histograms with proper bucketing) while OTel handles traces —
traces and metrics are complementary, not competing signals.
"""
from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.requests import Request
from starlette.responses import Response

HTTP_REQUESTS = Counter(
    "booking_http_requests_total",
    "Total HTTP requests",
    ["method", "route", "status"],
)
HTTP_LATENCY = Histogram(
    "booking_http_request_duration_seconds",
    "End-to-end request latency (includes PG lock wait + Redis hops)",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
LOCK_WAIT = Histogram(
    "booking_pg_lock_wait_seconds",
    "Time blocked acquiring seat row lock via SELECT FOR UPDATE",
    ["outcome"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)
OUTBOX_PENDING = Gauge("booking_outbox_pending_messages", "Outbox rows awaiting relay")
OUTBOX_PUBLISHED = Counter("booking_outbox_published_total", "Outbox messages relayed to Redis Streams")
OUTBOX_DEAD = Counter("booking_outbox_dead_total", "Outbox messages moved to dead status")
WEBHOOK_REJECTS = Counter("booking_webhook_signature_rejects_total", "Webhooks rejected by signature verification")
RATE_LIMITED = Counter("booking_rate_limited_total", "Requests shed by the rate limiter", ["bucket_class"])


def observe_request(method: str, route: str, status: int, duration: float) -> None:
    HTTP_REQUESTS.labels(method, route, str(status)).inc()
    HTTP_LATENCY.labels(method, route).observe(duration)


def render_metrics(request: Request) -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
