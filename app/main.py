"""High-Concurrency Booking Engine — API surface.

Request pipeline (outermost -> innermost):

    RateLimitMiddleware (Redis sliding window, fail-closed)
      -> RequestMetricsMiddleware (Prometheus latency/counters)
         -> FastAPI router
              |- Depends(require_valid_webhook_signature)   # zero-trust gate
              |- Depends(verify_inbound_idempotency)        # replay guard
              `- app.repository                            # FOR UPDATE / SKIP LOCKED
                   `- app.outbox.add_outbox_message        # durable enqueue in-tx

The synchronous DB stack runs on Starlette's threadpool; OpenTelemetry's
FastAPIInstrumentor keeps the active span propagated into those handlers.
"""
from __future__ import annotations

import time
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import Base, engine, get_db
from app.metrics import observe_request, render_metrics
from app.models import Event, Seat, WebhookEvent, utc_now
from app.observability import get_tracer, setup_telemetry
from app.repository import (
    LockAcquisitionTimeout,
    acquire_seat_for_update,
    create_hold,
    process_payment_event,
)
from app.schemas import EventCreate, EventOut, SeatOut, WebhookIn, WebhookOut
from app.security import SignatureError, require_valid_webhook_signature
from app.services import _ReplayShortCircuit, record_idempotent_result, verify_inbound_idempotency

setup_telemetry(engine)
Base.metadata.create_all(bind=engine)

app = FastAPI(title="High-Concurrency Booking Engine", version="2.0.0")
tracer = get_tracer("app.api")


# --------------------------------------------------------------------------
# Middleware stack
# --------------------------------------------------------------------------
class RequestMetricsMiddleware:
    """Pure-ASGI timing middleware — no BaseHTTPMiddleware streaming overhead."""

    def __init__(self, app):  # noqa: ANN001
        self.app = app

    async def __call__(self, scope, receive, send):  # noqa: ANN001
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        start = time.perf_counter()
        status = 500

        async def send_wrapper(message):  # noqa: ANN001
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            route = scope.get("route")
            label = getattr(route, "path", None) or scope["path"]
            observe_request(scope["method"], label, status, time.perf_counter() - start)


app.add_middleware(RequestMetricsMiddleware)

if settings.rate_limit_enabled and settings.redis_url:
    from app.ratelimit import RateLimitMiddleware

    app.add_middleware(RateLimitMiddleware)


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    details = [{"type": e.get("type"), "loc": e.get("loc"), "msg": e.get("msg")} for e in exc.errors()]
    return JSONResponse(status_code=422, content={"error": {"code": "validation_error", "message": "Request validation failed", "details": jsonable_encoder(details)}})


@app.exception_handler(HTTPException)
async def http_error_handler(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, dict) else {"code": "request_error", "message": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content={"error": {**detail, "details": detail.get("details", [])}})


@app.exception_handler(_ReplayShortCircuit)
async def idempotent_replay_handler(request: Request, exc: _ReplayShortCircuit):
    """Dependency-level short circuit: replay returns the stored outcome."""
    return exc.response


@app.exception_handler(SignatureError)
async def signature_error_handler(request: Request, exc: SignatureError):
    from app.metrics import WEBHOOK_REJECTS

    WEBHOOK_REJECTS.inc()  # security signal: alert when this counter spikes
    return JSONResponse(status_code=exc.status_code, content={"error": {**exc.detail, "details": []}})


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics_endpoint(request: Request):
    return render_metrics(request)


@app.post("/v1/events", response_model=EventOut, status_code=201)
def create_event(payload: EventCreate, db: Session = Depends(get_db)):
    event = Event(id=str(uuid4()), name=payload.name.strip())
    db.add(event)
    db.add_all([Seat(id=str(uuid4()), event_id=event.id, number=n, status="available") for n in range(1, payload.seat_count + 1)])
    db.commit()
    return {"id": event.id, "name": event.name, "seat_count": payload.seat_count}


@app.get("/v1/events/{event_id}/seats", response_model=list[SeatOut])
def list_seats(event_id: str, limit: int = 100, db: Session = Depends(get_db)):
    if db.get(Event, event_id) is None:
        raise HTTPException(status_code=404, detail={"code": "event_not_found", "message": "Event not found"})
    statement = select(Seat).where(Seat.event_id == event_id).order_by(Seat.number).limit(max(1, min(limit, 500)))
    return list(db.scalars(statement))


@app.post("/v1/events/{event_id}/seats/{number}/hold", response_model=SeatOut)
def hold_seat(
    request: Request,
    event_id: str,
    number: int,
    idempotency_guard=Depends(verify_inbound_idempotency),
    db: Session = Depends(get_db),
):
    """Acquire an expiring hold on one seat.

    Concurrency contract:
      * SELECT ... FOR UPDATE with SET LOCAL lock_timeout bounds contention.
      * The hold row AND its outbox event commit atomically (no dual write).
      * Replay of the same Idempotency-Key returns the stored outcome.
    """
    with tracer.start_as_current_span("seat.hold", attributes={"seat.event_id": event_id, "seat.number": number}):
        try:
            seat = acquire_seat_for_update(db, event_id, number)
        except LockAcquisitionTimeout as exc:
            raise HTTPException(
                status_code=503,
                detail={"code": "seat_contention", "message": "Seat is hot right now; retry shortly."},
                headers={"Retry-After": "1"},
            ) from exc
        if seat is None:
            raise HTTPException(status_code=404, detail={"code": "seat_not_found", "message": "Seat not found"})
        now = utc_now()
        expiry = _aware(seat.hold_expires_at)
        if seat.status == "booked":
            raise HTTPException(status_code=409, detail={"code": "seat_booked", "message": "Seat is already booked"})
        if seat.status == "held" and expiry and expiry > now:
            raise HTTPException(status_code=409, detail={"code": "seat_held", "message": "Seat is currently held"})
        try:
            seat = create_hold(db, seat)
        except IntegrityError as exc:
            db.rollback()
            raise HTTPException(status_code=409, detail={"code": "seat_held", "message": "Seat was concurrently held"}) from exc
        record_idempotent_result(db, request, 200, SeatOut.model_validate(seat).model_dump(mode="json"))
        return seat


@app.post("/v1/webhooks/payment", response_model=WebhookOut)
def payment_webhook(
    request: Request,
    payload: WebhookIn,
    verified: dict = Depends(require_valid_webhook_signature),  # <-- zero-trust gate
    x_process_inline: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    """Payment provider callback.  Reaches this handler ONLY with a valid HMAC.

    Postgres IS the durable queue here: we ACK (200) only after the state
    transition + outbox append have COMMITTED, so the provider stops retrying
    exactly when the change is crash-proof.  Never ACK-before-process.
    """
    with tracer.start_as_current_span("webhook.payment.process", attributes={"payment.event_id": payload.event_id}):
        duplicate = process_payment_event(db, payload)
        return {"accepted": True, "duplicate": duplicate, "queued": False}


# Distributed tracing: wrap the ASGI app once, at import time.
FastAPIInstrumentor.instrument_app(app, excluded_urls="health,metrics")
