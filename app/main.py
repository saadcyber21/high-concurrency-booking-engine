import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import Base, engine, get_db
from app.models import Event, Seat, WebhookEvent, utc_now
from app.queue import enqueue_webhook
from app.schemas import EventCreate, EventOut, SeatOut, WebhookIn, WebhookOut

Base.metadata.create_all(bind=engine)
app = FastAPI(title="High-Concurrency Booking Engine", version="0.1.0", description="Seat booking with expiring holds and idempotent payment webhooks.")


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    details = [{"type": e.get("type"), "loc": e.get("loc"), "msg": e.get("msg")} for e in exc.errors()]
    return JSONResponse(status_code=422, content={"error": {"code": "validation_error", "message": "Request validation failed", "details": jsonable_encoder(details)}})


@app.exception_handler(HTTPException)
async def http_error_handler(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, dict) else {"code": "request_error", "message": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content={"error": {**detail, "details": detail.get("details", [])}})


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@app.get("/health", tags=["system"])
def health():
    return {"status": "ok"}


@app.post("/v1/events", response_model=EventOut, status_code=201, tags=["events"])
def create_event(payload: EventCreate, db: Session = Depends(get_db)):
    event = Event(id=str(uuid4()), name=payload.name.strip())
    db.add(event)
    seats = [Seat(id=str(uuid4()), event_id=event.id, number=n, status="available") for n in range(1, payload.seat_count + 1)]
    db.add_all(seats)
    db.commit()
    return {"id": event.id, "name": event.name, "seat_count": payload.seat_count}


@app.get("/v1/events/{event_id}/seats", response_model=list[SeatOut], tags=["seats"])
def list_seats(event_id: str, limit: int = 100, db: Session = Depends(get_db)):
    if db.get(Event, event_id) is None:
        raise HTTPException(status_code=404, detail={"code": "event_not_found", "message": "Event not found"})
    statement = select(Seat).where(Seat.event_id == event_id).order_by(Seat.number).limit(max(1, min(limit, 500)))
    return list(db.scalars(statement))


@app.post("/v1/events/{event_id}/seats/{number}/hold", response_model=SeatOut, tags=["seats"])
def hold_seat(event_id: str, number: int, idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=255), db: Session = Depends(get_db)):
    seat = db.scalar(select(Seat).where(Seat.event_id == event_id, Seat.number == number).with_for_update())
    if seat is None:
        raise HTTPException(status_code=404, detail={"code": "seat_not_found", "message": "Seat not found"})
    now = utc_now()
    expiry = _aware(seat.hold_expires_at)
    if seat.status == "booked":
        raise HTTPException(status_code=409, detail={"code": "seat_booked", "message": "Seat is already booked"})
    if seat.status == "held" and expiry and expiry > now:
        if seat.hold_id == idempotency_key:
            return seat
        raise HTTPException(status_code=409, detail={"code": "seat_held", "message": "Seat is currently held"})
    seat.status = "held"
    seat.hold_id = str(uuid4())
    seat.hold_expires_at = now + timedelta(seconds=max(30, int(os.getenv("HOLD_TTL_SECONDS", "300"))))
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"code": "seat_held", "message": "Seat was concurrently held"}) from exc
    db.refresh(seat)
    return seat


def process_webhook(payload: WebhookIn, db: Session) -> bool:
    if db.scalar(select(WebhookEvent).where(WebhookEvent.provider_event_id == payload.event_id)):
        return True
    try:
        seat = db.scalar(select(Seat).where(Seat.hold_id == payload.hold_id).with_for_update())
        if seat is None:
            raise HTTPException(status_code=404, detail={"code": "hold_not_found", "message": "Seat hold not found"})
        now = utc_now()
        expires_at = _aware(seat.hold_expires_at)
        if payload.status == "succeeded":
            if seat.status != "held" or not expires_at or expires_at <= now:
                raise HTTPException(status_code=409, detail={"code": "hold_expired", "message": "Seat hold is no longer active"})
            seat.status = "booked"
            seat.booked_at = now
            seat.hold_expires_at = None
        elif seat.status == "held":
            seat.status = "available"
            seat.hold_id = None
            seat.hold_expires_at = None
        db.add(WebhookEvent(provider_event_id=payload.event_id, hold_id=payload.hold_id, status=payload.status))
        db.commit()
        return False
    except IntegrityError:
        db.rollback()
        return True


@app.post("/v1/webhooks/payment", response_model=WebhookOut, tags=["webhooks"])
def payment_webhook(payload: WebhookIn, x_process_inline: str | None = Header(default=None), db: Session = Depends(get_db)):
    if x_process_inline != "true" and enqueue_webhook(payload.model_dump()):
        return {"accepted": True, "queued": True}
    return {"accepted": True, "duplicate": process_webhook(payload, db), "queued": False}
