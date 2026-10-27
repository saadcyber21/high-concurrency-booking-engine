"""Booking repository — all SQL concurrency primitives live here.

Two distinct SKIP LOCKED use-cases in this system:

1. SEAT HOLDING (targeted pessimistic lock)
   A hold request targets ONE known seat, so it takes a plain
   ``SELECT ... FOR UPDATE`` on that row.  Crucially we set
   ``lock_timeout`` so a contended seat fails fast (409/503 + Retry-After)
   instead of stacking an unbounded queue of blocked transactions behind one
   hot row during a ticket drop.  Blocked transactions are thread-pool poison:
   each holds a DB connection AND a uvicorn worker slot.

2. HOLD EXPIRY SWEEPER (queue-style batch claim)
   Releasing expired holds is *set-oriented*: many workers sweep many rows.
   ``FOR UPDATE SKIP LOCKED`` turns the seats table itself into a work queue —
   concurrent sweeps never collide and never block each other.  This is the
   pattern that prevents worker bottlenecking under massive traffic spikes.
"""
from __future__ import annotations

import time
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from app.config import settings
from app.metrics import LOCK_WAIT
from app.models import Seat, WebhookEvent, utc_now
from app.outbox import add_outbox_message
from app.schemas import WebhookIn


class LockAcquisitionTimeout(RuntimeError):
    """Raised when the row lock could not be taken within lock_timeout."""


def _is_lock_timeout(exc: Exception) -> bool:
    orig = getattr(exc, "orig", exc)
    # psycopg3: pgcode 55P03 lock_not_available; sqlite: "database is locked"
    return getattr(orig, "pgcode", None) == "55P03" or "locked" in str(orig).lower()


def acquire_seat_for_update(db: Session, event_id: str, number: int) -> Seat | None:
    """Targeted row lock with bounded wait — the anti-bottleneck primitive.

    ``nowait=False`` + ``lock_timeout`` (set per-transaction below) gives us
    cooperative waiting with a hard ceiling, which beats both pure NOWAIT
    (spurious failures on uncontended blips) and unbounded waits (pile-ups).
    """
    started = time.perf_counter()
    outcome = "acquired"
    try:
        if db.bind and db.bind.dialect.name == "postgresql":
            # Bound the wait: 2s max on a single hot seat row.
            db.execute(text_sa("SET LOCAL lock_timeout = '2s'"))
        stmt = (
            select(Seat)
            .where(Seat.event_id == event_id, Seat.number == number)
            .with_for_update()          # SELECT ... FOR UPDATE
        )
        seat = db.scalar(stmt)
        if seat is None:
            outcome = "not_found"
        return seat
    except DBAPIError as exc:
        if _is_lock_timeout(exc):
            outcome = "timeout"
            db.rollback()
            raise LockAcquisitionTimeout(f"seat {event_id}/{number} lock contention") from exc
        raise
    finally:
        LOCK_WAIT.labels(outcome).observe(time.perf_counter() - started)


def text_sa(sql: str):  # tiny indirection keeps imports tidy
    from sqlalchemy import text

    return text(sql)


def create_hold(db: Session, seat: Seat) -> Seat:
    """Mutate the (already locked) seat into `held` + append outbox event.

    Single commit → seat row AND outbox row become durable atomically.
    """
    now = utc_now()
    seat.status = "held"
    seat.hold_id = str(uuid4())
    seat.hold_expires_at = now + timedelta(seconds=settings.hold_ttl_seconds)
    add_outbox_message(
        db,
        aggregate_type="seat_hold",
        aggregate_id=seat.hold_id,
        event_type="hold.created",
        payload={
            "hold_id": seat.hold_id,
            "seat_id": seat.id,
            "event_id": seat.event_id,
            "seat_number": seat.number,
            "expires_at": seat.hold_expires_at.isoformat(),
        },
    )
    db.commit()
    db.refresh(seat)
    return seat


def sweep_expired_holds(db: Session, *, batch_size: int = 100) -> int:
    """Release holds whose TTL lapsed — SKIP LOCKED batch claim (case #2 above).

    Safe to run from N workers simultaneously: each claims disjoint row sets.
    Every release also emits a ``hold.expired`` outbox event in the same tx so
    downstream systems (waitlists, analytics) see a complete event stream.
    """
    now = utc_now()
    claim = (
        select(Seat.id, Seat.hold_id)
        .where(
            Seat.status == "held",
            Seat.hold_expires_at <= now,
        )
        .order_by(Seat.hold_expires_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)   # <-- FOR UPDATE SKIP LOCKED
    )
    rows = list(db.execute(claim))
    for seat_id, hold_id in rows:
        db.execute(
            update(Seat)
            .where(Seat.id == seat_id)
            .values(status="available", hold_id=None, hold_expires_at=None)
        )
        add_outbox_message(
            db,
            aggregate_type="seat_hold",
            aggregate_id=hold_id or seat_id,
            event_type="hold.expired",
            payload={"seat_id": seat_id, "released_at": now.isoformat()},
        )
    db.commit()
    return len(rows)


def process_payment_event(db: Session, payload: WebhookIn) -> bool:
    """Idempotent, transactional application of a payment result webhook.

    Ordering matters: we take the seat lock FIRST, then insert the dedupe row,
    then mutate — all before one commit.  A crash anywhere loses nothing
    (nothing was applied) and duplicates are impossible after (UNIQUE
    provider_event_id collides → rollback → treated as duplicate).

    Returns True if this call was a duplicate/no-op.
    """
    if db.scalar(select(WebhookEvent).where(WebhookEvent.provider_event_id == payload.event_id)):
        return True
    try:
        seat = db.scalar(select(Seat).where(Seat.hold_id == payload.hold_id).with_for_update())
        if seat is None:
            from fastapi import HTTPException

            raise HTTPException(status_code=404, detail={"code": "hold_not_found", "message": "Seat hold not found"})
        now = utc_now()
        expires_at = seat.hold_expires_at
        if expires_at is not None and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=now.tzinfo)
        if payload.status == "succeeded":
            if seat.status != "held" or not expires_at or expires_at <= now:
                from fastapi import HTTPException

                raise HTTPException(status_code=409, detail={"code": "hold_expired", "message": "Seat hold is no longer active"})
            seat.status = "booked"
            seat.booked_at = now
            seat.hold_expires_at = None
            add_outbox_message(
                db,
                aggregate_type="seat_hold",
                aggregate_id=payload.hold_id,
                event_type="booking.confirmed",
                payload=payload.model_dump(),
            )
        elif seat.status == "held":
            seat.status = "available"
            seat.hold_id = None
            seat.hold_expires_at = None
            add_outbox_message(
                db,
                aggregate_type="seat_hold",
                aggregate_id=payload.hold_id,
                event_type="booking.cancelled",
                payload=payload.model_dump(),
            )
        db.add(WebhookEvent(provider_event_id=payload.event_id, hold_id=payload.hold_id, status=payload.status))
        db.commit()
        return False
    except DBAPIError:
        db.rollback()
        return True
