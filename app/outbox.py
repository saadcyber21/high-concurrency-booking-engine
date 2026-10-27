"""Transactional Outbox — producer side + relay dispatcher.

THE PROBLEM WITH THE OLD DESIGN
------------------------------
The previous flow was::

    db.commit()            # seat state changes...
    redis.rpush(...)       # ...then "best effort" enqueue   <-- DUAL WRITE

If the process dies, or Redis blips between those two statements, the DB says
"booked" but the downstream consumer never learns.  No rollback, no retry,
silent divergence.  Redis AOF everysec also admits ~1s of loss on crash.

THE OUTBOX FIX
--------------
``add_outbox_message`` inserts the event row in the *same session/transaction*
as the business mutation.  Postgres commits both atomically or neither.  A
separate relay (``app.outbox.dispatch_once`` / worker loop) moves rows to
Redis with at-least-once semantics; consumers dedupe on ``message_id``
(delivered exactly-once *effectively*).

CLAIMING WITH SKIP LOCKED
-------------------------
The relay claims a batch via::

    SELECT ... WHERE status='pending' AND next_attempt_at <= now()
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT :batch

``FOR UPDATE`` serialises claimers; ``SKIP LOCKED`` means a second relay does
NOT block waiting for the first — it immediately sees the *next* unlocked rows.
Result: N relays drain one table in parallel with zero contention and zero
double-processing — the same primitive that makes queue-on-Postgres viable at
ticket-drop traffic (see app/repository.py for the booking-side analogue).
"""
from __future__ import annotations

import json
import logging
import random
from datetime import timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.config import settings
from app.models import OutboxMessage, utc_now
from app.observability import get_tracer, propagate_traceparent

logger = logging.getLogger("app.outbox")
tracer = get_tracer("app.outbox")


# --------------------------------------------------------------------------
# Producer side — called INSIDE the caller's open transaction.
# --------------------------------------------------------------------------
def add_outbox_message(
    db: Session,
    *,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    payload: dict[str, Any],
) -> OutboxMessage:
    """Append an event to the outbox within the caller's transaction.

    The W3C traceparent is captured NOW so the async relay can rehydrate the
    originating request's trace — you keep one end-to-end span graph across
    the sync/async boundary.
    """
    message = OutboxMessage(
        id=str(uuid4()),
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=json.dumps(payload, sort_keys=True, separators=(",", ":")),
        status="pending",
        max_attempts=settings.outbox_max_attempts,
        next_attempt_at=utc_now(),
        traceparent=propagate_traceparent(),
    )
    db.add(message)  # NOT committed here — caller commits atomically with its own writes.
    return message


# --------------------------------------------------------------------------
# Relay side — runs in the worker process.
# --------------------------------------------------------------------------
def claim_pending_batch(db: Session, *, worker_id: str, batch_size: int) -> list[OutboxMessage]:
    """Atomically claim up to ``batch_size`` due messages (SKIP LOCKED).

    SQLite (local dev/tests) ignores FOR UPDATE — harmless there since only
    one process writes; on Postgres this is the concurrency-safe primitive.
    """
    now = utc_now()
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.status == "pending", OutboxMessage.next_attempt_at <= now)
        .order_by(OutboxMessage.created_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)   # -> FOR UPDATE SKIP LOCKED
    )
    messages = list(db.scalars(stmt))
    if not messages:
        return []
    for message in messages:
        message.status = "in_flight"
        message.claimed_at = now
        message.claimed_by = worker_id
    db.commit()  # release row locks; rows are durably marked in_flight
    return messages


def _backoff_seconds(attempts: int) -> float:
    """Exponential backoff with full jitter (AWS-style) to avoid thundering herd."""
    cap = min(300.0, settings.outbox_base_backoff_seconds * (2 ** attempts))
    return random.uniform(0, cap)


def publish_to_redis(redis_client, messages: list[OutboxMessage]) -> tuple[int, int]:  # noqa: ANN001
    """Push claimed messages onto Redis Streams (XADD) and mark published.

    Redis Streams (not LPUSH lists) give us: server-assigned IDs, per-consumer
    groups, and XACK/XAUTOCLAIM-based redelivery for crashed consumers.
    """
    stream = stream_name()
    published = dead = 0
    for message in messages:
        with tracer.start_as_current_span("outbox.publish", attributes={"outbox.message_id": message.id, "outbox.event_type": message.event_type}):
            try:
                redis_client.xadd(
                    stream,
                    {
                        "message_id": message.id,
                        "event_type": message.event_type,
                        "aggregate_type": message.aggregate_type,
                        "aggregate_id": message.aggregate_id,
                        "payload": message.payload,
                        "traceparent": message.traceparent or "",
                    },
                    maxlen=100_000,          # approximate capping keeps memory bounded
                    approximate=True,
                )
            except Exception as exc:  # noqa: BLE001 — relay must survive any broker fault
                was_dead = _mark_retry_or_dead(message, error=str(exc))
                dead += 1 if was_dead else 0
                continue
            message.status = "published"
            message.published_at = utc_now()
            message.last_error = None
            published += 1
    return published, dead


def stream_name() -> str:
    """Redis Stream carrying payment/booking events (consumer group target)."""
    from os import getenv

    return getenv("WEBHOOK_STREAM", "payment-webhook-events")


def _mark_retry_or_dead(message: OutboxMessage, *, error: str) -> bool:
    """Returns True when the message transitioned to the terminal 'dead' state."""
    message.attempts += 1
    message.last_error = error[:2000]
    if message.attempts >= message.max_attempts:
        message.status = "dead"   # parked for operator triage; visible via /metrics & Grafana
        logger.error("outbox message %s dead after %d attempts: %s", message.id, message.attempts, error)
        return True
    message.status = "pending"
    message.next_attempt_at = utc_now() + timedelta(seconds=_backoff_seconds(message.attempts))
    return False


def reap_stale_claims(db: Session, timeout_seconds: int = 300) -> int:
    """Crash recovery: a relay that died mid-flight leaves ``in_flight`` rows.

    Single UPDATE returns them to ``pending`` so another relay re-claims them.
    This is what turns "at-least-once" from a claim into a guarantee.
    """
    cutoff = utc_now() - timedelta(seconds=timeout_seconds)
    result = db.execute(
        update(OutboxMessage)
        .where(OutboxMessage.status == "in_flight", OutboxMessage.claimed_at <= cutoff)
        .values(status="pending", claimed_at=None, claimed_by=None)
    )
    db.commit()
    return int(result.rowcount or 0)


def dispatch_once(db: Session, redis_client, *, worker_id: str = "relay-1") -> tuple[int, int, int]:  # noqa: ANN001
    """One relay iteration.  Returns (claimed, published, newly_dead)."""
    with tracer.start_as_current_span("outbox.relay_cycle"):
        stale = reap_stale_claims(db)
        if stale:
            logger.warning("reaped %d stale in_flight outbox rows", stale)
        batch = claim_pending_batch(db, worker_id=worker_id, batch_size=settings.outbox_batch_size)
        if not batch:
            return 0, 0, 0
        published, dead = publish_to_redis(redis_client, batch)
        db.commit()
        return len(batch), published, dead
