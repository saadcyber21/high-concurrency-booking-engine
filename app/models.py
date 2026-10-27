from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(UTC)


class Event(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Seat(Base):
    __tablename__ = "seats"
    __table_args__ = (UniqueConstraint("event_id", "number", name="uq_event_seat_number"), Index("ix_seat_event_status_expiry", "event_id", "status", "hold_expires_at"))

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_id: Mapped[str] = mapped_column(ForeignKey("events.id"), nullable=False)
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(12), default="available", nullable=False)
    hold_id: Mapped[str | None] = mapped_column(String(36), nullable=True, unique=True)
    hold_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    booked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class WebhookEvent(Base):
    """Inbound dedupe ledger — a provider event id may be processed exactly once."""

    __tablename__ = "webhook_events"
    __table_args__ = (Index("ix_webhook_event_created", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider_event_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    hold_id: Mapped[str] = mapped_column(String(36), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class OutboxMessage(Base):
    """Transactional Outbox (CQRS-lite write side).

    Rows are inserted *inside the same ACID transaction* that mutates seat
    state, so queue writes can never diverge from DB state (dual-write
    problem eliminated).  The dispatcher claims rows with
    ``SELECT ... FOR UPDATE SKIP LOCKED`` which lets N workers drain the
    table concurrently without ever double-claiming a message.

    Statuses: pending -> in_flight -> published | dead
    """

    __tablename__ = "outbox_messages"
    # Partial-index friendly composite: dispatcher scans (status, next_attempt_at).
    __table_args__ = (
        Index("ix_outbox_claim", "status", "next_attempt_at", "created_at"),
        Index("ix_outbox_aggregate", "aggregate_type", "aggregate_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(40), nullable=False)   # "seat_hold"
    aggregate_id: Mapped[str] = mapped_column(String(36), nullable=False)     # hold_id
    event_type: Mapped[str] = mapped_column(String(80), nullable=False)       # "hold.created" ...
    payload: Mapped[str] = mapped_column(JSONB().with_variant(Text, "sqlite"), nullable=False)  # canonical JSON
    status: Mapped[str] = mapped_column(String(12), nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    claimed_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    traceparent: Mapped[str | None] = mapped_column(String(64), nullable=True)  # W3C context propagated to consumer
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class IdempotencyKey(Base):
    """Server-enforced idempotency for mutating endpoints.

    Client-supplied ``Idempotency-Key`` headers are scoped per principal +
    route + method.  A UNIQUE constraint makes concurrent retries collide at
    the database level instead of executing the side-effect twice.
    """

    __tablename__ = "idempotency_keys"
    __table_args__ = (UniqueConstraint("scope", "key", name="uq_idempotency_scope_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(String(255), nullable=False)  # "{principal}:{method}:{path}"
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)  # sha256 of normalized body
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
