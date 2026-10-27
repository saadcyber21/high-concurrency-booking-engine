"""Outbox relay + hold-expiry sweeper daemon.

One process runs two cooperating loops:

  RELAY   claims pending outbox rows (FOR UPDATE SKIP LOCKED) and XADDs them
          onto Redis Streams -> at-least-once delivery to consumers.
  SWEEPER releases expired seat holds (also SKIP LOCKED) so N workers never
          collide, emitting hold.expired events through the same outbox.

Scale-out story: run `replicas: N` of this container.  SKIP LOCKED makes the
workers mutually invisible — no external coordination, no leader election,
no double-claims.  A crashed worker's in_flight rows are reaped after the
claim timeout, which is what actually delivers the at-least-once promise.
"""
from __future__ import annotations

import logging
import os
import signal
import time

from redis import Redis
from redis.exceptions import RedisError

from app.config import settings
from app.database import SessionLocal
from sqlalchemy import func, select as sa_select

from app.metrics import OUTBOX_DEAD, OUTBOX_PENDING, OUTBOX_PUBLISHED
from app.models import OutboxMessage
from app.outbox import dispatch_once, stream_name
from app.repository import sweep_expired_holds

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("app.worker")

_running = True


def _stop(signum, frame):  # noqa: ANN001
    global _running
    logger.info("signal %s received — draining", signum)
    _running = False


def build_redis() -> Redis:
    if not settings.redis_url:
        raise RuntimeError("REDIS_URL is required for the outbox relay")
    return Redis.from_url(settings.redis_url, socket_connect_timeout=2, socket_timeout=2)


def relay_loop(redis_client: Redis, worker_id: str) -> None:
    while _running:
        claimed = published = newly_dead = 0
        with SessionLocal() as db:
            try:
                claimed, published, newly_dead = dispatch_once(db, redis_client, worker_id=worker_id)
            except (RedisError, OSError) as exc:
                # Broker down? Rows stay 'pending' in Postgres — NOTHING is
                # lost; we simply retry after backoff.  That is the entire
                # point of making Postgres (not Redis) the source of truth.
                logger.error("relay cycle failed (will retry): %s", exc)
            pending = db.scalar(sa_select(func.count()).select_from(OutboxMessage).where(OutboxMessage.status == "pending")) or 0
        OUTBOX_PENDING.set(pending)
        if published:
            OUTBOX_PUBLISHED.inc(published)
        if newly_dead:
            OUTBOX_DEAD.inc(newly_dead)
        if claimed == 0:
            time.sleep(settings.outbox_poll_seconds)


def sweeper_loop(interval: float = 5.0) -> None:
    while _running:
        with SessionLocal() as db:
            released = sweep_expired_holds(db)
        if released:
            logger.info("released %d expired holds", released)
        time.sleep(interval)


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    worker_id = os.getenv("WORKER_ID", f"relay-{os.getpid()}")
    redis_client = build_redis()

    import threading

    threads = [
        threading.Thread(target=relay_loop, args=(redis_client, worker_id), name="outbox-relay"),
        threading.Thread(target=sweeper_loop, name="hold-sweeper"),
    ]
    for t in threads:
        t.start()
    logger.info("worker %s online (stream=%s)", worker_id, stream_name())
    while any(t.is_alive() for t in threads):
        time.sleep(0.5)


if __name__ == "__main__":
    main()
