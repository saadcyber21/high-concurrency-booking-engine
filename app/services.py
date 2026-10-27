"""Application services: server-enforced idempotency for mutating endpoints.

Idempotency-Key semantics (Stripe-style, but enforced in Postgres):
  * First request with a key inserts a 'started' row inside its own tx and
    proceeds; the handler later marks it completed via record_idempotent_result.
  * A concurrent replay of the SAME key hits the UNIQUE constraint -> 409
    "idempotency_in_progress" (client must retry after a jitter).
  * A replay with the same key but DIFFERENT body fingerprint -> 422
    (key reuse abuse — never silently process a second payload).
Key scope = principal + method + path, so keys can't collide across tenants.
"""
from __future__ import annotations

import hashlib
import json

from fastapi import Header, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import IdempotencyKey


def _fingerprint(request_body: bytes) -> str:
    return hashlib.sha256(request_body or b"").hexdigest()


async def verify_inbound_idempotency(
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", min_length=1, max_length=255),
) -> None:
    """FastAPI dependency guarding hold/create endpoints.

    Note: we reserve the key BEFORE the handler runs.  The reservation is
    committed immediately — a crash between reserve-and-complete leaves a
    'started' row which ops tooling can expire (older than N minutes =>
    safe to reclaim because the side-effect transaction never completed).
    """
    if idempotency_key is None:
        raise HTTPException(
            status_code=400,
            detail={"code": "idempotency_key_required", "message": "Mutating requests must supply an Idempotency-Key header"},
        )

    body = await request.body()
    principal = request.headers.get("x-api-key", "anon")[:64]
    scope = f"{principal}:{request.method}:{request.url.path}"
    fingerprint = _fingerprint(body)

    db: Session = next(get_db())
    try:
        existing = db.scalar(select(IdempotencyKey).where(IdempotencyKey.scope == scope, IdempotencyKey.key == idempotency_key))
        if existing is not None:
            if existing.request_fingerprint != fingerprint:
                raise HTTPException(
                    status_code=422,
                    detail={"code": "idempotency_key_reuse_conflict", "message": "Idempotency-Key reused with a different request body"},
                )
            if existing.completed:
                # Replay AFTER completion: return the STORED canonical outcome
                # verbatim — the side effect is never re-executed. This is the
                # whole point of server-enforced idempotency under flaky
                # mobile clients that retry on timeouts.
                from fastapi.responses import JSONResponse

                content = json.loads(existing.response_body) if existing.response_body else {}
                raise _ReplayShortCircuit(
                    JSONResponse(status_code=existing.response_status or 200, content=content)
                )
            raise HTTPException(
                status_code=409,
                detail={"code": "idempotency_in_progress", "message": "A request with this Idempotency-Key is still processing"},
                headers={"Retry-After": "2"},
            )

        row = IdempotencyKey(scope=scope, key=idempotency_key, request_fingerprint=fingerprint, completed=False)
        db.add(row)
        db.commit()
        request.state.idempotent_replay = False
        request.state.idempotent_row_id = row.id
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={"code": "idempotency_in_progress", "message": "Concurrent replay of the same Idempotency-Key"},
            headers={"Retry-After": "2"},
        ) from None
    finally:
        db.close()


def record_idempotent_result(db: Session, request: Request, status_code: int, response_body: dict) -> None:
    """Called by handlers on completion to persist the canonical outcome."""
    row_id = getattr(request.state, "idempotent_row_id", None)
    if row_id is None:
        return
    row = db.get(IdempotencyKey, row_id)
    if row is not None:
        row.response_status = status_code
        row.response_body = json.dumps(response_body, sort_keys=True)
        row.completed = True
        db.commit()


class _ReplayShortCircuit(Exception):
    """Internal control-flow: carries the cached replay response.

    Starlette does not allow raising a Response from a dependency, so we wrap
    it in this exception and translate via an app-level exception handler
    (registered in app/main.py).
    """

    def __init__(self, response) -> None:  # noqa: ANN001
        super().__init__("idempotent replay short-circuit")
        self.response = response
