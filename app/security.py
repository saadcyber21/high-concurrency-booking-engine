"""Webhook signature verification — Stripe-compatible ``Stripe-Signature`` scheme.

Header format:  ``t=<unix_epoch>,v1=<hex-hmac>,v1=<hex-hmac>``
Signed payload: ``"{t}.{raw_body}"``  (HMAC-SHA256, secret = endpoint key)

Zero-trust properties enforced here:
  1. Raw request bytes are verified BEFORE any JSON parsing/deserialization —
     re-serialising a parsed body would change whitespace/key order and break
     the MAC, so we never trust a mutated representation of the payload.
  2. Constant-time comparison (``hmac.compare_digest``) — naive ``==`` leaks
     byte-position timing to an attacker probing for forgeries.
  3. Replay defense via timestamp tolerance window.
  4. Multi-secret map keyed by ``key_id`` supports overlap-window rotation.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass

from fastapi import Header, HTTPException, Request

from app.config import settings


class SignatureError(HTTPException):
    """401 with a machine-readable error contract; never echo the secret."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(status_code=401, detail={"code": code, "message": message})


@dataclass(frozen=True)
class SignatureHeader:
    timestamp: int
    signatures: tuple[str, ...]
    key_id: str | None = None

    @classmethod
    def parse(cls, header_value: str) -> "SignatureHeader":
        """Parse ``t=...,v1=...`` (Stripe) or ``s=...,t=...,v1=...`` (rotated key id)."""
        timestamp: int | None = None
        signatures: list[str] = []
        key_id: str | None = None
        for part in header_value.split(","):
            part = part.strip()
            if not part or "=" not in part:
                continue
            k, _, v = part.partition("=")
            k = k.strip().lower()
            v = v.strip()
            if k == "t":
                try:
                    timestamp = int(v)
                except ValueError:
                    raise SignatureError("webhook_malformed_signature", "Invalid signature timestamp")
            elif k == "v1":
                signatures.append(v.lower())
            elif k == "s":
                key_id = v
        if timestamp is None or not signatures:
            raise SignatureError("webhook_malformed_signature", "Signature header missing t= or v1= element")
        return cls(timestamp=timestamp, signatures=tuple(signatures), key_id=key_id)


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    """Stripe-style signed payload: HMAC_SHA256(secret, f"{timestamp}.{body}")."""
    signed_payload = b"%d.%s" % (timestamp, body)
    return hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()


def verify_webhook_signature(
    body: bytes,
    signature_header: str,
    secrets: dict[str, str],
    *,
    tolerance_seconds: int,
    now: float | None = None,
) -> dict:
    """Verify MAC + freshness, returning the parsed body on success.

    Raises ``SignatureError`` (→ HTTP 401) on any failure.  When several
    ``v1=`` candidates are present (provider rotation), ANY valid match passes —
    but every comparison is constant-time against the derived MAC.
    """
    if not secrets:
        # Fail closed: an unconfigured verifier must never accept traffic.
        raise SignatureError("webhook_verifier_unconfigured", "No webhook signing secrets configured")

    parsed = SignatureHeader.parse(signature_header)
    current = time.time() if now is None else now

    # 1) Replay window — reject stale timestamps before doing any crypto.
    if abs(current - parsed.timestamp) > tolerance_seconds:
        raise SignatureError("webhook_timestamp_out_of_range", "Webhook timestamp outside acceptance window")

    # 2) Select candidate secret(s): key-scoped if header carries `s=`, else all active ids.
    if parsed.key_id is not None:
        candidate_secrets = {parsed.key_id: secrets[parsed.key_id]} if parsed.key_id in secrets else {}
    else:
        candidate_secrets = secrets
    if not candidate_secrets:
        raise SignatureError("webhook_unknown_key_id", "Signature key id is not recognized")

    # 3) Constant-time MAC verification over the RAW bytes.
    expected_by_secret = [compute_signature(sec, parsed.timestamp, body) for sec in candidate_secrets.values()]
    matched = any(
        hmac.compare_digest(candidate, expected)
        for candidate in parsed.signatures
        for expected in expected_by_secret
    )
    if not matched:
        raise SignatureError("webhook_signature_mismatch", "Request signature does not match any active secret")

    # 4) Only NOW is the payload trusted enough to deserialize.
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SignatureError("webhook_malformed_payload", "Signature valid but body is not JSON") from exc


async def require_valid_webhook_signature(
    request: Request,
    stripe_signature: str | None = Header(default=None, alias="Stripe-Signature"),
) -> dict:
    """FastAPI dependency: gate the webhook route behind cryptographic auth.

    Usage::

        @app.post("/v1/webhooks/payment", dependencies=[Depends(require_valid_webhook_signature)])

    The verified raw body is stashed on ``request.state`` so downstream handlers
    re-use the exact authenticated bytes instead of re-parsing.
    """
    if stripe_signature is None:
        raise SignatureError("webhook_missing_signature", "Missing Stripe-Signature header")
    body = await request.body()  # cached by Starlette; handler can read it again safely
    data = verify_webhook_signature(
        body=body,
        signature_header=stripe_signature,
        secrets=settings.webhook_secret_map,
        tolerance_seconds=settings.webhook_tolerance_seconds,
    )
    request.state.webhook_raw_body = body
    request.state.webhook_verified = True
    return data
