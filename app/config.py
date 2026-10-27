"""Centralised, typed runtime configuration (12-factor).

Every knob the architecture exposes is declared here exactly once so that
main.py / worker.py / middleware never read ``os.getenv`` ad-hoc.  This is the
single source of truth referenced by docker-compose and .env.example.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    # --- storage -----------------------------------------------------------
    database_url: str = field(default_factory=lambda: os.getenv("DATABASE_URL", "sqlite:///./booking.db"))
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", ""))

    # --- booking domain ----------------------------------------------------
    hold_ttl_seconds: int = field(default_factory=lambda: _int("HOLD_TTL_SECONDS", 300))

    # --- outbox dispatcher -------------------------------------------------
    outbox_batch_size: int = field(default_factory=lambda: _int("OUTBOX_BATCH_SIZE", 50))
    outbox_poll_seconds: float = field(default_factory=lambda: _float("OUTBOX_POLL_SECONDS", 1.0))
    outbox_max_attempts: int = field(default_factory=lambda: _int("OUTBOX_MAX_ATTEMPTS", 10))
    outbox_base_backoff_seconds: float = field(default_factory=lambda: _float("OUTBOX_BASE_BACKOFF_SECONDS", 1.0))

    # --- zero-trust webhook security ----------------------------------------
    webhook_secrets: str = field(default_factory=lambda: os.getenv("WEBHOOK_SECRETS", ""))  # comma-separated key_id:secret
    webhook_tolerance_seconds: int = field(default_factory=lambda: _int("WEBHOOK_TOLERANCE_SECONDS", 300))

    # --- rate limiting (sliding window, Redis-backed) -----------------------
    rate_limit_enabled: bool = field(default_factory=lambda: os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true")
    rate_limit_per_minute: int = field(default_factory=lambda: _int("RATE_LIMIT_PER_MINUTE", 120))
    rate_limit_burst: int = field(default_factory=lambda: _int("RATE_LIMIT_BURST", 40))
    rate_limit_scope_paths: tuple[str, ...] = ("/v1/events", "/v1/webhooks")

    # --- observability -------------------------------------------------------
    otel_service_name: str = field(default_factory=lambda: os.getenv("OTEL_SERVICE_NAME", "booking-engine-api"))
    otlp_endpoint: str = field(default_factory=lambda: os.getenv("OTLP_ENDPOINT", ""))  # e.g. http://otel-collector:4318
    metrics_enabled: bool = field(default_factory=lambda: os.getenv("METRICS_ENABLED", "true").lower() == "true")

    @property
    def webhook_secret_map(self) -> dict[str, str]:
        """Parse ``key_id:secret`` pairs; supports secret rotation windows."""
        out: dict[str, str] = {}
        for chunk in self.webhook_secrets.split(","):
            chunk = chunk.strip()
            if not chunk or ":" not in chunk:
                continue
            key_id, secret = chunk.split(":", 1)
            out[key_id.strip()] = secret.strip()
        return out


settings = Settings()
