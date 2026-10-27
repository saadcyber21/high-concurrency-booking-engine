"""Redis-backed sliding-window rate limiter as pure ASGI middleware.

Why sliding window (not fixed window)?  Fixed windows permit 2x bursts at
boundary instants (N requests at 12:00:59 + N at 12:01:00).  A ZSET-per-key
sliding log scores each request by epoch-ms; we ZREMRANGEBYSCORE away entries
older than the window, then count what remains — mathematically exact.

Atomicity: the whole check runs as one Lua script (EVALSHA) so concurrent
requests cannot interleave read-then-write and smuggle a burst past the cap.

Fail-closed policy: if Redis is unreachable the limiter *denies* scoped
endpoints (503).  For a booking endpoint under volumetric attack, failing
open converts an availability incident into inventory corruption.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid

from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.config import settings

# KEYS[1] = bucket zset            ARGV[1]=now_ms  ARGV[2]=window_ms
# ARGV[3]=limit  ARGV[4]=member unique id         ARGV[5]=ttl_ms (window + slack)
SLIDING_WINDOW_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1] - ARGV[2])
local current = redis.call('ZCARD', KEYS[1])
if current + 1 > tonumber(ARGV[3]) then
  local earliest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  local retry_after = 1
  if earliest[2] then
    retry_after = math.ceil((tonumber(earliest[2]) + tonumber(ARGV[2]) - tonumber(ARGV[1])) / 1000.0)
  end
  return {0, retry_after, current}
end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return {1, 0, current + 1}
"""


class SlidingWindowRateLimiter:
    def __init__(self, redis: Redis, limit_per_minute: int, burst: int) -> None:
        self._redis = redis
        self._script = redis.register_script(SLIDING_WINDOW_LUA)
        self._limit = max(1, limit_per_minute)
        self._burst = max(1, burst)

    async def hit(self, bucket: str) -> tuple[bool, int, int]:
        """Returns (allowed, remaining, retry_after_seconds)."""
        now_ms = int(time.time() * 1000)
        window_ms = 60_000
        # Effective cap: sustained per-minute rate, with a small extra burst
        # allowance folded into the same window (token-bucket-like headroom).
        cap = self._limit + self._burst
        result = await self._script(
            keys=[f"rl:{bucket}"],
            args=[now_ms, window_ms, cap, uuid.uuid4().hex, window_ms * 2],
        )
        allowed = int(result[0]) == 1
        return allowed, int(result[2]), int(result[1])


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Applies only to scoped paths; identity = API key hash else client IP."""

    def __init__(self, app, redis: Redis | None = None) -> None:  # noqa: ANN001
        super().__init__(app)
        self._redis = redis
        self._limiter: SlidingWindowRateLimiter | None = None

    def _scoped(self, path: str) -> bool:
        return any(path.startswith(p) for p in settings.rate_limit_scope_paths)

    @staticmethod
    def _identity(request: Request) -> str:
        # Prefer a hashed API key (per-tenant fairness); fall back to the
        # closest trustworthy client IP from the edge proxy chain.
        api_key = request.headers.get("x-api-key")
        if api_key:
            return "key:" + hashlib.sha256(api_key.encode()).hexdigest()[:24]
        forwarded = request.headers.get("x-forwarded-for", "")
        ip = forwarded.split(",")[0].strip() or (request.client.host if request.client else "unknown")
        return "ip:" + ip

    async def dispatch(self, request: Request, call_next) -> Response:  # noqa: ANN001
        if not settings.rate_limit_enabled or request.method == "OPTIONS" or not self._scoped(request.url.path):
            return await call_next(request)

        if self._limiter is None:
            self._redis = self._redis or Redis.from_url(settings.redis_url or "redis://localhost:6379", decode_responses=False)
            self._limiter = SlidingWindowRateLimiter(self._redis, settings.rate_limit_per_minute, settings.rate_limit_burst)

        bucket = f"{self._identity(request)}:{request.url.path.split('/')[1] if '/' in request.url.path else 'root'}"
        try:
            allowed, remaining, retry_after = await self._limiter.hit(bucket)
        except (RedisError, OSError):
            # FAIL CLOSED — see module docstring.
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "rate_limiter_unavailable", "message": "Dependency unavailable, request refused"}},
                headers={"Retry-After": "1"},
            )

        if not allowed:
            return JSONResponse(
                status_code=429,
                content={"error": {"code": "rate_limited", "message": f"Rate limit exceeded. Retry in {retry_after}s."}},
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(settings.rate_limit_per_minute + settings.rate_limit_burst),
                    "X-RateLimit-Remaining": "0",
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_per_minute + settings.rate_limit_burst)
        response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
        return response


# Small helper used by tests / ops tooling to produce canonical cache-safe JSON.
def canonical_json(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))
