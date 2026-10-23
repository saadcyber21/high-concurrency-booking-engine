import json
import os


def enqueue_webhook(payload: dict) -> bool:
    url = os.getenv("REDIS_URL")
    if not url:
        return False
    try:
        from redis import Redis

        Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5).rpush(os.getenv("WEBHOOK_QUEUE", "payment-webhooks"), json.dumps(payload))
        return True
    except (OSError, TimeoutError, ConnectionError):
        return False
