import json
import os

from app.database import SessionLocal
from app.main import process_webhook
from app.schemas import WebhookIn


def drain_once() -> bool:
    url = os.getenv("REDIS_URL")
    if not url:
        raise RuntimeError("REDIS_URL is required to drain the webhook queue")
    from redis import Redis

    queue_name = os.getenv("WEBHOOK_QUEUE", "payment-webhooks")
    client = Redis.from_url(url)
    item = client.lpop(queue_name)
    if item is None:
        return False
    with SessionLocal() as db:
        process_webhook(WebhookIn.model_validate(json.loads(item)), db)
    return True


if __name__ == "__main__":
    while drain_once():
        pass
