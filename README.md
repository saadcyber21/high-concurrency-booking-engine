# High-Concurrency Booking Engine

A seat booking API starter. PostgreSQL row-level locks protect seat holds; Redis can queue payment webhooks. SQLite is for local tests only and does not provide row-level locks.

## Features
- Expiring seat holds and indexed inventory.
- Idempotent payment webhook processing.
- Optional Redis webhook queue and worker.
- Concurrent load-test script.
- FastAPI OpenAPI docs.

## Run
Requires Python 3.11+.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
uvicorn app.main:app --reload
```

Docs: http://127.0.0.1:8000/docs

## Load test

```bash
python scripts/load_test.py --base-url http://127.0.0.1:8000 --event-id YOUR_EVENT_UUID --seat 1 --users 100
```

Set DATABASE_URL to PostgreSQL for row-level locking. Set REDIS_URL to enable the optional queue; run `python -m app.worker` to drain it.

No real payment provider or webhook signature checks are included. Add authentication, durable queue/outbox behavior, and security review before production use.
