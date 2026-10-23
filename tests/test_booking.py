from fastapi.testclient import TestClient

from app.database import Base, engine
from app.main import app

client = TestClient(app)


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def make_event(count=2):
    response = client.post("/v1/events", json={"name": "Concert", "seat_count": count})
    assert response.status_code == 201
    return response.json()["id"]


def test_event_and_hold_conflict():
    event_id = make_event()
    first = client.post(f"/v1/events/{event_id}/seats/1/hold", headers={"Idempotency-Key": "hold-1"})
    second = client.post(f"/v1/events/{event_id}/seats/1/hold", headers={"Idempotency-Key": "hold-2"})
    assert first.status_code == 200
    assert second.status_code == 409


def test_payment_webhook_is_idempotent_and_books_seat():
    event_id = make_event(1)
    hold = client.post(f"/v1/events/{event_id}/seats/1/hold", headers={"Idempotency-Key": "hold-one"})
    payload = {"event_id": "payment-event-1", "hold_id": hold.json()["hold_id"], "status": "succeeded"}
    first = client.post("/v1/webhooks/payment", headers={"X-Process-Inline": "true"}, json=payload)
    duplicate = client.post("/v1/webhooks/payment", headers={"X-Process-Inline": "true"}, json=payload)
    assert first.status_code == 200 and not first.json()["duplicate"]
    assert duplicate.status_code == 200 and duplicate.json()["duplicate"]
    assert client.get(f"/v1/events/{event_id}/seats").json()[0]["status"] == "booked"


def test_failed_payment_releases_hold():
    event_id = make_event(1)
    hold = client.post(f"/v1/events/{event_id}/seats/1/hold", headers={"Idempotency-Key": "hold-one"})
    response = client.post("/v1/webhooks/payment", headers={"X-Process-Inline": "true"}, json={"event_id": "payment-fail", "hold_id": hold.json()["hold_id"], "status": "failed"})
    assert response.status_code == 200
    assert client.get(f"/v1/events/{event_id}/seats").json()[0]["status"] == "available"


def test_unknown_seat_error_contract():
    response = client.post("/v1/events/missing/seats/4/hold", headers={"Idempotency-Key": "x"})
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "seat_not_found"
