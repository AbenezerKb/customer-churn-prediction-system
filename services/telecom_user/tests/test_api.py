from fastapi.testclient import TestClient

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.main import app


def test_root():
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "Telecom API"}


def save_call_session():
    payload = {
  "customer_id": 1,
  "period_date": "2025-10-26",
  "dropped_calls": 0,
  "blocked_calls": 0,
  "unanswered_calls": 0,
  "customer_care_calls": 0,
  "three_way_calls": 0,
  "received_calls": 0,
  "outbound_calls": 0,
  "inbound_calls": 0,
  "peak_calls_in_out": 0,
  "off_peak_calls_in_out": 0,
  "dropped_blocked_calls": 0,
  "call_forwarding_calls": 0,
  "call_waiting_calls": 0,
  "director_assisted_calls": 0
}
    with TestClient(app) as client:
        response = client.post("/call-sessions",content=payload)
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}