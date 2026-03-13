"""
Tests for the webhook archival flow:
  - backend/ingest_webhook_payload.py  (archive module)
  - POST /webhooks/tradingview/archive  (API endpoint)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import event_writer, ingest_webhook_payload
from backend.api_server import app

SIGNAL_KEY = "test-archive-key"
AUTH_HEADERS = {"X-SIGNAL-KEY": SIGNAL_KEY}


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """FastAPI test client with isolated DB and ingest dir."""
    test_db_path = tmp_path / "test_archive.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
    ingest_dir = tmp_path / "webhook_ingest"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", SIGNAL_KEY)
    monkeypatch.setenv("TRADINGVIEW_INGEST_SIGNAL_KEY", SIGNAL_KEY)
    monkeypatch.setenv("SIGNAL_CLAIM_TIMEOUT_SECONDS", "60")
    monkeypatch.setenv("WEBHOOK_INGEST_DIR", str(ingest_dir))

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def ingest_dir(tmp_path, monkeypatch):
    """Isolated ingest directory for unit tests."""
    d = tmp_path / "webhook_ingest"
    monkeypatch.setenv("WEBHOOK_INGEST_DIR", str(d))
    return d


# ---------------------------------------------------------------------------
# Unit tests: ingest_webhook_payload module
# ---------------------------------------------------------------------------


def _sample_payload() -> dict:
    return {
        "strategy_id": "bridge_signal_sender_v2",
        "release_version": "2.1.0",
        "batch_id": "batch-test-001",
        "source": "tradingview",
        "symbol": "BTCUSDT",
        "side": "buy",
    }


class TestArchivePayload:
    def test_creates_file_in_ingest_dir(self, ingest_dir):
        payload = _sample_payload()
        written = ingest_webhook_payload.archive_payload(payload)

        assert written.exists()
        assert written.parent == ingest_dir

    def test_filename_contains_strategy_and_release(self, ingest_dir):
        payload = _sample_payload()
        written = ingest_webhook_payload.archive_payload(payload)

        assert "bridge_signal_sender_v2" in written.name
        assert "2.1.0" in written.name

    def test_file_content_matches_payload(self, ingest_dir):
        payload = _sample_payload()
        written = ingest_webhook_payload.archive_payload(payload)

        stored = json.loads(written.read_text(encoding="utf-8"))
        assert stored == payload

    def test_missing_strategy_id_uses_unknown(self, ingest_dir):
        payload = {"release_version": "1.0.0", "batch_id": "b1"}
        written = ingest_webhook_payload.archive_payload(payload)
        assert "unknown_strategy" in written.name

    def test_missing_release_version_uses_unknown(self, ingest_dir):
        payload = {"strategy_id": "my_strat", "batch_id": "b2"}
        written = ingest_webhook_payload.archive_payload(payload)
        assert "unknown_release" in written.name

    def test_special_characters_sanitised_in_filename(self, ingest_dir):
        payload = {
            "strategy_id": "strat/v1:beta",
            "release_version": "1.0 (rc)",
        }
        written = ingest_webhook_payload.archive_payload(payload)
        # No path separators or colons in the filename
        assert "/" not in written.name
        assert ":" not in written.name

    def test_creates_directory_if_missing(self, tmp_path, monkeypatch):
        deep_dir = tmp_path / "a" / "b" / "c"
        monkeypatch.setenv("WEBHOOK_INGEST_DIR", str(deep_dir))
        payload = _sample_payload()
        written = ingest_webhook_payload.archive_payload(payload)
        assert written.exists()

    def test_no_tmp_file_left_behind(self, ingest_dir):
        payload = _sample_payload()
        ingest_webhook_payload.archive_payload(payload)
        tmp_files = list(ingest_dir.glob("*.tmp"))
        assert tmp_files == []


class TestIngestFunction:
    def test_returns_summary_dict(self, ingest_dir):
        result = ingest_webhook_payload.ingest(json.dumps(_sample_payload()))
        assert result["status"] == "archived"
        assert result["strategy_id"] == "bridge_signal_sender_v2"
        assert result["release_version"] == "2.1.0"
        assert result["batch_id"] == "batch-test-001"
        assert "written_to" in result

    def test_raises_on_invalid_json(self, ingest_dir):
        with pytest.raises(ValueError, match="Invalid JSON"):
            ingest_webhook_payload.ingest("{not valid json}")

    def test_raises_on_json_array(self, ingest_dir):
        with pytest.raises(ValueError, match="JSON object"):
            ingest_webhook_payload.ingest("[1, 2, 3]")


# ---------------------------------------------------------------------------
# Integration tests: POST /webhooks/tradingview/archive endpoint
# ---------------------------------------------------------------------------


class TestArchiveEndpoint:
    def test_archive_returns_201_with_valid_payload(self, client):
        payload = _sample_payload()
        response = client.post(
            "/webhooks/tradingview/archive",
            json=payload,
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 201
        body = response.json()
        assert body["status"] == "archived"
        assert body["strategy_id"] == "bridge_signal_sender_v2"
        assert body["release_version"] == "2.1.0"
        assert "written_to" in body

    def test_archive_writes_file_to_ingest_dir(self, client, tmp_path, monkeypatch):
        ingest_dir = tmp_path / "webhook_ingest"
        monkeypatch.setenv("WEBHOOK_INGEST_DIR", str(ingest_dir))

        payload = _sample_payload()
        response = client.post(
            "/webhooks/tradingview/archive",
            json=payload,
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 201

        files = list(ingest_dir.glob("*.json"))
        assert len(files) == 1
        stored = json.loads(files[0].read_text())
        assert stored["strategy_id"] == "bridge_signal_sender_v2"

    def test_archive_rejects_missing_auth(self, client):
        response = client.post("/webhooks/tradingview/archive", json=_sample_payload())
        assert response.status_code == 401

    def test_archive_rejects_wrong_key(self, client):
        response = client.post(
            "/webhooks/tradingview/archive",
            json=_sample_payload(),
            headers={"X-SIGNAL-KEY": "wrong-key"},
        )
        assert response.status_code == 401

    def test_archive_accepts_signal_key_query_param(self, client):
        response = client.post(
            f"/webhooks/tradingview/archive?signal_key={SIGNAL_KEY}",
            json=_sample_payload(),
        )
        assert response.status_code == 201

    def test_archive_returns_422_on_non_object_json(self, client):
        response = client.post(
            "/webhooks/tradingview/archive",
            content=b"[1, 2, 3]",
            headers={**AUTH_HEADERS, "Content-Type": "application/json"},
        )
        assert response.status_code == 422

    def test_archive_does_not_run_downstream_pipeline(self, client):
        """The archive endpoint must not touch normalized_signals or the
        strategy/risk/execution pipeline tables."""
        from backend.event_writer import get_connection

        payload = _sample_payload()
        response = client.post(
            "/webhooks/tradingview/archive",
            json=payload,
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 201

        with get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM normalized_signals").fetchone()[0]
        assert count == 0, "Archive endpoint must not insert into normalized_signals"

    def test_archive_minimal_payload(self, client):
        """A minimal payload with only batch_id should still be accepted."""
        response = client.post(
            "/webhooks/tradingview/archive",
            json={"batch_id": "minimal-001"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 201
        body = response.json()
        assert body["status"] == "archived"
        # Missing fields come back as None
        assert body["strategy_id"] is None
        assert body["release_version"] is None
        assert body["batch_id"] == "minimal-001"
