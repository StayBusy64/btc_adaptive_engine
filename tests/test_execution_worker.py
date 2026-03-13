from pathlib import Path

import pytest

from backend import event_writer, execution_worker
from backend.execution_worker import ApiClient, ExecutionWorker, WorkerConfig


@pytest.fixture(autouse=True)
def stub_journal_write(monkeypatch):
    monkeypatch.setattr(execution_worker, "insert_execution_journal_entry", lambda **_: 1)


class FakeApiClient:
    def __init__(self, claim_sequence):
        self.claim_sequence = list(claim_sequence)
        self.claim_calls = []
        self.heartbeat_calls = []
        self.patch_calls = []

    def claim_next(self, worker_id):
        self.claim_calls.append(worker_id)
        if self.claim_sequence:
            return self.claim_sequence.pop(0)
        return {"status": "empty", "row": None}

    def send_heartbeat(self, candidate_id, worker_id, claim_token):
        payload = {
            "id": candidate_id,
            "worker_id": worker_id,
            "claim_token": claim_token,
        }
        self.heartbeat_calls.append(payload)
        return {
            "status": "ok",
            "id": candidate_id,
            "claimed_by": worker_id,
            "claim_token": claim_token,
            "claimed_at": "2026-03-10T20:00:00Z",
        }

    def patch_status(self, candidate_id, execution_status, execution_note):
        payload = {
            "candidate_id": candidate_id,
            "execution_status": execution_status,
            "execution_note": execution_note,
        }
        self.patch_calls.append(payload)
        return {
            "status": "updated",
            "id": candidate_id,
            "execution_status": execution_status,
            "execution_note": execution_note,
        }


def _base_config(*, oneshot=True, simulate_processing_seconds=0.0, heartbeat_interval_seconds=1.0):
    return WorkerConfig(
        api_base_url="http://127.0.0.1:9999",
        signal_webhook_key="test-key",
        worker_id="worker-test",
        poll_interval_seconds=0.0,
        oneshot=oneshot,
        confidence_threshold=0.75,
        simulate_processing_seconds=simulate_processing_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        http_timeout_seconds=2.0,
    )


def _claim_payload(
    *,
    candidate_id=1,
    symbol: "str | None" = "BTCUSDT.P",
    direction="long",
    timestamp="2026-03-10T20:30:00Z",
    confidence=0.9,
):
    return {
        "status": "claimed",
        "claim_token": "claim-token-123",
        "row": {
            "id": candidate_id,
            "symbol": symbol,
            "direction": direction,
            "timestamp": timestamp,
            "confidence": confidence,
            "signal_id": f"sig-{candidate_id}",
            "claimed_by": "worker-test",
        },
    }


def test_worker_handles_empty_queue_without_crashing():
    fake_client = FakeApiClient(claim_sequence=[])
    worker = ExecutionWorker(_base_config(oneshot=True), fake_client, sleep_fn=lambda _: None)

    processed = worker.run()

    assert processed == 0
    assert fake_client.claim_calls == ["worker-test"]
    assert fake_client.patch_calls == []


@pytest.mark.parametrize(
    "claim_payload,expected_status,note_prefix",
    [
        (_claim_payload(confidence=0.92), "filled", "simulation_filled_confidence_"),
        (_claim_payload(confidence=0.40), "skipped", "simulation_skipped_confidence_"),
        (_claim_payload(symbol=None, confidence=0.90), "rejected", "simulation_rejected_missing_fields:"),
    ],
)
def test_worker_marks_claimed_candidate_using_simulation_rules(claim_payload, expected_status, note_prefix):
    fake_client = FakeApiClient(claim_sequence=[claim_payload])
    worker = ExecutionWorker(_base_config(oneshot=True), fake_client, sleep_fn=lambda _: None)

    processed = worker.run()

    assert processed == 1
    assert len(fake_client.patch_calls) == 1
    patch_call = fake_client.patch_calls[0]
    assert patch_call["execution_status"] == expected_status
    assert patch_call["execution_note"].startswith(note_prefix)


def test_worker_uses_auth_header_for_api_calls(monkeypatch):
    class StubResponse:
        def __init__(self, status_code, payload):
            self._status_code = status_code
            self._payload = payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def getcode(self):
            return self._status_code

        def read(self):
            return execution_worker.json.dumps(self._payload).encode("utf-8")

    captured = {}

    def fake_urlopen(request, timeout):
        captured["method"] = request.get_method()
        captured["url"] = request.full_url
        captured["headers"] = {k.lower(): v for k, v in request.header_items()}
        captured["timeout"] = timeout
        return StubResponse(200, {"status": "empty", "row": None})

    monkeypatch.setattr(execution_worker.urllib.request, "urlopen", fake_urlopen)

    client = ApiClient("http://127.0.0.1:8010", "secret-key", timeout_seconds=9.0)
    response = client.claim_next("worker-auth")

    assert response["status"] == "empty"
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/trade_candidates/claim_next")
    assert captured["headers"]["x-signal-key"] == "secret-key"
    assert captured["headers"]["content-type"] == "application/json"
    assert captured["timeout"] == pytest.approx(9.0)


def test_worker_oneshot_exits_after_one_processed_claim_even_if_more_work_exists():
    fake_client = FakeApiClient(
        claim_sequence=[
            _claim_payload(candidate_id=11, confidence=0.91),
            _claim_payload(candidate_id=12, confidence=0.95),
        ]
    )
    worker = ExecutionWorker(_base_config(oneshot=True), fake_client, sleep_fn=lambda _: None)

    processed = worker.run()

    assert processed == 1
    assert len(fake_client.claim_calls) == 1
    assert len(fake_client.patch_calls) == 1
    assert fake_client.patch_calls[0]["candidate_id"] == 11


def test_worker_sends_heartbeat_before_final_update_when_processing_is_long():
    clock = {"now": 0.0}

    def fake_sleep(seconds):
        clock["now"] += seconds

    def fake_monotonic():
        return clock["now"]

    fake_client = FakeApiClient(claim_sequence=[_claim_payload(candidate_id=21, confidence=0.89)])
    worker = ExecutionWorker(
        _base_config(oneshot=True, simulate_processing_seconds=2.0, heartbeat_interval_seconds=1.0),
        fake_client,
        sleep_fn=fake_sleep,
        monotonic_fn=fake_monotonic,
    )

    processed = worker.run()

    assert processed == 1
    assert len(fake_client.heartbeat_calls) == 1
    assert fake_client.heartbeat_calls[0]["id"] == 21
    assert len(fake_client.patch_calls) == 1


def test_worker_creates_execution_journal_row_after_status_update(tmp_path, monkeypatch):
    test_db_path = tmp_path / "worker_journal.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setattr(execution_worker, "insert_execution_journal_entry", event_writer.insert_execution_journal_entry)

    event_writer.init_db()

    fake_client = FakeApiClient(claim_sequence=[_claim_payload(candidate_id=321, confidence=0.91)])
    worker = ExecutionWorker(_base_config(oneshot=True), fake_client, sleep_fn=lambda _: None)

    processed = worker.run()
    rows = event_writer.get_recent_execution_journal(10)

    assert processed == 1
    assert len(rows) == 1
    assert rows[0]["candidate_id"] == 321
    assert rows[0]["worker_id"] == "worker-test"
    assert rows[0]["execution_status"] == "filled"
    assert rows[0]["action"] == "simulation_decision"


def test_worker_still_updates_status_when_journal_write_fails(monkeypatch):
    def raise_journal_failure(**_):
        raise RuntimeError("journal unavailable")

    monkeypatch.setattr(execution_worker, "insert_execution_journal_entry", raise_journal_failure)

    fake_client = FakeApiClient(claim_sequence=[_claim_payload(candidate_id=444, confidence=0.82)])
    worker = ExecutionWorker(_base_config(oneshot=True), fake_client, sleep_fn=lambda _: None)

    processed = worker.run()

    assert processed == 1
    assert len(fake_client.patch_calls) == 1
    assert fake_client.patch_calls[0]["candidate_id"] == 444


def test_execution_worker_contains_no_broker_sdk_imports():
    source = Path(execution_worker.__file__).read_text(encoding="utf-8")

    forbidden_tokens = ["ccxt", "alpaca", "binance", "bybit", "kraken"]
    for token in forbidden_tokens:
        assert token not in source
