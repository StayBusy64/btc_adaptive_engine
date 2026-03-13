import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import event_writer
from backend.api_server import app

INGEST_SIGNAL_KEY = "ingest-secret"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_tradingview_ingest.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
    ingest_root = tmp_path / "ingest"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", "test-signal-key")
    monkeypatch.setenv("SIGNAL_CLAIM_TIMEOUT_SECONDS", "60")
    monkeypatch.setenv("TRADINGVIEW_INGEST_ROOT", str(ingest_root))
    monkeypatch.setenv("TRADINGVIEW_INGEST_SIGNAL_KEY", INGEST_SIGNAL_KEY)
    monkeypatch.setenv("TRADINGVIEW_SIGNAL_LOG_FILE", str(ingest_root / "tv_ingest" / "signals.log"))
    monkeypatch.setenv("TRADINGVIEW_INGEST_RECEIPT_LOG_FILE", str(ingest_root / "logs" / "tv_ingest_receipts.jsonl"))
    monkeypatch.setenv("TRADINGVIEW_INGEST_SCHEDULER_ENABLED", "0")

    with TestClient(app) as test_client:
        yield test_client


def _build_batch_payload(batch_id: str = "batch-001") -> dict:
    return {
        "source": "tradingview",
        "namespace": "btc.adaptive",
        "symbol": "BTCUSDT",
        "chart_tf": "1m",
        "batch_id": batch_id,
        "batch_trigger_side": "buy",
        "batch_size": 2,
        "batch_close_time": 1773205500,
        "confirmed": True,
        "events": [
            {
                "event_id": f"{batch_id}-evt-1",
                "event_time": 1773205490,
                "side": "buy",
                "signal_type": "vp_breakout",
                "signal_family": "continuation",
                "price": 84150.25,
                "confirmed": True,
                "micro": {
                    "delta": 18.4,
                    "imbalance": 1.2,
                },
                "macro": {
                    "regime": "trend",
                    "session": "ny",
                },
            },
            {
                "event_id": f"{batch_id}-evt-2",
                "event_time": 1773205500,
                "side": "sell",
                "signal_type": "failed_auction",
                "signal_family": "reversion",
                "price": 84205.0,
                "confirmed": True,
                "micro": {
                    "delta": -11.0,
                    "imbalance": 0.7,
                },
                "macro": {
                    "regime": "balance",
                    "session": "ny",
                },
            },
        ],
    }


def _build_contextual_batch_payload(
    batch_id: str,
    *,
    event_time_ms: int,
    price: float,
    side: str = "sell",
    signal_type: str = "ema_cross_short",
) -> dict:
    open_price = price + 12.0
    high_price = price + 22.0
    low_price = price - 18.0
    close_price = price

    return {
        "source": "tradingview",
        "namespace": "btc.adaptive",
        "symbol": "BTCUSDT.P",
        "chart_tf": "1m",
        "batch_id": batch_id,
        "batch_trigger_side": side,
        "batch_size": 1,
        "batch_close_time": event_time_ms,
        "confirmed": True,
        "events": [
            {
                "event_id": f"{batch_id}-evt-1",
                "event_time": event_time_ms,
                "side": side,
                "signal_type": signal_type,
                "signal_family": "continuation",
                "price": price,
                "confirmed": True,
                "micro": {
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": 1000.0 + (event_time_ms % 1000),
                    "fast_ema": price + 8.0,
                    "slow_ema": price + 21.0,
                    "rsi": 43.0,
                    "bar_index": int(event_time_ms / 60000),
                    "bar_time": "2026-03-12T12:00:00Z",
                },
                "macro": {
                    "session": "ny",
                    "regime": "trend",
                },
            }
        ],
    }


def test_tradingview_batch_requires_valid_signal_key(client):
    payload = _build_batch_payload()

    missing = client.post("/webhooks/tradingview/batch", json=payload)
    invalid = client.post("/webhooks/tradingview/batch?signal_key=wrong", json=payload)

    assert missing.status_code == 401
    assert invalid.status_code == 401


def test_tradingview_batch_rejects_strict_schema_violations(client):
    payload_extra = _build_batch_payload("batch-extra")
    payload_extra["unexpected"] = "field"

    response_extra = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload_extra,
    )
    assert response_extra.status_code == 422

    payload_mismatch = _build_batch_payload("batch-mismatch")
    payload_mismatch["batch_size"] = 3

    response_mismatch = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload_mismatch,
    )
    assert response_mismatch.status_code == 422


def test_tradingview_batch_ingest_is_idempotent_on_duplicate_batch_id(client):
    payload = _build_batch_payload("batch-dup")

    first = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    second = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )

    assert first.status_code == 201
    assert second.status_code == 201

    first_body = first.json()
    second_body = second.json()

    assert first_body["status"] == "accepted"
    assert first_body["duplicate_batch"] is False
    assert first_body["queued_for_cycle"] is True

    assert second_body["status"] == "duplicate"
    assert second_body["duplicate_batch"] is True
    assert second_body["queued_for_cycle"] is False

    recent = client.get("/webhooks/tradingview/batches/recent?status=queued")
    assert recent.status_code == 200
    recent_body = recent.json()
    assert recent_body["count"] == 1
    assert recent_body["rows"][0]["batch_id"] == "batch-dup"
    assert recent_body["rows"][0]["status"] == "queued"
    assert recent_body["rows"][0]["active_exists"] is True

    batch_detail = client.get("/webhooks/tradingview/batch/batch-dup")
    assert batch_detail.status_code == 200
    detail_body = batch_detail.json()
    assert detail_body["batch_id"] == "batch-dup"
    assert detail_body["raw"]["payload"]["batch_id"] == "batch-dup"
    assert detail_body["status"]["status"] == "queued"


def test_tradingview_batch_appends_signal_log_rows(client):
    payload = _build_batch_payload("batch-log")

    first = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    duplicate = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )

    assert first.status_code == 201
    assert duplicate.status_code == 201

    log_path = Path(os.environ["TRADINGVIEW_SIGNAL_LOG_FILE"])
    assert log_path.exists()

    rows = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) >= 2
    assert rows[-2]["batch_id"] == "batch-log"
    assert rows[-2]["duplicate"] is False
    assert rows[-1]["batch_id"] == "batch-log"
    assert rows[-1]["duplicate"] is True


def test_tradingview_batch_appends_ingest_receipt_rows(client):
    payload = _build_batch_payload("batch-receipt")

    first = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    duplicate = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )

    assert first.status_code == 201
    assert duplicate.status_code == 201

    receipt_log = Path(os.environ["TRADINGVIEW_INGEST_RECEIPT_LOG_FILE"])
    assert receipt_log.exists()

    rows = [json.loads(line) for line in receipt_log.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) >= 2

    stored = rows[-2]
    dupe = rows[-1]

    assert stored["batch_id"] == "batch-receipt"
    assert stored["symbol"] == "BTCUSDT"
    assert stored["chart_tf"] == "1m"
    assert stored["batch_trigger_side"] == "buy"
    assert stored["event_count"] == 2
    assert stored["auth_result"] == "passed"
    assert stored["parse_result"] == "passed"
    assert stored["write_result"] == "stored"
    assert stored["status"] == "accepted"

    assert dupe["batch_id"] == "batch-receipt"
    assert dupe["write_result"] == "duplicate"
    assert dupe["status"] == "duplicate"


def test_tradingview_batch_normalizes_timestamp_inputs(client):
    payload = _build_batch_payload("batch-time-normalize")
    payload["batch_close_time"] = 0
    payload["events"][0]["event_time"] = "2026-03-12T10:00:00Z"
    payload["events"][1]["event_time"] = 0

    ingest = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    assert ingest.status_code == 201

    batch_detail = client.get("/webhooks/tradingview/batch/batch-time-normalize")
    assert batch_detail.status_code == 200

    raw_payload = batch_detail.json()["raw"]["payload"]
    assert isinstance(raw_payload["batch_close_time"], int)
    assert raw_payload["batch_close_time"] > 0
    assert raw_payload["events"][0]["event_time"] > 0
    assert raw_payload["events"][1]["event_time"] == raw_payload["batch_close_time"]


def test_tradingview_batch_rejects_invalid_timestamp_strings(client):
    payload = _build_batch_payload("batch-invalid-ts")
    payload["batch_close_time"] = "not-a-timestamp"

    response = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    assert response.status_code == 422


def test_tradingview_cycle_commits_and_wipes_active_intake(client):
    payload = _build_batch_payload("batch-cycle")
    ingest = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    assert ingest.status_code == 201

    cycle = client.post("/webhooks/tradingview/cycle/run")
    assert cycle.status_code == 200
    cycle_body = cycle.json()

    assert cycle_body["processed_batches"] == 1
    assert cycle_body["failed_batches"] == 0
    assert cycle_body["normalized_events_written"] == 2
    assert cycle_body["duplicate_events"] == 0
    assert cycle_body["active_remaining"] == 0

    batches_recent = client.get("/webhooks/tradingview/batches/recent?status=processed")
    assert batches_recent.status_code == 200
    rows = batches_recent.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["batch_id"] == "batch-cycle"
    assert rows[0]["status"] == "processed"
    assert rows[0]["active_exists"] is False

    events_recent = client.get("/webhooks/tradingview/events/recent?symbol=BTCUSDT")
    assert events_recent.status_code == 200
    events_rows = events_recent.json()["rows"]
    assert len(events_rows) == 2
    assert events_rows[0]["event_id"] == "batch-cycle-evt-2"
    assert events_rows[0]["side"] == "short"
    assert events_rows[1]["event_id"] == "batch-cycle-evt-1"
    assert events_rows[1]["side"] == "long"

    event_detail = client.get("/webhooks/tradingview/event/batch-cycle-evt-1")
    assert event_detail.status_code == 200
    event_body = event_detail.json()
    assert event_body["event"]["event_id"] == "batch-cycle-evt-1"
    assert event_body["micro"]["micro"]["delta"] == pytest.approx(18.4)
    assert event_body["macro"]["macro"]["regime"] == "trend"


def test_tradingview_cycle_preserves_release_metadata_and_research_context(client):
    payload = _build_contextual_batch_payload(
        "batch-versioned",
        event_time_ms=1773316800000,
        price=70350.0,
    )
    payload["release_id"] = "bridge_signal_sender_v2.1.0"
    payload["release_version"] = "2.1.0"
    payload["release_channel"] = "production"
    payload["contract_version"] = "tv-bridge-batch-v1"
    payload["telemetry_schema_version"] = "tv-telemetry-v1"
    payload["events"][0]["signal_name"] = "ema_cross_short"
    payload["events"][0]["strategy_id"] = "bridge_signal_sender_v2"
    payload["events"][0]["research"] = {
        "signal_quality_score": 0.82,
        "trend_slope_score": 0.76,
        "continuation_confidence": 0.79,
        "mean_reversion_risk": 0.24,
        "regime_bias_score": 0.71,
        "contradiction_pressure": 0.18,
        "unknown_probe": 0.44,
    }

    ingest = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    assert ingest.status_code == 201

    cycle = client.post("/webhooks/tradingview/cycle/run")
    assert cycle.status_code == 200

    event_detail = client.get("/webhooks/tradingview/event/batch-versioned-evt-1")
    assert event_detail.status_code == 200
    event_body = event_detail.json()["event"]
    assert event_body["signal_name"] == "ema_cross_short"
    assert event_body["strategy_id"] == "bridge_signal_sender_v2"
    assert event_body["release_id"] == "bridge_signal_sender_v2.1.0"
    assert event_body["release_version"] == "2.1.0"
    assert event_body["release_channel"] == "production"
    assert event_body["contract_version"] == "tv-bridge-batch-v1"
    assert event_body["telemetry_schema_version"] == "tv-telemetry-v1"
    assert event_body["research_context"]["signal_quality_score"] == pytest.approx(0.82)
    assert event_body["research_context"]["contradiction_pressure"] == pytest.approx(0.18)
    assert event_body["research_unknown_context"]["unknown_probe"] == pytest.approx(0.44)

    journal = client.get("/webhooks/tradingview/signal-journal/recent?signal_name=ema_cross_short")
    assert journal.status_code == 200
    journal_rows = journal.json()["rows"]
    assert len(journal_rows) == 1
    assert journal_rows[0]["release_version"] == "2.1.0"
    assert journal_rows[0]["release_channel"] == "production"
    assert journal_rows[0]["contract_version"] == "tv-bridge-batch-v1"
    assert journal_rows[0]["research_context"]["signal_quality_score"] == pytest.approx(0.82)
    assert journal_rows[0]["research_unknown_context"]["unknown_probe"] == pytest.approx(0.44)


def test_tradingview_batch_replay_supports_duplicate_and_overwrite_modes(client):
    payload = _build_batch_payload("batch-replay")
    ingest = client.post(
        f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
        json=payload,
    )
    assert ingest.status_code == 201

    first_cycle = client.post("/webhooks/tradingview/cycle/run")
    assert first_cycle.status_code == 200

    replay = client.post("/webhooks/tradingview/batch/batch-replay/replay")
    assert replay.status_code == 200
    replay_body = replay.json()
    assert replay_body["status"] == "replayed"
    assert replay_body["events_in_batch"] == 2
    assert replay_body["written_events"] == 0
    assert replay_body["duplicate_events"] == 2

    replay_overwrite = client.post("/webhooks/tradingview/batch/batch-replay/replay?overwrite=true")
    assert replay_overwrite.status_code == 200
    replay_overwrite_body = replay_overwrite.json()
    assert replay_overwrite_body["status"] == "replayed_overwrite"
    assert replay_overwrite_body["written_events"] == 2
    assert replay_overwrite_body["duplicate_events"] == 0


def test_tradingview_ingest_returns_404_for_missing_batch_or_event(client):
    missing_batch = client.get("/webhooks/tradingview/batch/does-not-exist")
    missing_event = client.get("/webhooks/tradingview/event/does-not-exist")
    replay_missing = client.post("/webhooks/tradingview/batch/does-not-exist/replay")

    assert missing_batch.status_code == 404
    assert missing_event.status_code == 404
    assert replay_missing.status_code == 404


def test_tradingview_cycle_accumulates_signal_journal_outcomes_and_market_bias(client):
    start_time_ms = 1773316800000

    for index in range(6):
        batch_id = f"batch-analytics-{index + 1}"
        payload = _build_contextual_batch_payload(
            batch_id,
            event_time_ms=start_time_ms + (index * 60000),
            price=70350.0 - (index * 12.5),
        )

        ingest = client.post(
            f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
            json=payload,
        )
        assert ingest.status_code == 201

        cycle = client.post("/webhooks/tradingview/cycle/run")
        assert cycle.status_code == 200

    journal = client.get("/webhooks/tradingview/signal-journal/recent?signal_name=ema_cross_short")
    assert journal.status_code == 200
    journal_rows = journal.json()["rows"]
    assert len(journal_rows) == 6
    assert journal_rows[0]["symbol"] == "BTCUSDT.P"
    assert journal_rows[0]["signal_name"] == "ema_cross_short"
    assert journal_rows[0]["ema_spread"] is not None
    assert journal_rows[0]["distance_from_fast"] is not None
    assert journal_rows[0]["candle_range"] is not None
    assert journal_rows[0]["wick_ratio"] is not None

    outcomes = client.get("/webhooks/tradingview/signal-outcomes/recent?signal_name=ema_cross_short")
    assert outcomes.status_code == 200
    outcome_rows = outcomes.json()["rows"]
    assert len(outcome_rows) >= 1
    assert outcome_rows[0]["signal_name"] == "ema_cross_short"
    assert outcome_rows[0]["reversion_hit_5bar"] in {True, False}
    assert outcome_rows[0]["continuation_hit_5bar"] in {True, False}
    assert outcome_rows[0]["bars_available"] >= 5

    bias = client.get("/webhooks/tradingview/market-bias/recent?signal_name=ema_cross_short")
    assert bias.status_code == 200
    bias_rows = bias.json()["rows"]
    assert len(bias_rows) >= 1
    assert bias_rows[0]["signal_name"] == "ema_cross_short"
    assert bias_rows[0]["sample_count"] >= 1
    assert 0.0 <= bias_rows[0]["reversion_bias"] <= 1.0
    assert 0.0 <= bias_rows[0]["continuation_bias"] <= 1.0


def test_signal_outcome_run_backfills_journal_from_existing_normalized_events(client):
    start_time_ms = 1773316800000

    for index in range(6):
        batch_id = f"batch-backfill-{index + 1}"
        payload = _build_contextual_batch_payload(
            batch_id,
            event_time_ms=start_time_ms + (index * 60000),
            price=70400.0 - (index * 10.0),
        )

        ingest = client.post(
            f"/webhooks/tradingview/batch?signal_key={INGEST_SIGNAL_KEY}",
            json=payload,
        )
        assert ingest.status_code == 201

        cycle = client.post("/webhooks/tradingview/cycle/run")
        assert cycle.status_code == 200

    ingest_root = Path(os.environ["TRADINGVIEW_INGEST_ROOT"])
    journal_file = ingest_root / "state" / "outcomes" / "signal_journal.jsonl"
    outcome_file = ingest_root / "state" / "outcomes" / "signal_outcomes.jsonl"
    bias_file = ingest_root / "state" / "policy_scores" / "market_bias_scores.jsonl"

    for path in (journal_file, outcome_file, bias_file):
        if path.exists():
            path.unlink()

    run_response = client.post("/webhooks/tradingview/signal-outcomes/run?min_future_bars=1")
    assert run_response.status_code == 200

    run_body = run_response.json()
    assert run_body["journal_backfill"]["written_count"] == 6
    assert run_body["outcome"]["evaluated_new_count"] >= 1
    assert run_body["bias"]["computed_count"] >= 1

    journal = client.get("/webhooks/tradingview/signal-journal/recent?signal_name=ema_cross_short")
    assert journal.status_code == 200
    assert journal.json()["count"] == 6

    outcomes = client.get("/webhooks/tradingview/signal-outcomes/recent?signal_name=ema_cross_short")
    assert outcomes.status_code == 200
    assert outcomes.json()["count"] >= 1

    bias = client.get("/webhooks/tradingview/market-bias/recent?signal_name=ema_cross_short")
    assert bias.status_code == 200
    assert bias.json()["count"] >= 1
