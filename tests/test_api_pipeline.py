import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import event_writer
from backend.api_server import app

SIGNAL_KEY = "test-signal-key"
AUTH_HEADERS = {"X-SIGNAL-KEY": SIGNAL_KEY}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_system.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", SIGNAL_KEY)
    monkeypatch.setenv("SIGNAL_CLAIM_TIMEOUT_SECONDS", "60")

    with TestClient(app) as test_client:
        yield test_client


def test_health_returns_alive_and_database_reachable(client):
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["api"] == "alive"
    assert body["database"] == "reachable"


def test_bar_states_recent_returns_200(client):
    response = client.get("/bar_states/recent")

    assert response.status_code == 200
    body = response.json()
    assert "rows" in body
    assert isinstance(body["rows"], list)


def test_feature_snapshots_recent_returns_inserted_rows(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:20:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=12,
        feature_version="pytest-v1",
        feature_values={
            "trend_alignment_score": 0.8,
            "volatility_zscore_20": 1.2,
        },
        regime_output={
            "regime_id": "trend_expansion",
            "regime_confidence": 0.83,
            "transition_risk": 0.18,
        },
        model_output={
            "long_probability": 0.61,
            "short_probability": 0.17,
            "no_trade_probability": 0.22,
            "expected_excursion": 15.0,
            "setup_trust_score": 0.56,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] >= 1
    assert body["limit"] == 20

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["timestamp"] == "2026-03-10T14:20:00Z"
    assert row["symbol"] == "BTCUSDT.P"
    assert row["timeframe"] == "1m"
    assert row["source_bar_id"] == 12
    assert row["regime_id"] == "trend_expansion"
    assert row["long_probability"] == pytest.approx(0.61)
    assert row["short_probability"] == pytest.approx(0.17)
    assert row["no_trade_probability"] == pytest.approx(0.22)

    feature_json = json.loads(row["feature_json"])
    assert feature_json["trend_alignment_score"] == pytest.approx(0.8)
    assert feature_json["volatility_zscore_20"] == pytest.approx(1.2)


def test_feature_snapshots_recent_exposes_vp_failed_auction_context_flags(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:21:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=13,
        feature_version="pytest-v1",
        feature_values={
            "vp_failed_auction_above": 1,
            "vp_failed_auction_below": 0,
            "vp_reversion_to_value_from_above_context": 1,
            "vp_reversion_to_value_from_below_context": 0,
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_failed_auction_above"] == 1
    assert row["vp_failed_auction_below"] == 0
    assert row["vp_reversion_to_value_from_above_context"] == 1
    assert row["vp_reversion_to_value_from_below_context"] == 0


def test_feature_snapshots_recent_exposes_vp_acceptance_and_continuation_flags(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:22:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=14,
        feature_version="pytest-v1",
        feature_values={
            "vp_acceptance_outside_value_above": 1,
            "vp_acceptance_outside_value_below": 0,
            "vp_continuation_auction_up": 1,
            "vp_continuation_auction_down": 0,
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_acceptance_outside_value_above"] == 1
    assert row["vp_acceptance_outside_value_below"] == 0
    assert row["vp_continuation_auction_up"] == 1
    assert row["vp_continuation_auction_down"] == 0


def test_feature_snapshots_recent_exposes_vp_auction_regime(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:23:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=15,
        feature_version="pytest-v1",
        feature_values={
            "vp_auction_regime": "continuation_up",
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_auction_regime"] == "continuation_up"


def test_feature_snapshots_recent_exposes_vp_trade_bias(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:24:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=16,
        feature_version="pytest-v1",
        feature_values={
            "vp_trade_bias": "long_continuation",
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_trade_bias"] == "long_continuation"


def test_feature_snapshots_recent_exposes_vp_trade_bias_actionable(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:25:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=17,
        feature_version="pytest-v1",
        feature_values={
            "vp_trade_bias_actionable": 1,
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_trade_bias_actionable"] == 1


def test_feature_snapshots_recent_exposes_vp_trade_bias_confidence(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:26:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=18,
        feature_version="pytest-v1",
        feature_values={
            "vp_trade_bias_confidence": "high",
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_trade_bias_confidence"] == "high"


def test_feature_snapshots_recent_exposes_vp_trade_bias_score(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:27:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=19,
        feature_version="pytest-v1",
        feature_values={
            "vp_trade_bias_score": 3,
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_trade_bias_score"] == 3


def test_feature_snapshots_recent_exposes_vp_policy_candidate(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:27:30Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=191,
        feature_version="pytest-v1",
        feature_values={
            "vp_policy_candidate": 1,
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_policy_candidate"] == 1


def test_feature_snapshots_recent_exposes_vp_policy_side(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:27:45Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=192,
        feature_version="pytest-v1",
        feature_values={
            "vp_policy_side": "long",
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_policy_side"] == "long"


def test_feature_snapshots_recent_exposes_vp_policy_reason(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:28:15Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=193,
        feature_version="pytest-v1",
        feature_values={
            "vp_policy_reason": "long|continuation_up|long_continuation|high|score=3|candidate=1",
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_policy_reason"] == "long|continuation_up|long_continuation|high|score=3|candidate=1"


def test_feature_snapshots_recent_exposes_vp_trade_bias_summary(client):
    snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:28:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=20,
        feature_version="pytest-v1",
        feature_values={
            "vp_trade_bias_summary": "continuation_up|long_continuation|high|score=3",
        },
        regime_output={
            "regime_id": "balanced",
            "regime_confidence": 0.5,
            "transition_risk": 0.2,
        },
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == snapshot_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_trade_bias_summary"] == "continuation_up|long_continuation|high|score=3"


def test_execution_journal_recent_exposes_vp_policy_decision_fields(client):
    journal_id = event_writer.insert_execution_journal_entry(
        candidate_id=999,
        signal_id="sig-vp-policy-001",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        confidence=0.82,
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=82500.0,
        metadata_json={
            "vp_policy_candidate": 1,
            "vp_policy_side": "long",
            "vp_trade_bias_score": 3,
            "vp_policy_reason": "long|continuation_up|long_continuation|high|score=3|candidate=1",
        },
        created_at="2026-03-10T16:00:00Z",
    )

    response = client.get("/execution_journal/recent?action=simulation_decision")

    assert response.status_code == 200
    body = response.json()

    inserted_rows = [row for row in body["rows"] if row["id"] == journal_id]
    assert len(inserted_rows) == 1

    row = inserted_rows[0]
    assert row["vp_policy_candidate"] == 1
    assert row["vp_policy_side"] == "long"
    assert row["vp_trade_bias_score"] == 3
    assert row["vp_policy_reason"] == "long|continuation_up|long_continuation|high|score=3|candidate=1"


def test_feature_snapshots_recent_filters_by_symbol(client):
    event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:30:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=31,
        feature_version="pytest-v1",
        feature_values={"feature_index": 1.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )
    event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:31:00Z",
        symbol="ETHUSDT.P",
        timeframe="1m",
        source_bar_id=32,
        feature_version="pytest-v1",
        feature_values={"feature_index": 2.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent?symbol=BTCUSDT.P")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] >= 1
    assert all(row["symbol"] == "BTCUSDT.P" for row in body["rows"])


def test_feature_snapshots_recent_filters_by_timeframe(client):
    event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:40:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=41,
        feature_version="pytest-v1",
        feature_values={"feature_index": 1.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )
    event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:41:00Z",
        symbol="BTCUSDT.P",
        timeframe="5m",
        source_bar_id=42,
        feature_version="pytest-v1",
        feature_values={"feature_index": 2.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent?timeframe=5m")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] >= 1
    assert all(row["timeframe"] == "5m" for row in body["rows"])


def test_feature_snapshots_recent_filters_by_symbol_and_timeframe(client):
    event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:50:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=51,
        feature_version="pytest-v1",
        feature_values={"feature_index": 1.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )
    matching_snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:51:00Z",
        symbol="BTCUSDT.P",
        timeframe="5m",
        source_bar_id=52,
        feature_version="pytest-v1",
        feature_values={"feature_index": 2.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )
    event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T14:52:00Z",
        symbol="ETHUSDT.P",
        timeframe="5m",
        source_bar_id=53,
        feature_version="pytest-v1",
        feature_values={"feature_index": 3.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent?symbol=BTCUSDT.P&timeframe=5m")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["rows"][0]["id"] == matching_snapshot_id
    assert body["rows"][0]["symbol"] == "BTCUSDT.P"
    assert body["rows"][0]["timeframe"] == "5m"


def test_feature_snapshots_recent_without_filters_preserves_current_behavior(client):
    older_snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T15:00:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=61,
        feature_version="pytest-v1",
        feature_values={"feature_index": 1.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )
    newer_snapshot_id = event_writer.insert_feature_snapshot(
        timestamp="2026-03-10T15:01:00Z",
        symbol="ETHUSDT.P",
        timeframe="5m",
        source_bar_id=62,
        feature_version="pytest-v1",
        feature_values={"feature_index": 2.0},
        regime_output={"regime_id": "balanced", "regime_confidence": 0.4, "transition_risk": 0.3},
        model_output={
            "long_probability": 0.4,
            "short_probability": 0.3,
            "no_trade_probability": 0.3,
            "expected_excursion": 1.0,
            "setup_trust_score": 0.4,
        },
    )

    response = client.get("/feature_snapshots/recent?limit=2")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["limit"] == 2
    assert [row["id"] for row in body["rows"]] == [newer_snapshot_id, older_snapshot_id]


def test_feature_snapshots_recent_honors_limit(client):
    for index in range(3):
        event_writer.insert_feature_snapshot(
            timestamp=f"2026-03-10T14:2{index}:00Z",
            symbol="BTCUSDT.P",
            timeframe="1m",
            source_bar_id=index + 1,
            feature_version="pytest-v1",
            feature_values={"feature_index": float(index)},
            regime_output={
                "regime_id": f"regime-{index}",
                "regime_confidence": 0.5,
                "transition_risk": 0.2,
            },
            model_output={
                "long_probability": 0.4,
                "short_probability": 0.3,
                "no_trade_probability": 0.3,
                "expected_excursion": 1.0,
                "setup_trust_score": 0.4,
            },
        )

    response = client.get("/feature_snapshots/recent?limit=2")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["limit"] == 2
    assert len(body["rows"]) == 2
    assert body["rows"][0]["timestamp"] == "2026-03-10T14:22:00Z"
    assert body["rows"][1]["timestamp"] == "2026-03-10T14:21:00Z"


def test_feature_snapshots_recent_returns_empty_rows_when_no_snapshots_exist(client):
    response = client.get("/feature_snapshots/recent")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 0
    assert body["limit"] == 20
    assert body["rows"] == []


def test_post_trade_candidates_and_recent_returns_inserted_record(client):
    payload = {
        "signal_id": "test-signal-001",
        "timestamp": "2026-03-10T14:00:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
        "entry_price": 82000.0,
        "stop_price": 81750.0,
        "tp1": 82300.0,
        "tp2": 82600.0,
        "confidence": 0.81,
        "setup_family": "momentum",
        "payload_json": {"source": "pytest"},
    }

    post_response = client.post("/trade_candidates", json=payload, headers=AUTH_HEADERS)

    assert post_response.status_code == 201
    post_body = post_response.json()
    inserted_id = post_body["id"]
    assert inserted_id > 0

    recent_response = client.get("/trade_candidates/recent")
    assert recent_response.status_code == 200

    recent_body = recent_response.json()
    assert recent_body["count"] >= 1
    inserted_rows = [row for row in recent_body["rows"] if row["id"] == inserted_id]
    assert len(inserted_rows) == 1
    assert inserted_rows[0]["execution_status"] == "pending"


def test_trade_candidates_recent_limit_is_capped(client):
    response = client.get("/trade_candidates/recent?limit=999")

    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 500


def test_trade_candidates_invalid_payload_returns_422(client):
    invalid_payload = {
        "timestamp": "2026-03-10T14:00:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "up",
    }

    response = client.post("/trade_candidates", json=invalid_payload, headers=AUTH_HEADERS)

    assert response.status_code == 422


def test_trade_candidates_from_event_derives_and_inserts(client):
    payload = {
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "side": "buy",
        "price": 82200.5,
        "event_type": "breakout",
        "strategy": "adaptive-v1",
        "source": "webhook",
        "confidence": 0.74,
        "timestamp": "2026-03-10T14:05:00Z",
        "payload_json": {"event_id": "evt-001"},
    }

    response = client.post("/trade_candidates/from_event", json=payload, headers=AUTH_HEADERS)

    assert response.status_code == 201
    body = response.json()
    assert body["derived_from_event"] is True
    assert body["id"] > 0

    recent_response = client.get("/trade_candidates/recent")
    assert recent_response.status_code == 200
    recent_rows = recent_response.json()["rows"]
    matching_rows = [row for row in recent_rows if row["id"] == body["id"]]
    assert len(matching_rows) == 1
    assert matching_rows[0]["execution_status"] == "pending"


def test_trade_candidates_from_event_missing_header_returns_403(client):
    payload = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:06:00Z",
        "side": "buy",
        "price": 82210.0,
    }

    response = client.post("/trade_candidates/from_event", json=payload)

    assert response.status_code == 403


def test_trade_candidates_from_event_wrong_header_returns_403(client):
    payload = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:06:30Z",
        "side": "sell",
        "price": 82190.0,
    }

    response = client.post(
        "/trade_candidates/from_event",
        json=payload,
        headers={"X-SIGNAL-KEY": "wrong-key"},
    )

    assert response.status_code == 403


def test_trade_candidates_recent_filters_by_symbol(client):
    payload_a = {
        "signal_id": "sig-filter-symbol-1",
        "timestamp": "2026-03-10T14:10:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    payload_b = {
        "signal_id": "sig-filter-symbol-2",
        "timestamp": "2026-03-10T14:11:00Z",
        "symbol": "ETHUSDT.P",
        "direction": "short",
    }
    assert client.post("/trade_candidates", json=payload_a, headers=AUTH_HEADERS).status_code == 201
    assert client.post("/trade_candidates", json=payload_b, headers=AUTH_HEADERS).status_code == 201

    response = client.get("/trade_candidates/recent?symbol=BTCUSDT.P")
    assert response.status_code == 200
    rows = response.json()["rows"]
    assert len(rows) >= 1
    assert all(row["symbol"] == "BTCUSDT.P" for row in rows)


def test_trade_candidates_recent_filters_by_direction(client):
    payload_long = {
        "signal_id": "sig-filter-direction-1",
        "timestamp": "2026-03-10T14:12:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    payload_short = {
        "signal_id": "sig-filter-direction-2",
        "timestamp": "2026-03-10T14:13:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "short",
    }
    assert client.post("/trade_candidates", json=payload_long, headers=AUTH_HEADERS).status_code == 201
    assert client.post("/trade_candidates", json=payload_short, headers=AUTH_HEADERS).status_code == 201

    response = client.get("/trade_candidates/recent?direction=long")
    assert response.status_code == 200
    rows = response.json()["rows"]
    assert len(rows) >= 1
    assert all(row["direction"] == "long" for row in rows)


def test_trade_candidates_recent_filters_by_strategy_and_source(client):
    event_a = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:14:00Z",
        "side": "buy",
        "timeframe": "1m",
        "price": 82250.0,
        "strategy": "adaptive-v2",
        "source": "tv-webhook",
        "event_type": "continuation",
    }
    event_b = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:15:00Z",
        "side": "buy",
        "timeframe": "1m",
        "price": 82275.0,
        "strategy": "mean-reversion",
        "source": "other-feed",
        "event_type": "fade",
    }
    assert client.post("/trade_candidates/from_event", json=event_a, headers=AUTH_HEADERS).status_code == 201
    assert client.post("/trade_candidates/from_event", json=event_b, headers=AUTH_HEADERS).status_code == 201

    strategy_response = client.get("/trade_candidates/recent?strategy=adaptive-v2")
    assert strategy_response.status_code == 200
    for row in strategy_response.json()["rows"]:
        payload_json = json.loads(row["payload_json"]) if row["payload_json"] else {}
        assert payload_json.get("strategy") == "adaptive-v2"

    source_response = client.get("/trade_candidates/recent?source=tv-webhook")
    assert source_response.status_code == 200
    for row in source_response.json()["rows"]:
        payload_json = json.loads(row["payload_json"]) if row["payload_json"] else {}
        assert payload_json.get("source") == "tv-webhook"


def test_trade_candidates_recent_filters_by_derived_flag(client):
    plain_payload = {
        "signal_id": "sig-derived-filter-plain",
        "timestamp": "2026-03-10T14:16:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    event_payload = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:17:00Z",
        "side": "buy",
        "timeframe": "1m",
        "price": 82300.0,
        "strategy": "adaptive-v2",
        "source": "webhook",
        "event_type": "continuation",
    }
    assert client.post("/trade_candidates", json=plain_payload, headers=AUTH_HEADERS).status_code == 201
    assert client.post("/trade_candidates/from_event", json=event_payload, headers=AUTH_HEADERS).status_code == 201

    derived_response = client.get("/trade_candidates/recent?derived_from_event=true")
    assert derived_response.status_code == 200
    for row in derived_response.json()["rows"]:
        payload_json = json.loads(row["payload_json"]) if row["payload_json"] else {}
        assert payload_json.get("derived_from_event") is True


def test_trade_candidates_recent_combined_filters(client):
    match_payload = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:18:00Z",
        "side": "buy",
        "timeframe": "1m",
        "price": 82320.0,
        "strategy": "adaptive-v3",
        "source": "webhook",
        "event_type": "continuation",
    }
    non_match_payload = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:19:00Z",
        "side": "buy",
        "timeframe": "5m",
        "price": 82330.0,
        "strategy": "adaptive-v3",
        "source": "webhook",
        "event_type": "continuation",
    }
    assert client.post("/trade_candidates/from_event", json=match_payload, headers=AUTH_HEADERS).status_code == 201
    assert client.post("/trade_candidates/from_event", json=non_match_payload, headers=AUTH_HEADERS).status_code == 201

    response = client.get(
        "/trade_candidates/recent?symbol=BTCUSDT.P&timeframe=1m&derived_from_event=true"
    )
    assert response.status_code == 200
    rows = response.json()["rows"]
    assert len(rows) >= 1
    for row in rows:
        payload_json = json.loads(row["payload_json"]) if row["payload_json"] else {}
        assert row["symbol"] == "BTCUSDT.P"
        assert payload_json.get("timeframe") == "1m"
        assert payload_json.get("derived_from_event") is True


def test_trade_candidates_recent_limit_capping_with_filters(client):
    payload = {
        "symbol": "BTCUSDT.P",
        "timestamp": "2026-03-10T14:20:00Z",
        "side": "buy",
        "timeframe": "1m",
        "price": 82350.0,
        "strategy": "adaptive-v2",
        "source": "webhook",
        "event_type": "continuation",
    }
    assert client.post("/trade_candidates/from_event", json=payload, headers=AUTH_HEADERS).status_code == 201

    response = client.get("/trade_candidates/recent?symbol=BTCUSDT.P&limit=999")
    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 500


def test_openapi_json_loads_and_contains_trade_candidate_examples(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200

    schema = response.json()
    trade_candidate_examples = (
        schema["paths"]["/trade_candidates"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    )
    from_event_examples = (
        schema["paths"]["/trade_candidates/from_event"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    )
    recent_description = schema["paths"]["/trade_candidates/recent"]["get"].get("description", "")

    assert "momentumLong" in trade_candidate_examples
    assert "webhookEvent" in from_event_examples
    assert "/trade_candidates/recent?symbol=BTCUSDT.P" in recent_description


def test_trade_candidates_from_event_duplicate_replay_is_not_inserted_twice(client):
    payload = {
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "side": "buy",
        "price": 82510.0,
        "event_type": "continuation",
        "strategy": "adaptive-v2",
        "source": "webhook",
        "confidence": 0.88,
        "timestamp": "2026-03-10T18:00:00Z",
        "payload_json": {"event_id": "evt-dup-001"},
    }

    first_response = client.post("/trade_candidates/from_event", json=payload, headers=AUTH_HEADERS)
    assert first_response.status_code == 201
    first_body = first_response.json()

    second_response = client.post("/trade_candidates/from_event", json=payload, headers=AUTH_HEADERS)
    assert second_response.status_code == 200
    second_body = second_response.json()
    assert second_body["status"] == "duplicate"
    assert second_body["replayed"] is True
    assert second_body["id"] == first_body["id"]

    recent_response = client.get("/trade_candidates/recent")
    assert recent_response.status_code == 200
    rows = recent_response.json()["rows"]
    matching_event_rows = []
    for row in rows:
        payload_json = json.loads(row["payload_json"]) if row["payload_json"] else {}
        if payload_json.get("event_id") == "evt-dup-001":
            matching_event_rows.append(row)

    assert len(matching_event_rows) == 1


def test_patch_trade_candidate_status_success(client):
    create_payload = {
        "signal_id": "sig-status-001",
        "timestamp": "2026-03-10T18:05:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201
    candidate_id = create_response.json()["id"]

    patch_payload = {
        "execution_status": "filled",
        "execution_note": "Filled by broker",
        "executed_at": "2026-03-10T18:06:00Z",
    }
    patch_response = client.patch(
        f"/trade_candidates/{candidate_id}/status",
        json=patch_payload,
        headers=AUTH_HEADERS,
    )

    assert patch_response.status_code == 200
    body = patch_response.json()
    assert body["status"] == "updated"
    assert body["id"] == candidate_id
    assert body["execution_status"] == "filled"


def test_patch_trade_candidate_status_missing_id_returns_404(client):
    patch_payload = {
        "execution_status": "rejected",
        "execution_note": "No liquidity",
    }

    patch_response = client.patch(
        "/trade_candidates/999999/status",
        json=patch_payload,
        headers=AUTH_HEADERS,
    )

    assert patch_response.status_code == 404


def test_patch_trade_candidate_status_validation_failure_returns_422(client):
    create_payload = {
        "signal_id": "sig-status-002",
        "timestamp": "2026-03-10T18:07:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "short",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201
    candidate_id = create_response.json()["id"]

    invalid_patch_payload = {
        "execution_status": "unknown",
    }
    patch_response = client.patch(
        f"/trade_candidates/{candidate_id}/status",
        json=invalid_patch_payload,
        headers=AUTH_HEADERS,
    )

    assert patch_response.status_code == 422


def test_trade_candidates_recent_filters_by_execution_status(client):
    create_payload = {
        "signal_id": "sig-status-003",
        "timestamp": "2026-03-10T18:08:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201
    candidate_id = create_response.json()["id"]

    patch_response = client.patch(
        f"/trade_candidates/{candidate_id}/status",
        json={"execution_status": "filled"},
        headers=AUTH_HEADERS,
    )
    assert patch_response.status_code == 200

    response = client.get("/trade_candidates/recent?execution_status=filled")
    assert response.status_code == 200
    rows = response.json()["rows"]
    assert any(row["id"] == candidate_id for row in rows)
    assert all(row["execution_status"] == "filled" for row in rows)


def test_claim_next_success_moves_one_pending_to_submitted(client):
    first = {
        "signal_id": "sig-claim-001",
        "timestamp": "2026-03-10T19:10:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    second = {
        "signal_id": "sig-claim-002",
        "timestamp": "2026-03-10T19:11:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "short",
    }

    response_a = client.post("/trade_candidates", json=first, headers=AUTH_HEADERS)
    response_b = client.post("/trade_candidates", json=second, headers=AUTH_HEADERS)
    assert response_a.status_code == 201
    assert response_b.status_code == 201

    claim_response = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-a"},
        headers=AUTH_HEADERS,
    )
    assert claim_response.status_code == 200
    claim_body = claim_response.json()
    assert claim_body["status"] == "claimed"
    assert claim_body["row"]["execution_status"] == "submitted"
    assert claim_body["row"]["id"] == response_b.json()["id"]
    assert claim_body["claim_token"] == claim_body["row"]["claim_token"]
    assert claim_body["row"]["claimed_by"] == "worker-a"
    assert claim_body["row"]["claimed_at"] is not None

    pending_response = client.get("/trade_candidates/recent?execution_status=pending")
    submitted_response = client.get("/trade_candidates/recent?execution_status=submitted")
    assert pending_response.status_code == 200
    assert submitted_response.status_code == 200
    assert pending_response.json()["count"] == 1
    assert submitted_response.json()["count"] == 1


def test_claim_next_returns_empty_when_no_pending(client):
    response = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-a"},
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "empty"
    assert body["row"] is None


def test_claim_next_requires_auth(client):
    response = client.post("/trade_candidates/claim_next", json={"worker_id": "worker-a"})

    assert response.status_code == 403


def test_trade_candidates_summary_counts_are_correct(client):
    payload_1 = {
        "signal_id": "sig-summary-001",
        "timestamp": "2026-03-10T19:20:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    payload_2 = {
        "signal_id": "sig-summary-002",
        "timestamp": "2026-03-10T19:21:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "short",
    }
    payload_3 = {
        "signal_id": "sig-summary-003",
        "timestamp": "2026-03-10T19:22:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }

    c1 = client.post("/trade_candidates", json=payload_1, headers=AUTH_HEADERS)
    c2 = client.post("/trade_candidates", json=payload_2, headers=AUTH_HEADERS)
    c3 = client.post("/trade_candidates", json=payload_3, headers=AUTH_HEADERS)
    assert c1.status_code == 201
    assert c2.status_code == 201
    assert c3.status_code == 201

    id_1 = c1.json()["id"]
    id_2 = c2.json()["id"]

    patch_1 = client.patch(
        f"/trade_candidates/{id_1}/status",
        json={"execution_status": "filled"},
        headers=AUTH_HEADERS,
    )
    patch_2 = client.patch(
        f"/trade_candidates/{id_2}/status",
        json={"execution_status": "rejected"},
        headers=AUTH_HEADERS,
    )
    claim = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-a"},
        headers=AUTH_HEADERS,
    )

    assert patch_1.status_code == 200
    assert patch_2.status_code == 200
    assert claim.status_code == 200
    assert claim.json()["status"] == "claimed"

    summary_response = client.get("/trade_candidates/summary")
    assert summary_response.status_code == 200
    summary = summary_response.json()

    assert summary["pending"] == 0
    assert summary["submitted"] == 1
    assert summary["filled"] == 1
    assert summary["rejected"] == 1
    assert summary["skipped"] == 0
    assert summary["leased_submitted_count"] == 1
    assert summary["stale_submitted_count"] == 0
    assert summary["total"] == 3


def test_heartbeat_success(client):
    create_payload = {
        "signal_id": "sig-heartbeat-001",
        "timestamp": "2026-03-10T19:30:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201

    claim_response = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-heartbeat"},
        headers=AUTH_HEADERS,
    )
    assert claim_response.status_code == 200
    claim_body = claim_response.json()
    candidate_id = claim_body["row"]["id"]
    claim_token = claim_body["claim_token"]

    heartbeat_response = client.post(
        f"/trade_candidates/{candidate_id}/heartbeat",
        json={"worker_id": "worker-heartbeat", "claim_token": claim_token},
        headers=AUTH_HEADERS,
    )
    assert heartbeat_response.status_code == 200
    body = heartbeat_response.json()
    assert body["status"] == "ok"
    assert body["id"] == candidate_id
    assert body["claimed_by"] == "worker-heartbeat"
    assert body["claim_token"] == claim_token


def test_heartbeat_lease_mismatch_returns_409(client):
    create_payload = {
        "signal_id": "sig-heartbeat-002",
        "timestamp": "2026-03-10T19:31:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "short",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201

    claim_response = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-one"},
        headers=AUTH_HEADERS,
    )
    assert claim_response.status_code == 200
    claim_body = claim_response.json()
    candidate_id = claim_body["row"]["id"]

    heartbeat_response = client.post(
        f"/trade_candidates/{candidate_id}/heartbeat",
        json={"worker_id": "worker-two", "claim_token": claim_body["claim_token"]},
        headers=AUTH_HEADERS,
    )
    assert heartbeat_response.status_code == 409


def test_release_back_to_pending_success(client):
    create_payload = {
        "signal_id": "sig-release-001",
        "timestamp": "2026-03-10T19:32:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201

    claim_response = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-release"},
        headers=AUTH_HEADERS,
    )
    assert claim_response.status_code == 200
    claim_body = claim_response.json()
    candidate_id = claim_body["row"]["id"]

    release_response = client.post(
        f"/trade_candidates/{candidate_id}/release",
        json={
            "worker_id": "worker-release",
            "claim_token": claim_body["claim_token"],
            "execution_status": "pending",
            "execution_note": "release to queue",
        },
        headers=AUTH_HEADERS,
    )
    assert release_response.status_code == 200
    body = release_response.json()
    assert body["status"] == "released"
    assert body["execution_status"] == "pending"
    assert body["claimed_by"] is None
    assert body["claim_token"] is None
    assert body["claimed_at"] is None


@pytest.mark.parametrize("release_status", ["skipped", "rejected"])
def test_release_to_skipped_or_rejected_success(client, release_status):
    create_payload = {
        "signal_id": f"sig-release-{release_status}",
        "timestamp": "2026-03-10T19:33:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "short",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201

    claim_response = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-release-final"},
        headers=AUTH_HEADERS,
    )
    assert claim_response.status_code == 200
    claim_body = claim_response.json()
    candidate_id = claim_body["row"]["id"]

    release_response = client.post(
        f"/trade_candidates/{candidate_id}/release",
        json={
            "worker_id": "worker-release-final",
            "claim_token": claim_body["claim_token"],
            "execution_status": release_status,
            "execution_note": f"{release_status} by worker",
        },
        headers=AUTH_HEADERS,
    )
    assert release_response.status_code == 200
    body = release_response.json()
    assert body["status"] == "released"
    assert body["execution_status"] == release_status


def test_stale_submitted_row_can_be_reclaimed_after_timeout(client, monkeypatch):
    create_payload = {
        "signal_id": "sig-stale-001",
        "timestamp": "2026-03-10T19:34:00Z",
        "symbol": "BTCUSDT.P",
        "direction": "long",
    }
    create_response = client.post("/trade_candidates", json=create_payload, headers=AUTH_HEADERS)
    assert create_response.status_code == 201

    first_claim = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-a"},
        headers=AUTH_HEADERS,
    )
    assert first_claim.status_code == 200
    first_body = first_claim.json()

    monkeypatch.setenv("SIGNAL_CLAIM_TIMEOUT_SECONDS", "0")

    second_claim = client.post(
        "/trade_candidates/claim_next",
        json={"worker_id": "worker-b"},
        headers=AUTH_HEADERS,
    )
    assert second_claim.status_code == 200
    second_body = second_claim.json()
    assert second_body["status"] == "claimed"
    assert second_body["row"]["id"] == first_body["row"]["id"]
    assert second_body["row"]["claimed_by"] == "worker-b"
    assert second_body["claim_token"] != first_body["claim_token"]

def test_volume_profile_snapshots_recent_filters(client):
    event_writer.insert_volume_profile_snapshot(
        timestamp="2026-03-11T12:00:00Z",
        symbol="BTCUSDT.P",
        timeframe="5m",
        engine_version="v1.0",
        profile_low=50000.0, val=50100.0, poc=50200.0, vah=50300.0, profile_high=50400.0,
        shape_label="b_shape",
        balance_state="balanced",
        source_bar_count=100,
        profile_range=400.0,
        value_area_width=200.0,
        value_area_width_pct=0.5,
        poc_relative=0.5,
        poc_distance_from_mid=0.0
    )

    event_writer.insert_volume_profile_snapshot(
        timestamp="2026-03-11T12:05:00Z",
        symbol="ETHUSDT.P",
        timeframe="5m",
        engine_version="v1.0",
        profile_low=3000.0, val=3010.0, poc=3020.0, vah=3030.0, profile_high=3040.0,
        shape_label="p_shape",
        balance_state="imbalanced_up",
        source_bar_count=100,
        profile_range=40.0,
        value_area_width=20.0,
        value_area_width_pct=0.5,
        poc_relative=0.5,
        poc_distance_from_mid=0.0
    )

    # Unfiltered
    res_all = client.get("/volume_profile_snapshots/recent", headers=AUTH_HEADERS)
    assert res_all.status_code == 200
    assert res_all.json()["count"] >= 2
    
    # Filter by symbol
    res_eth = client.get("/volume_profile_snapshots/recent?symbol=ETHUSDT.P", headers=AUTH_HEADERS)
    assert res_eth.status_code == 200
    body = res_eth.json()
    assert body["count"] == 1
    assert body["rows"][0]["symbol"] == "ETHUSDT.P"
    assert body["rows"][0]["balance_state"] == "imbalanced_up"


def test_volume_profile_snapshots_recent_returns_vp_interaction_fields(client):
    event_writer.insert_volume_profile_snapshot(
        timestamp="2026-03-11T12:10:00Z",
        symbol="VPROOFUSDT.P",
        timeframe="1m",
        engine_version="v1.0",
        profile_low=100.0,
        val=105.0,
        poc=110.0,
        vah=115.0,
        profile_high=120.0,
        shape_label="d_shape",
        balance_state="balanced",
        source_bar_count=50,
        profile_range=20.0,
        value_area_width=10.0,
        value_area_width_pct=0.5,
        poc_relative=0.5,
        poc_distance_from_mid=0.0,
        close_position_in_profile=0.85,
        distance_to_poc=2.0,
        distance_to_vah=3.0,
        distance_to_val=8.0,
        distance_to_poc_pct=0.1,
        distance_to_vah_pct=0.15,
        distance_to_val_pct=0.4,
        inside_value_area=1,
        above_vah=0,
        below_val=0,
    )

    response = client.get(
        "/volume_profile_snapshots/recent?symbol=VPROOFUSDT.P&timeframe=1m",
        headers=AUTH_HEADERS,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1

    row = body["rows"][0]
    assert row["close_position_in_profile"] == pytest.approx(0.85)
    assert row["distance_to_poc"] == pytest.approx(2.0)
    assert row["distance_to_vah"] == pytest.approx(3.0)
    assert row["distance_to_val"] == pytest.approx(8.0)
    assert row["distance_to_poc_pct"] == pytest.approx(0.1)
    assert row["distance_to_vah_pct"] == pytest.approx(0.15)
    assert row["distance_to_val_pct"] == pytest.approx(0.4)
    assert row["inside_value_area"] == 1
    assert row["above_vah"] == 0
    assert row["below_val"] == 0
