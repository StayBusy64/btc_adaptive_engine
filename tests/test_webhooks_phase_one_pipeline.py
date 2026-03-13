import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import api_server
from backend import event_writer
from backend.api_server import app

SIGNAL_KEY = "test-signal-key"
AUTH_HEADERS = {"X-SIGNAL-KEY": SIGNAL_KEY}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_phase_one_pipeline.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", SIGNAL_KEY)
    monkeypatch.setenv("SIGNAL_CLAIM_TIMEOUT_SECONDS", "60")

    with TestClient(app) as test_client:
        yield test_client


def _build_alert_payload() -> dict:
    return {
        "source": "tradingview",
        "symbol": "BTCUSDT",
        "timeframe": "1",
        "side": "buy",
        "signal_name": "vp_breakout_long",
        "strategy_id": "smart_algo_v1",
        "score": 4,
        "bar_time": "2026-03-11T13:05:00Z",
        "price": 84250.5,
        "atr": 185.2,
        "volume_ratio": 1.42,
        "metadata": {
            "confluence": "strong",
            "session": "london_ny_overlap",
        },
    }


def test_webhooks_tradingview_phase_one_pipeline_persists_chain(client):
    payload = _build_alert_payload()

    response = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)

    assert response.status_code == 201
    body = response.json()

    assert body["status"] == "processed"
    assert body["mode"] == "simulated"
    assert body["raw_event_duplicate"] is False
    assert body["normalized_duplicate"] is False
    assert body["strategy_duplicate"] is False
    assert body["risk_duplicate"] is False
    assert body["execution_duplicate"] is False

    # Empty analytics history should force defer->deny->blocked for Alpha dry-run intake.
    assert body["strategy_decision"]["decision"] == "defer"
    assert body["strategy_decision"]["monitor_status"] == "empty"
    assert body["risk_decision"]["risk_decision"] == "deny"
    assert body["execution_request"]["execution_status"] == "blocked"
    assert body["order"]["status"] == "rejected"
    assert body["order_duplicate"] is False
    assert body["fill"] is None
    assert body["fill_duplicate"] is None
    assert body["position_update"] is None

    with event_writer.get_connection() as conn:
        raw_row = conn.execute(
            "SELECT * FROM raw_webhook_events WHERE event_id = ? LIMIT 1",
            (body["event_id"],),
        ).fetchone()
        normalized_row = conn.execute(
            "SELECT * FROM normalized_signals WHERE normalized_id = ? LIMIT 1",
            (body["normalized_signal_id"],),
        ).fetchone()
        strategy_row = conn.execute(
            "SELECT * FROM strategy_decisions WHERE strategy_decision_id = ? LIMIT 1",
            (body["strategy_decision_id"],),
        ).fetchone()
        risk_row = conn.execute(
            "SELECT * FROM risk_events WHERE risk_event_id = ? LIMIT 1",
            (body["risk_event_id"],),
        ).fetchone()
        execution_row = conn.execute(
            "SELECT * FROM execution_requests WHERE execution_request_id = ? LIMIT 1",
            (body["execution_request_id"],),
        ).fetchone()
        broker_order_row = conn.execute(
            "SELECT * FROM broker_orders WHERE execution_request_id = ? LIMIT 1",
            (body["execution_request_id"],),
        ).fetchone()
        fill_count = conn.execute("SELECT COUNT(*) AS c FROM fills").fetchone()["c"]
        open_position_count = conn.execute("SELECT COUNT(*) AS c FROM positions WHERE status = 'open'").fetchone()["c"]

    assert raw_row is not None
    assert normalized_row is not None
    assert strategy_row is not None
    assert risk_row is not None
    assert execution_row is not None
    assert broker_order_row is not None
    assert broker_order_row["status"] == "rejected"
    assert int(fill_count) == 0
    assert int(open_position_count) == 0

    assert normalized_row["symbol"] == "BTCUSDT"
    assert normalized_row["timeframe"] == "1m"
    assert normalized_row["side"] == "long"

    raw_payload = json.loads(raw_row["payload_json"])
    assert raw_payload["signal_name"] == "vp_breakout_long"


def test_webhooks_tradingview_phase_one_is_idempotent_on_duplicate_payload(client):
    payload = _build_alert_payload()

    first = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)
    second = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)

    assert first.status_code == 201
    assert second.status_code == 201

    first_body = first.json()
    second_body = second.json()

    assert second_body["raw_event_duplicate"] is True
    assert second_body["normalized_duplicate"] is True
    assert second_body["strategy_duplicate"] is True
    assert second_body["risk_duplicate"] is True
    assert second_body["execution_duplicate"] is True
    assert second_body["order_duplicate"] is True
    assert second_body["fill"] is None
    assert second_body["fill_duplicate"] is None

    assert second_body["event_id"] == first_body["event_id"]
    assert second_body["normalized_signal_id"] == first_body["normalized_signal_id"]
    assert second_body["strategy_decision_id"] == first_body["strategy_decision_id"]
    assert second_body["risk_event_id"] == first_body["risk_event_id"]
    assert second_body["execution_request_id"] == first_body["execution_request_id"]

    with event_writer.get_connection() as conn:
        raw_count = conn.execute("SELECT COUNT(*) AS c FROM raw_webhook_events").fetchone()["c"]
        normalized_count = conn.execute("SELECT COUNT(*) AS c FROM normalized_signals").fetchone()["c"]
        strategy_count = conn.execute("SELECT COUNT(*) AS c FROM strategy_decisions").fetchone()["c"]
        risk_count = conn.execute("SELECT COUNT(*) AS c FROM risk_events").fetchone()["c"]
        execution_count = conn.execute("SELECT COUNT(*) AS c FROM execution_requests").fetchone()["c"]
        order_count = conn.execute("SELECT COUNT(*) AS c FROM broker_orders").fetchone()["c"]
        fill_count = conn.execute("SELECT COUNT(*) AS c FROM fills").fetchone()["c"]
        position_count = conn.execute("SELECT COUNT(*) AS c FROM positions").fetchone()["c"]

    assert int(raw_count) == 1
    assert int(normalized_count) == 1
    assert int(strategy_count) == 1
    assert int(risk_count) == 1
    assert int(execution_count) == 1
    assert int(order_count) == 1
    assert int(fill_count) == 0
    assert int(position_count) == 0


def test_phase_one_actionable_signal_creates_filled_order_fill_and_open_position(client, monkeypatch):
    monkeypatch.setattr(
        api_server,
        "execution_outcomes_vp_policy_reason_monitor",
        lambda **_: {
            "monitor_status": "healthy",
            "top_quality_score": 2.0,
            "bottom_quality_score": 0.8,
            "quality_spread": 1.2,
            "best_count": 5,
            "worst_count": 5,
            "count": 5,
            "rows": [],
        },
    )

    payload = _build_alert_payload()
    payload["score"] = 6
    payload["price"] = 85000.0

    response = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)
    assert response.status_code == 201
    body = response.json()

    assert body["execution_request"]["execution_status"] == "ready_simulated"
    assert body["order"]["status"] == "filled"
    assert body["fill"] is not None
    assert body["fill"]["fill_status"] == "filled"
    assert body["fill_duplicate"] is False
    assert body["position_update"] is not None
    assert body["position_update"]["status"] == "opened"
    assert body["position_update"]["position"]["status"] == "open"
    assert body["position_update"]["position"]["side"] == "long"

    orders_response = client.get(f"/orders/recent?event_id={body['event_id']}")
    fills_response = client.get(f"/fills/recent?event_id={body['event_id']}")
    positions_open_response = client.get("/positions/open?symbol=BTCUSDT")

    assert orders_response.status_code == 200
    assert fills_response.status_code == 200
    assert positions_open_response.status_code == 200

    orders_rows = orders_response.json()["rows"]
    fills_rows = fills_response.json()["rows"]
    positions_rows = positions_open_response.json()["rows"]

    assert len(orders_rows) == 1
    assert len(fills_rows) == 1
    assert len(positions_rows) == 1

    assert orders_rows[0]["event_id"] == body["event_id"]
    assert orders_rows[0]["execution_request_id"] == body["execution_request_id"]
    assert fills_rows[0]["event_id"] == body["event_id"]
    assert fills_rows[0]["execution_request_id"] == body["execution_request_id"]
    assert fills_rows[0]["order_id"] == orders_rows[0]["order_id"]


def test_phase_one_actionable_duplicate_replay_stays_idempotent_for_order_fill_position(client, monkeypatch):
    monkeypatch.setattr(
        api_server,
        "execution_outcomes_vp_policy_reason_monitor",
        lambda **_: {
            "monitor_status": "healthy",
            "top_quality_score": 1.9,
            "bottom_quality_score": 0.9,
            "quality_spread": 1.0,
            "best_count": 3,
            "worst_count": 3,
            "count": 3,
            "rows": [],
        },
    )

    payload = _build_alert_payload()
    payload["score"] = 7
    payload["price"] = 85111.0

    first = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)
    second = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)

    assert first.status_code == 201
    assert second.status_code == 201

    second_body = second.json()
    assert second_body["order_duplicate"] is True
    assert second_body["fill_duplicate"] is True
    assert second_body["position_update"] is None

    with event_writer.get_connection() as conn:
        order_count = conn.execute("SELECT COUNT(*) AS c FROM broker_orders").fetchone()["c"]
        fill_count = conn.execute("SELECT COUNT(*) AS c FROM fills").fetchone()["c"]
        open_positions = conn.execute("SELECT COUNT(*) AS c FROM positions WHERE status = 'open'").fetchone()["c"]

    assert int(order_count) == 1
    assert int(fill_count) == 1
    assert int(open_positions) == 1


def test_paper_execution_recent_surfaces_support_side_and_status_filters(client, monkeypatch):
    monkeypatch.setattr(
        api_server,
        "execution_outcomes_vp_policy_reason_monitor",
        lambda **_: {
            "monitor_status": "healthy",
            "top_quality_score": 2.1,
            "bottom_quality_score": 1.0,
            "quality_spread": 1.1,
            "best_count": 4,
            "worst_count": 4,
            "count": 4,
            "rows": [],
        },
    )

    payload_long = _build_alert_payload()
    payload_long["symbol"] = "BTCUSDT"
    payload_long["side"] = "buy"
    payload_long["score"] = 6
    payload_long["price"] = 86000.0

    payload_short = _build_alert_payload()
    payload_short["symbol"] = "ETHUSDT"
    payload_short["side"] = "sell"
    payload_short["score"] = 5
    payload_short["price"] = 3200.0
    payload_short["bar_time"] = "2026-03-11T13:06:00Z"

    first = client.post("/webhooks/tradingview", json=payload_long, headers=AUTH_HEADERS)
    second = client.post("/webhooks/tradingview", json=payload_short, headers=AUTH_HEADERS)

    assert first.status_code == 201
    assert second.status_code == 201

    orders_short = client.get("/orders/recent?side=sell&status=filled")
    fills_short = client.get("/fills/recent?side=sell")
    positions_short = client.get("/positions/open?side=sell")
    positions_recent_open = client.get("/positions/recent?status=open")

    assert orders_short.status_code == 200
    assert fills_short.status_code == 200
    assert positions_short.status_code == 200
    assert positions_recent_open.status_code == 200

    orders_rows = orders_short.json()["rows"]
    fills_rows = fills_short.json()["rows"]
    positions_rows = positions_short.json()["rows"]
    positions_recent_rows = positions_recent_open.json()["rows"]

    assert len(orders_rows) == 1
    assert orders_rows[0]["symbol"] == "ETHUSDT"
    assert orders_rows[0]["side"] == "short"
    assert orders_rows[0]["status"] == "filled"

    assert len(fills_rows) == 1
    assert fills_rows[0]["symbol"] == "ETHUSDT"
    assert fills_rows[0]["side"] == "short"

    assert len(positions_rows) == 1
    assert positions_rows[0]["symbol"] == "ETHUSDT"
    assert positions_rows[0]["side"] == "short"
    assert positions_rows[0]["status"] == "open"

    assert len(positions_recent_rows) == 2


def test_webhooks_tradingview_phase_one_rejects_malformed_payload(client):
    malformed_payload = {
        "source": "tradingview",
        "timeframe": "1m",
        "side": "buy",
    }

    response = client.post("/webhooks/tradingview", json=malformed_payload, headers=AUTH_HEADERS)

    assert response.status_code == 422


def test_webhooks_tradingview_phase_one_rejects_unknown_side_alias(client):
    payload = _build_alert_payload()
    payload["side"] = "moonshot"

    response = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)

    assert response.status_code == 422


def test_phase_one_preserves_rich_webhook_stats_in_normalized_features(client):
    payload = _build_alert_payload()
    payload.update(
        {
            "open": 84210.0,
            "high": 84320.0,
            "low": 84180.0,
            "close": 84255.0,
            "volume": 1525.0,
            "fast_ema": 84240.0,
            "slow_ema": 84205.0,
            "rsi": 57.4,
            "bar_index": 456789,
            "signal_family": "continuation",
        }
    )

    response = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)

    assert response.status_code == 201
    normalized_signal_id = response.json()["normalized_signal_id"]

    with event_writer.get_connection() as conn:
        normalized_row = conn.execute(
            "SELECT features_json FROM normalized_signals WHERE normalized_id = ? LIMIT 1",
            (normalized_signal_id,),
        ).fetchone()

    assert normalized_row is not None

    features = json.loads(normalized_row["features_json"])
    assert features["open"] == pytest.approx(84210.0)
    assert features["high"] == pytest.approx(84320.0)
    assert features["low"] == pytest.approx(84180.0)
    assert features["close"] == pytest.approx(84255.0)
    assert features["volume"] == pytest.approx(1525.0)
    assert features["fast_ema"] == pytest.approx(84240.0)
    assert features["slow_ema"] == pytest.approx(84205.0)
    assert features["rsi"] == pytest.approx(57.4)
    assert features["bar_index"] == pytest.approx(456789.0)
    assert features["signal_family"] == "continuation"


def test_phase_one_market_bias_can_downgrade_strategy_and_resize_risk(client, monkeypatch):
    monkeypatch.setattr(
        api_server,
        "execution_outcomes_vp_policy_reason_monitor",
        lambda **_: {
            "monitor_status": "healthy",
            "top_quality_score": 2.0,
            "bottom_quality_score": 0.8,
            "quality_spread": 1.2,
            "best_count": 5,
            "worst_count": 5,
            "count": 5,
            "rows": [],
        },
    )
    monkeypatch.setattr(
        api_server,
        "compute_market_bias_preview_for_normalized_signal",
        lambda **_: {
            "signal_id": "sig-preview-1",
            "computed_at": "2026-03-12T12:00:00+00:00",
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "side": "long",
            "signal_name": "vp_breakout_long",
            "bucket_key": "vp_breakout_long|long|tf:1m|rsi:gt60|ema:large|vol:high|hour:h13",
            "sample_count": 42,
            "reversion_rate": 0.68,
            "continuation_rate": 0.32,
            "reversion_bias": 0.74,
            "continuation_bias": 0.26,
            "confidence": "medium",
            "status": "ready",
            "reasons": ["Historical bucket reversion rate 68.0% across 42 signals"],
            "avg_mfe_pct": 0.21,
            "avg_mae_pct": 0.47,
            "avg_move_3bar_pct": -0.18,
        },
    )

    payload = _build_alert_payload()
    payload["score"] = 6
    payload["price"] = 85000.0

    response = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)

    assert response.status_code == 201
    body = response.json()

    assert body["strategy_decision"]["decision"] == "downgrade"
    assert body["strategy_decision"]["reason_code"] == "historical_reversion_risk"
    assert body["strategy_decision"]["policy_version"] == "vp_policy_v4_bias_bridge"
    assert body["strategy_decision"]["signal_market_bias"]["sample_count"] == 42
    assert body["strategy_decision"]["signal_market_bias"]["reversion_bias"] == pytest.approx(0.74)

    assert body["risk_decision"]["risk_decision"] == "resize"
    assert body["risk_decision"]["reason_code"] == "strategy_downgrade_resized"
    assert body["risk_decision"]["position_size"] == pytest.approx(0.3)


def test_phase_one_recent_query_surfaces_return_pipeline_chain(client):
    payload = _build_alert_payload()
    ingest = client.post("/webhooks/tradingview", json=payload, headers=AUTH_HEADERS)
    assert ingest.status_code == 201
    ingest_body = ingest.json()

    webhooks_response = client.get("/webhooks/events/recent")
    signals_response = client.get("/signals/recent")
    decisions_response = client.get("/decisions/recent")
    execution_response = client.get("/execution/requests/recent")

    assert webhooks_response.status_code == 200
    assert signals_response.status_code == 200
    assert decisions_response.status_code == 200
    assert execution_response.status_code == 200

    webhooks_body = webhooks_response.json()
    signals_body = signals_response.json()
    decisions_body = decisions_response.json()
    execution_body = execution_response.json()

    assert webhooks_body["count"] == 1
    assert signals_body["count"] == 1
    assert decisions_body["count"] == 1
    assert execution_body["count"] == 1

    webhook_row = webhooks_body["rows"][0]
    signal_row = signals_body["rows"][0]
    decision_row = decisions_body["rows"][0]
    execution_row = execution_body["rows"][0]

    assert webhook_row["event_id"] == ingest_body["event_id"]
    assert webhook_row["payload_json"]["symbol"] == "BTCUSDT"

    assert signal_row["normalized_id"] == ingest_body["normalized_signal_id"]
    assert signal_row["event_id"] == ingest_body["event_id"]
    assert signal_row["timeframe"] == "1m"
    assert signal_row["side"] == "long"
    assert signal_row["features_json"]["atr"] == pytest.approx(185.2)

    assert decision_row["strategy_decision_id"] == ingest_body["strategy_decision_id"]
    assert decision_row["risk_event_id"] == ingest_body["risk_event_id"]
    assert decision_row["event_id"] == ingest_body["event_id"]
    assert decision_row["strategy_decision"] == "defer"
    assert decision_row["risk_decision"] == "deny"
    assert isinstance(decision_row["decision_json"], dict)
    assert isinstance(decision_row["risk_json"], dict)

    assert execution_row["execution_request_id"] == ingest_body["execution_request_id"]
    assert execution_row["event_id"] == ingest_body["event_id"]
    assert execution_row["mode"] == "simulated"
    assert execution_row["execution_status"] == "blocked"
    assert isinstance(execution_row["request_json"], dict)


def test_phase_one_recent_query_surfaces_support_event_id_and_side_filters(client):
    payload_a = _build_alert_payload()
    payload_b = _build_alert_payload()
    payload_b["symbol"] = "ETHUSDT"
    payload_b["side"] = "sell"
    payload_b["signal_name"] = "vp_breakout_short"
    payload_b["strategy_id"] = "smart_algo_v2"
    payload_b["bar_time"] = "2026-03-11T13:06:00Z"
    payload_b["price"] = 3123.4
    payload_b["atr"] = 22.5

    first = client.post("/webhooks/tradingview", json=payload_a, headers=AUTH_HEADERS)
    second = client.post("/webhooks/tradingview", json=payload_b, headers=AUTH_HEADERS)

    assert first.status_code == 201
    assert second.status_code == 201

    target_event_id = second.json()["event_id"]

    webhooks_filtered = client.get(f"/webhooks/events/recent?event_id={target_event_id}")
    signals_filtered = client.get(f"/signals/recent?event_id={target_event_id}")
    decisions_filtered = client.get(f"/decisions/recent?event_id={target_event_id}")
    execution_filtered = client.get(f"/execution/requests/recent?event_id={target_event_id}")
    signals_side_filtered = client.get("/signals/recent?side=sell")
    execution_side_filtered = client.get("/execution/requests/recent?side=sell")

    assert webhooks_filtered.status_code == 200
    assert signals_filtered.status_code == 200
    assert decisions_filtered.status_code == 200
    assert execution_filtered.status_code == 200
    assert signals_side_filtered.status_code == 200
    assert execution_side_filtered.status_code == 200

    assert webhooks_filtered.json()["count"] == 1
    assert signals_filtered.json()["count"] == 1
    assert decisions_filtered.json()["count"] == 1
    assert execution_filtered.json()["count"] == 1

    assert webhooks_filtered.json()["rows"][0]["event_id"] == target_event_id
    assert signals_filtered.json()["rows"][0]["event_id"] == target_event_id
    assert decisions_filtered.json()["rows"][0]["event_id"] == target_event_id
    assert execution_filtered.json()["rows"][0]["event_id"] == target_event_id

    assert signals_side_filtered.json()["count"] == 1
    assert signals_side_filtered.json()["rows"][0]["side"] == "short"
    assert execution_side_filtered.json()["count"] == 1
    assert execution_side_filtered.json()["rows"][0]["side"] == "short"
