import csv
import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import event_writer
from backend.api_server import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_execution_journal.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", "test-signal-key")
    monkeypatch.setenv("SIGNAL_CLAIM_TIMEOUT_SECONDS", "60")
    monkeypatch.setenv("OUTCOME_WIN_THRESHOLD_PCT", "0.10")
    monkeypatch.setenv("OUTCOME_LOSS_THRESHOLD_PCT", "-0.10")

    with TestClient(app) as test_client:
        yield test_client


def _insert_timeline_candidate(
    *,
    signal_id: str,
    symbol: str = "BTCUSDT.P",
    direction: str = "long",
    entry_price: float = 82000.0,
    confidence: float = 0.8,
    setup_family: str = "momentum",
    timestamp: str = "2026-03-10T19:00:00Z",
    strategy: str = "adaptive-v2",
    source: str = "worker-test",
) -> int:
    return event_writer.insert_trade_candidate(
        {
            "signal_id": signal_id,
            "timestamp": timestamp,
            "symbol": symbol,
            "direction": direction,
            "entry_price": entry_price,
            "confidence": confidence,
            "setup_family": setup_family,
            "payload_json": {
                "strategy": strategy,
                "source": source,
                "event_type": "continuation",
                "derived_from_event": True,
            },
        }
    )


def _insert_outcome_with_candidate_context(
    *,
    journal_id: int,
    signal_id: str,
    worker_id: str,
    symbol: str,
    direction: str,
    strategy: str | None,
    source: str | None,
    setup_family: str | None,
    outcome_status: str,
    pnl_points: float | None,
    pnl_pct: float | None,
    evaluated_at: str,
    entry_price: float = 1000.0,
):
    payload = {
        "event_type": "continuation",
        "derived_from_event": True,
    }
    if strategy is not None:
        payload["strategy"] = strategy
    if source is not None:
        payload["source"] = source

    candidate_id = event_writer.insert_trade_candidate(
        {
            "signal_id": signal_id,
            "timestamp": "2026-03-11T04:00:00Z",
            "symbol": symbol,
            "direction": direction,
            "entry_price": entry_price,
            "confidence": 0.8,
            "setup_family": setup_family,
            "payload_json": payload,
        }
    )

    exit_price = entry_price + pnl_points if pnl_points is not None else None

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=journal_id,
            candidate_id=candidate_id,
            signal_id=signal_id,
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            reference_timestamp="2026-03-11T04:00:00Z",
            evaluation_window_minutes=15,
            outcome_status=outcome_status,
            exit_price=exit_price,
            pnl_points=pnl_points,
            pnl_pct=pnl_pct,
            evaluated_at=evaluated_at,
        )
    )


def _assert_vp_reason_rank_rows(actual_rows, expected_rows):
    assert len(actual_rows) == len(expected_rows)

    for actual_row, expected_row in zip(actual_rows, expected_rows):
        for expected_key, expected_value in expected_row.items():
            assert actual_row[expected_key] == expected_value

        assert "stdev_pnl" in actual_row
        assert "pnl_ci_low" in actual_row
        assert "pnl_ci_high" in actual_row


def _seed_cohort_outcomes_dataset() -> None:
    _insert_outcome_with_candidate_context(
        journal_id=9501,
        signal_id="sig-cohort-v2-winner",
        worker_id="worker-alpha",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="adaptive-v2",
        source="scanner-a",
        setup_family="momentum",
        outcome_status="evaluated",
        pnl_points=20.0,
        pnl_pct=0.20,
        evaluated_at="2026-03-11T04:01:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9502,
        signal_id="sig-cohort-v2-scratch",
        worker_id="worker-alpha",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="adaptive-v2",
        source="scanner-a",
        setup_family="momentum",
        outcome_status="evaluated",
        pnl_points=-2.0,
        pnl_pct=-0.05,
        evaluated_at="2026-03-11T04:02:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9503,
        signal_id="sig-cohort-v3-loser",
        worker_id="worker-beta",
        symbol="ETHUSDT.P",
        direction="short",
        strategy="adaptive-v3",
        source="scanner-b",
        setup_family="reversal",
        outcome_status="evaluated",
        pnl_points=-15.0,
        pnl_pct=-0.20,
        evaluated_at="2026-03-11T04:03:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9504,
        signal_id="sig-cohort-v3-winner",
        worker_id="worker-beta",
        symbol="ETHUSDT.P",
        direction="short",
        strategy="adaptive-v3",
        source="scanner-b",
        setup_family="reversal",
        outcome_status="evaluated",
        pnl_points=8.0,
        pnl_pct=0.15,
        evaluated_at="2026-03-11T04:04:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9505,
        signal_id="sig-cohort-unknown",
        worker_id="worker-beta",
        symbol="ETHUSDT.P",
        direction="long",
        strategy=None,
        source=None,
        setup_family=None,
        outcome_status="insufficient_data",
        pnl_points=None,
        pnl_pct=None,
        evaluated_at="2026-03-11T04:05:00Z",
    )


def test_execution_journal_recent_returns_rows(client):
    event_writer.insert_execution_journal_entry(
        candidate_id=1,
        signal_id="sig-1",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        execution_note="first",
        confidence=0.8,
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=82000.0,
        metadata_json={"tag": "first"},
        created_at="2026-03-10T19:00:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=2,
        signal_id="sig-2",
        worker_id="worker-b",
        action="simulation_decision",
        execution_status="skipped",
        execution_note="second",
        confidence=0.5,
        symbol="ETHUSDT.P",
        direction="short",
        entry_price=2000.0,
        metadata_json={"tag": "second"},
        created_at="2026-03-10T19:01:00Z",
    )

    response = client.get("/execution_journal/recent")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["rows"][0]["candidate_id"] == 2
    assert body["rows"][1]["candidate_id"] == 1


def test_execution_journal_recent_filters_work(client):
    event_writer.insert_execution_journal_entry(
        candidate_id=10,
        signal_id="sig-filter-1",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        execution_note="ok",
        confidence=0.9,
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=82500.0,
        created_at="2026-03-10T19:10:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=11,
        signal_id="sig-filter-2",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="rejected",
        execution_note="missing fields",
        confidence=None,
        symbol="ETHUSDT.P",
        direction="short",
        entry_price=2100.0,
        created_at="2026-03-10T19:11:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=12,
        signal_id="sig-filter-3",
        worker_id="worker-b",
        action="manual_override",
        execution_status="skipped",
        execution_note="manual",
        confidence=0.3,
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=82400.0,
        created_at="2026-03-10T19:12:00Z",
    )

    worker_filtered = client.get("/execution_journal/recent?worker_id=worker-a")
    assert worker_filtered.status_code == 200
    worker_rows = worker_filtered.json()["rows"]
    assert len(worker_rows) == 2
    assert all(row["worker_id"] == "worker-a" for row in worker_rows)

    status_filtered = client.get("/execution_journal/recent?execution_status=rejected")
    assert status_filtered.status_code == 200
    status_rows = status_filtered.json()["rows"]
    assert len(status_rows) == 1
    assert status_rows[0]["execution_status"] == "rejected"

    symbol_action_filtered = client.get("/execution_journal/recent?symbol=BTCUSDT.P&action=manual_override")
    assert symbol_action_filtered.status_code == 200
    symbol_action_rows = symbol_action_filtered.json()["rows"]
    assert len(symbol_action_rows) == 1
    assert symbol_action_rows[0]["candidate_id"] == 12


def test_execution_journal_summary_returns_correct_counts(client):
    event_writer.insert_execution_journal_entry(
        candidate_id=101,
        signal_id="sig-sum-1",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        execution_note="filled",
        created_at="2026-03-10T19:20:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=102,
        signal_id="sig-sum-2",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="skipped",
        execution_note="skipped",
        created_at="2026-03-10T19:21:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=103,
        signal_id="sig-sum-3",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="rejected",
        execution_note="rejected",
        created_at="2026-03-10T19:22:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=104,
        signal_id="sig-sum-4",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="pending_review",
        execution_note="other",
        created_at="2026-03-10T19:23:00Z",
    )

    response = client.get("/execution_journal/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["filled"] == 1
    assert body["skipped"] == 1
    assert body["rejected"] == 1
    assert body["other"] == 1
    assert body["total"] == 4
    assert body["worker_count"] == 1
    assert body["latest_created_at"] == "2026-03-10T19:23:00Z"


def test_execution_journal_timeline_returns_joined_rows(client):
    candidate_id = _insert_timeline_candidate(
        signal_id="sig-timeline-001",
        timestamp="2026-03-10T19:30:00Z",
        strategy="timeline-strategy",
        source="timeline-source",
    )

    with event_writer.get_connection() as conn:
        conn.execute(
            "UPDATE trade_candidates SET claimed_by = ?, claim_token = ? WHERE id = ?",
            ("worker-timeline", "claim-token-timeline", candidate_id),
        )
        conn.commit()

    event_writer.insert_execution_journal_entry(
        candidate_id=candidate_id,
        signal_id="sig-timeline-001",
        worker_id="worker-timeline",
        action="simulation_decision",
        execution_status="filled",
        execution_note="timeline-note",
        confidence=0.91,
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=82100.0,
        metadata_json={"audit": True},
        created_at="2026-03-10T19:31:00Z",
    )

    response = client.get("/execution_journal/timeline")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    row = body["rows"][0]
    assert row["journal_id"] >= 1
    assert row["candidate_id"] == candidate_id
    assert row["signal_id"] == "sig-timeline-001"
    assert row["worker_id"] == "worker-timeline"
    assert row["strategy"] == "timeline-strategy"
    assert row["source"] == "timeline-source"
    assert row["setup_family"] == "momentum"
    assert row["candidate_timestamp"] == "2026-03-10T19:30:00Z"
    assert row["claimed_by"] == "worker-timeline"
    assert row["claim_token"] == "claim-token-timeline"


def test_execution_journal_timeline_filters_work(client):
    candidate_a = _insert_timeline_candidate(
        signal_id="sig-timeline-filter-a",
        symbol="BTCUSDT.P",
        direction="long",
        timestamp="2026-03-10T20:00:00Z",
    )
    candidate_b = _insert_timeline_candidate(
        signal_id="sig-timeline-filter-b",
        symbol="ETHUSDT.P",
        direction="short",
        timestamp="2026-03-10T20:01:00Z",
    )

    event_writer.insert_execution_journal_entry(
        candidate_id=candidate_a,
        signal_id="sig-timeline-filter-a",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        execution_note="a",
        confidence=0.82,
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=82200.0,
        created_at="2026-03-10T20:00:30Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=candidate_b,
        signal_id="sig-timeline-filter-b",
        worker_id="worker-b",
        action="simulation_decision",
        execution_status="rejected",
        execution_note="b",
        confidence=None,
        symbol="ETHUSDT.P",
        direction="short",
        entry_price=2020.0,
        created_at="2026-03-10T20:01:30Z",
    )

    worker_filtered = client.get("/execution_journal/timeline?worker_id=worker-a")
    assert worker_filtered.status_code == 200
    worker_rows = worker_filtered.json()["rows"]
    assert len(worker_rows) == 1
    assert worker_rows[0]["worker_id"] == "worker-a"

    status_symbol_filtered = client.get("/execution_journal/timeline?execution_status=rejected&symbol=ETHUSDT.P")
    assert status_symbol_filtered.status_code == 200
    status_rows = status_symbol_filtered.json()["rows"]
    assert len(status_rows) == 1
    assert status_rows[0]["signal_id"] == "sig-timeline-filter-b"

    candidate_filtered = client.get(f"/execution_journal/timeline?candidate_id={candidate_a}")
    assert candidate_filtered.status_code == 200
    candidate_rows = candidate_filtered.json()["rows"]
    assert len(candidate_rows) == 1
    assert candidate_rows[0]["candidate_id"] == candidate_a


def test_execution_journal_export_csv_returns_expected_headers_and_rows(client):
    candidate_id = _insert_timeline_candidate(
        signal_id="sig-export-001",
        timestamp="2026-03-10T20:10:00Z",
        strategy="export-strategy",
        source="export-source",
    )

    event_writer.insert_execution_journal_entry(
        candidate_id=candidate_id,
        signal_id="sig-export-001",
        worker_id="worker-export",
        action="simulation_decision",
        execution_status="skipped",
        execution_note="export-note",
        confidence=0.41,
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=82300.0,
        metadata_json={"csv": True},
        created_at="2026-03-10T20:10:30Z",
    )

    response = client.get("/execution_journal/export.csv?worker_id=worker-export")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    text = response.text
    assert "journal_id,candidate_id,signal_id,worker_id,action,execution_status" in text

    parsed_rows = list(csv.DictReader(io.StringIO(text)))
    assert len(parsed_rows) == 1
    assert parsed_rows[0]["worker_id"] == "worker-export"
    assert parsed_rows[0]["signal_id"] == "sig-export-001"
    assert parsed_rows[0]["execution_status"] == "skipped"


def test_execution_journal_analytics_returns_correct_metrics(client):
    event_writer.insert_execution_journal_entry(
        candidate_id=201,
        signal_id="sig-analytics-1",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        execution_note="a",
        confidence=0.9,
        symbol="BTCUSDT.P",
        direction="long",
        created_at="2026-03-10T20:00:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=202,
        signal_id="sig-analytics-2",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="skipped",
        execution_note="b",
        confidence=0.5,
        symbol="BTCUSDT.P",
        direction="short",
        created_at="2026-03-10T20:01:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=203,
        signal_id="sig-analytics-3",
        worker_id="worker-b",
        action="simulation_decision",
        execution_status="rejected",
        execution_note="c",
        confidence=None,
        symbol="ETHUSDT.P",
        direction="long",
        created_at="2026-03-10T20:02:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=204,
        signal_id="sig-analytics-4",
        worker_id="worker-b",
        action="simulation_decision",
        execution_status="filled",
        execution_note="d",
        confidence=0.8,
        symbol="ETHUSDT.P",
        direction="short",
        created_at="2026-03-10T20:03:00Z",
    )

    response = client.get("/execution_journal/analytics")

    assert response.status_code == 200
    body = response.json()
    assert body["total_decisions"] == 4
    assert body["filled_count"] == 2
    assert body["skipped_count"] == 1
    assert body["rejected_count"] == 1
    assert body["fill_rate"] == pytest.approx(0.5)
    assert body["skip_rate"] == pytest.approx(0.25)
    assert body["reject_rate"] == pytest.approx(0.25)
    assert body["avg_confidence"] == pytest.approx((0.9 + 0.5 + 0.8) / 3)
    assert body["avg_confidence_filled"] == pytest.approx(0.85)
    assert body["avg_confidence_skipped"] == pytest.approx(0.5)
    assert body["avg_confidence_rejected"] is None
    assert body["by_symbol"] == {"BTCUSDT.P": 2, "ETHUSDT.P": 2}
    assert body["by_worker"] == {"worker-a": 2, "worker-b": 2}
    assert body["latest_created_at"] == "2026-03-10T20:03:00Z"


def test_execution_journal_analytics_filters_work(client):
    event_writer.insert_execution_journal_entry(
        candidate_id=301,
        signal_id="sig-analytics-filter-1",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        confidence=0.9,
        symbol="BTCUSDT.P",
        created_at="2026-03-10T21:00:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=302,
        signal_id="sig-analytics-filter-2",
        worker_id="worker-b",
        action="simulation_decision",
        execution_status="rejected",
        confidence=0.2,
        symbol="ETHUSDT.P",
        created_at="2026-03-10T21:01:00Z",
    )

    response = client.get(
        "/execution_journal/analytics?worker_id=worker-a&symbol=BTCUSDT.P&since=2026-03-10T20:59:00Z&until=2026-03-10T21:00:30Z"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total_decisions"] == 1
    assert body["filled_count"] == 1
    assert body["by_worker"] == {"worker-a": 1}
    assert body["by_symbol"] == {"BTCUSDT.P": 1}


def test_execution_journal_daily_rollup_returns_grouped_rows(client):
    event_writer.insert_execution_journal_entry(
        candidate_id=401,
        signal_id="sig-rollup-1",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="filled",
        symbol="BTCUSDT.P",
        created_at="2026-03-10T10:00:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=402,
        signal_id="sig-rollup-2",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="skipped",
        symbol="BTCUSDT.P",
        created_at="2026-03-10T11:00:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=403,
        signal_id="sig-rollup-3",
        worker_id="worker-a",
        action="simulation_decision",
        execution_status="rejected",
        symbol="BTCUSDT.P",
        created_at="2026-03-11T09:00:00Z",
    )

    response = client.get("/execution_journal/daily_rollup")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["rows"][0]["day"] == "2026-03-11"
    assert body["rows"][0]["total"] == 1
    assert body["rows"][0]["rejected"] == 1
    assert body["rows"][1]["day"] == "2026-03-10"
    assert body["rows"][1]["total"] == 2
    assert body["rows"][1]["filled"] == 1
    assert body["rows"][1]["skipped"] == 1


def test_execution_journal_daily_rollup_csv_returns_expected_headers_and_rows(client):
    event_writer.insert_execution_journal_entry(
        candidate_id=501,
        signal_id="sig-rollup-csv-1",
        worker_id="worker-csv",
        action="simulation_decision",
        execution_status="filled",
        symbol="BTCUSDT.P",
        created_at="2026-03-12T10:00:00Z",
    )
    event_writer.insert_execution_journal_entry(
        candidate_id=502,
        signal_id="sig-rollup-csv-2",
        worker_id="worker-csv",
        action="simulation_decision",
        execution_status="skipped",
        symbol="BTCUSDT.P",
        created_at="2026-03-12T11:00:00Z",
    )

    response = client.get("/execution_journal/daily_rollup.csv?worker_id=worker-csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    text = response.text
    assert "day,total,filled,skipped,rejected" in text

    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) == 1
    assert rows[0]["day"] == "2026-03-12"
    assert rows[0]["total"] == "2"
    assert rows[0]["filled"] == "1"
    assert rows[0]["skipped"] == "1"
    assert rows[0]["rejected"] == "0"


def test_execution_outcomes_recent_returns_rows_and_filters(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9001,
            candidate_id=801,
            signal_id="sig-outcomes-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T22:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82120.0,
            pnl_points=120.0,
            pnl_pct=0.1463414634,
            max_favorable_excursion=140.0,
            max_adverse_excursion=-40.0,
            evaluated_at="2026-03-10T22:16:00Z",
            metadata_json={"tag": "one"},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9002,
            candidate_id=802,
            signal_id="sig-outcomes-2",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T22:01:00Z",
            evaluation_window_minutes=15,
            outcome_status="insufficient_data",
            evaluated_at="2026-03-10T22:17:00Z",
            metadata_json={"reason": "no_price_points"},
        )
    )

    response = client.get("/execution_outcomes/recent")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["rows"][0]["journal_id"] == 9002
    assert body["rows"][1]["journal_id"] == 9001

    filtered_response = client.get("/execution_outcomes/recent?worker_id=worker-a&outcome_status=evaluated")
    assert filtered_response.status_code == 200
    filtered_rows = filtered_response.json()["rows"]
    assert len(filtered_rows) == 1
    assert filtered_rows[0]["signal_id"] == "sig-outcomes-1"


def test_execution_outcomes_recent_exposes_vp_policy_fields_with_realized_results(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9003,
            candidate_id=803,
            signal_id="sig-outcomes-vp-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T22:05:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82120.0,
            pnl_points=120.0,
            pnl_pct=0.1463414634,
            max_favorable_excursion=140.0,
            max_adverse_excursion=-40.0,
            evaluated_at="2026-03-10T22:18:00Z",
            metadata_json={
                "vp_policy_candidate": 1,
                "vp_policy_side": "long",
                "vp_trade_bias_score": 3,
                "vp_policy_reason": "long|continuation_up|long_continuation|high|score=3|candidate=1",
                "tag": "vp-quality",
            },
        )
    )

    response = client.get("/execution_outcomes/recent?signal_id=sig-outcomes-vp-1")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1

    row = body["rows"][0]
    assert row["outcome_status"] == "evaluated"
    assert row["pnl_points"] == pytest.approx(120.0)
    assert row["pnl_pct"] == pytest.approx(0.1463414634)
    assert row["vp_policy_candidate"] == 1
    assert row["vp_policy_side"] == "long"
    assert row["vp_trade_bias_score"] == 3
    assert row["vp_policy_reason"] == "long|continuation_up|long_continuation|high|score=3|candidate=1"


def test_execution_outcomes_vp_policy_summary_returns_exact_metrics(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9010,
            candidate_id=810,
            signal_id="sig-vp-summary-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:05:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=100.0,
            pnl_pct=0.12,
            evaluated_at="2026-03-10T23:20:00Z",
            metadata_json={
                "vp_policy_candidate": 1,
                "vp_policy_side": "long",
                "vp_trade_bias_score": 3,
                "vp_policy_reason": "long|continuation_up|long_continuation|high|score=3|candidate=1",
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9011,
            candidate_id=811,
            signal_id="sig-vp-summary-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:06:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81940.0,
            pnl_points=60.0,
            pnl_pct=0.07,
            evaluated_at="2026-03-10T23:21:00Z",
            metadata_json={
                "vp_policy_candidate": 1,
                "vp_policy_side": "short",
                "vp_trade_bias_score": 2,
                "vp_policy_reason": "short|rotation_down|short_reversion|medium|score=2|candidate=1",
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9012,
            candidate_id=812,
            signal_id="sig-vp-summary-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:07:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2004.0,
            pnl_points=4.0,
            pnl_pct=0.02,
            evaluated_at="2026-03-10T23:22:00Z",
            metadata_json={
                "vp_policy_candidate": 0,
                "vp_policy_side": "",
                "vp_trade_bias_score": 1,
                "vp_policy_reason": "flat|balance|neutral|low|score=1|candidate=0",
            },
        )
    )

    response = client.get("/execution_outcomes/vp_policy_summary")

    assert response.status_code == 200
    body = response.json()
    assert body["total_rows"] == 3
    assert body["candidate_rows"] == 2
    assert body["long_candidate_rows"] == 1
    assert body["short_candidate_rows"] == 1
    assert body["avg_vp_trade_bias_score"] == pytest.approx(2.0)


def test_execution_outcomes_vp_policy_cohorts_returns_grouped_metrics(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9020,
            candidate_id=820,
            signal_id="sig-vp-cohort-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:30:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=100.0,
            pnl_pct=0.12,
            max_favorable_excursion=150.0,
            max_adverse_excursion=-40.0,
            evaluated_at="2026-03-10T23:31:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_trade_bias_score": 3,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9021,
            candidate_id=821,
            signal_id="sig-vp-cohort-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:31:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=-20.0,
            pnl_pct=-0.02,
            max_favorable_excursion=10.0,
            max_adverse_excursion=-60.0,
            evaluated_at="2026-03-10T23:32:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_trade_bias_score": 3,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9022,
            candidate_id=822,
            signal_id="sig-vp-cohort-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:32:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1995.0,
            pnl_points=50.0,
            pnl_pct=0.03,
            max_favorable_excursion=70.0,
            max_adverse_excursion=-15.0,
            evaluated_at="2026-03-10T23:33:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_trade_bias_score": 2,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9023,
            candidate_id=823,
            signal_id="sig-vp-cohort-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:33:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            max_favorable_excursion=40.0,
            max_adverse_excursion=-25.0,
            evaluated_at="2026-03-10T23:34:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_trade_bias_score": 2,
            },
        )
    )

    response = client.get("/execution_outcomes/vp_policy_cohorts")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2

    cohorts = {
        (row["vp_policy_side"], row["vp_trade_bias_score"]): row
        for row in body["rows"]
    }

    long_high = cohorts[("long", 3.0)]
    assert long_high["row_count"] == 2
    assert long_high["avg_pnl"] == pytest.approx(40.0)
    assert long_high["avg_mfe"] == pytest.approx(80.0)
    assert long_high["avg_mae"] == pytest.approx(-50.0)
    assert long_high["direction_correct_rate"] == pytest.approx(1.0)

    short_medium = cohorts[("short", 2.0)]
    assert short_medium["row_count"] == 2
    assert short_medium["avg_pnl"] == pytest.approx(40.0)
    assert short_medium["avg_mfe"] == pytest.approx(55.0)
    assert short_medium["avg_mae"] == pytest.approx(-20.0)
    assert short_medium["direction_correct_rate"] == pytest.approx(0.5)


def test_execution_outcomes_vp_policy_reason_cohorts_returns_grouped_metrics(client):
    long_reason = "long|continuation_up|long_continuation|high|score=3|candidate=1"
    short_reason = "short|rotation_down|short_reversion|medium|score=2|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9030,
            candidate_id=830,
            signal_id="sig-vp-reason-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:40:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=100.0,
            pnl_pct=0.12,
            max_favorable_excursion=150.0,
            max_adverse_excursion=-30.0,
            evaluated_at="2026-03-10T23:41:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": long_reason,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9031,
            candidate_id=831,
            signal_id="sig-vp-reason-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:41:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=-20.0,
            pnl_pct=-0.02,
            max_favorable_excursion=20.0,
            max_adverse_excursion=-50.0,
            evaluated_at="2026-03-10T23:42:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": long_reason,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9032,
            candidate_id=832,
            signal_id="sig-vp-reason-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:42:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1994.0,
            pnl_points=60.0,
            pnl_pct=0.03,
            max_favorable_excursion=80.0,
            max_adverse_excursion=-10.0,
            evaluated_at="2026-03-10T23:43:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": short_reason,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9033,
            candidate_id=833,
            signal_id="sig-vp-reason-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:43:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            max_favorable_excursion=40.0,
            max_adverse_excursion=-25.0,
            evaluated_at="2026-03-10T23:44:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": short_reason,
            },
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_cohorts")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2

    cohorts = {
        row["vp_policy_reason"]: row
        for row in body["rows"]
    }

    long_cohort = cohorts[long_reason]
    assert long_cohort["row_count"] == 2
    assert long_cohort["avg_pnl"] == pytest.approx(40.0)
    assert long_cohort["avg_mfe"] == pytest.approx(85.0)
    assert long_cohort["avg_mae"] == pytest.approx(-40.0)
    assert long_cohort["direction_correct_rate"] == pytest.approx(1.0)

    short_cohort = cohorts[short_reason]
    assert short_cohort["row_count"] == 2
    assert short_cohort["avg_pnl"] == pytest.approx(45.0)
    assert short_cohort["avg_mfe"] == pytest.approx(60.0)
    assert short_cohort["avg_mae"] == pytest.approx(-17.5)
    assert short_cohort["direction_correct_rate"] == pytest.approx(0.5)


def test_execution_outcomes_vp_policy_reason_leaderboard_filters_and_orders_rows(client):
    reason_best = "long|continuation_up|long_continuation|high|score=3|candidate=1"
    reason_tie_lower_correct = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    reason_lower_pnl = "long|rotation_up|long_reversion|medium|score=2|candidate=1"
    reason_excluded = "flat|balance|neutral|low|score=1|candidate=0"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9040,
            candidate_id=840,
            signal_id="sig-vp-leader-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:50:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=60.0,
            pnl_pct=0.07,
            evaluated_at="2026-03-10T23:51:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_best},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9041,
            candidate_id=841,
            signal_id="sig-vp-leader-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:51:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=20.0,
            pnl_pct=0.02,
            evaluated_at="2026-03-10T23:52:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_best},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9042,
            candidate_id=842,
            signal_id="sig-vp-leader-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:52:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1995.0,
            pnl_points=50.0,
            pnl_pct=0.03,
            evaluated_at="2026-03-10T23:53:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_tie_lower_correct},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9043,
            candidate_id=843,
            signal_id="sig-vp-leader-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:53:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-10T23:54:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_tie_lower_correct},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9044,
            candidate_id=844,
            signal_id="sig-vp-leader-5",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:54:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82030.0,
            pnl_points=30.0,
            pnl_pct=0.035,
            evaluated_at="2026-03-10T23:55:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_lower_pnl},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9045,
            candidate_id=845,
            signal_id="sig-vp-leader-6",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:55:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=-10.0,
            pnl_pct=-0.01,
            evaluated_at="2026-03-10T23:56:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_lower_pnl},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9046,
            candidate_id=846,
            signal_id="sig-vp-leader-7",
            worker_id="worker-d",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:56:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82150.0,
            pnl_points=150.0,
            pnl_pct=0.18,
            evaluated_at="2026-03-10T23:57:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_excluded},
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_leaderboard")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 3

    rows = body["rows"]
    assert [row["vp_policy_reason"] for row in rows] == [reason_best, reason_tie_lower_correct, reason_lower_pnl]

    assert rows[0]["row_count"] == 2
    assert rows[0]["avg_pnl"] == pytest.approx(40.0)
    assert rows[0]["direction_correct_rate"] == pytest.approx(1.0)

    assert rows[1]["row_count"] == 2
    assert rows[1]["avg_pnl"] == pytest.approx(40.0)
    assert rows[1]["direction_correct_rate"] == pytest.approx(0.5)

    assert rows[2]["row_count"] == 2
    assert rows[2]["avg_pnl"] == pytest.approx(10.0)
    assert rows[2]["direction_correct_rate"] == pytest.approx(1.0)


def test_execution_outcomes_vp_policy_reason_laggards_filters_and_orders_rows(client):
    reason_worst = "long|failed_breakout|long_reversion|high|score=3|candidate=1"
    reason_tie_lower_correct = "long|rotation_up|long_reversion|medium|score=2|candidate=1"
    reason_tie_higher_correct = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    reason_excluded = "flat|balance|neutral|low|score=1|candidate=0"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9050,
            candidate_id=850,
            signal_id="sig-vp-laggard-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81950.0,
            pnl_points=-50.0,
            pnl_pct=-0.06,
            evaluated_at="2026-03-11T00:01:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_worst},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9051,
            candidate_id=851,
            signal_id="sig-vp-laggard-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:01:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81970.0,
            pnl_points=-30.0,
            pnl_pct=-0.035,
            evaluated_at="2026-03-11T00:02:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_worst},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9052,
            candidate_id=852,
            signal_id="sig-vp-laggard-3",
            worker_id="worker-b",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:02:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81990.0,
            pnl_points=-10.0,
            pnl_pct=-0.012,
            evaluated_at="2026-03-11T00:03:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_tie_lower_correct},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9053,
            candidate_id=853,
            signal_id="sig-vp-laggard-4",
            worker_id="worker-b",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:03:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81970.0,
            pnl_points=-30.0,
            pnl_pct=-0.036,
            evaluated_at="2026-03-11T00:04:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_tie_lower_correct},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9054,
            candidate_id=854,
            signal_id="sig-vp-laggard-5",
            worker_id="worker-c",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:04:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1990.0,
            pnl_points=10.0,
            pnl_pct=0.005,
            evaluated_at="2026-03-11T00:05:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_tie_higher_correct},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9055,
            candidate_id=855,
            signal_id="sig-vp-laggard-6",
            worker_id="worker-c",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:05:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1950.0,
            pnl_points=-50.0,
            pnl_pct=-0.025,
            evaluated_at="2026-03-11T00:06:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_tie_higher_correct},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9056,
            candidate_id=856,
            signal_id="sig-vp-laggard-7",
            worker_id="worker-d",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:06:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81880.0,
            pnl_points=-120.0,
            pnl_pct=-0.14,
            evaluated_at="2026-03-11T00:07:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_excluded},
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_laggards")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 3

    rows = body["rows"]
    assert [row["vp_policy_reason"] for row in rows] == [reason_worst, reason_tie_lower_correct, reason_tie_higher_correct]

    assert rows[0]["row_count"] == 2
    assert rows[0]["avg_pnl"] == pytest.approx(-40.0)
    assert rows[0]["direction_correct_rate"] == pytest.approx(0.0)

    assert rows[1]["row_count"] == 2
    assert rows[1]["avg_pnl"] == pytest.approx(-20.0)
    assert rows[1]["direction_correct_rate"] == pytest.approx(0.5)

    assert rows[2]["row_count"] == 2
    assert rows[2]["avg_pnl"] == pytest.approx(-20.0)
    assert rows[2]["direction_correct_rate"] == pytest.approx(1.0)


def test_execution_outcomes_vp_policy_reason_extremes_returns_leaders_and_laggards(client):
    reason_best = "long|continuation_up|long_continuation|high|score=3|candidate=1"
    reason_mid = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    reason_worst = "long|failed_breakout|long_reversion|high|score=3|candidate=1"
    reason_excluded = "flat|balance|neutral|low|score=1|candidate=0"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9060,
            candidate_id=860,
            signal_id="sig-vp-extreme-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:10:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=60.0,
            pnl_pct=0.07,
            evaluated_at="2026-03-11T00:11:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_best},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9061,
            candidate_id=861,
            signal_id="sig-vp-extreme-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:11:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=20.0,
            pnl_pct=0.02,
            evaluated_at="2026-03-11T00:12:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_best},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9062,
            candidate_id=862,
            signal_id="sig-vp-extreme-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:12:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1995.0,
            pnl_points=50.0,
            pnl_pct=0.03,
            evaluated_at="2026-03-11T00:13:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_mid},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9063,
            candidate_id=863,
            signal_id="sig-vp-extreme-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:13:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-11T00:14:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_mid},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9064,
            candidate_id=864,
            signal_id="sig-vp-extreme-5",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:14:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81950.0,
            pnl_points=-50.0,
            pnl_pct=-0.06,
            evaluated_at="2026-03-11T00:15:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_worst},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9065,
            candidate_id=865,
            signal_id="sig-vp-extreme-6",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:15:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81970.0,
            pnl_points=-30.0,
            pnl_pct=-0.035,
            evaluated_at="2026-03-11T00:16:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_worst},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9066,
            candidate_id=866,
            signal_id="sig-vp-extreme-7",
            worker_id="worker-d",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:16:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82150.0,
            pnl_points=150.0,
            pnl_pct=0.18,
            evaluated_at="2026-03-11T00:17:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_excluded},
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_extremes")

    assert response.status_code == 200
    body = response.json()
    _assert_vp_reason_rank_rows(body["leaders"], [
        {
            "vp_policy_reason": reason_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 1.0,
        },
        {
            "vp_policy_reason": reason_mid,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": reason_worst,
            "row_count": 2,
            "avg_pnl": -40.0,
            "direction_correct_rate": 0.0,
        },
    ])
    _assert_vp_reason_rank_rows(body["laggards"], [
        {
            "vp_policy_reason": reason_worst,
            "row_count": 2,
            "avg_pnl": -40.0,
            "direction_correct_rate": 0.0,
        },
        {
            "vp_policy_reason": reason_mid,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": reason_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 1.0,
        },
    ])


def test_execution_outcomes_vp_policy_reason_extremes_by_score_filters_one_band(client):
    score_two_best = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    score_two_worst = "long|rotation_up|long_reversion|medium|score=2|candidate=1"
    score_three_ignored = "long|continuation_up|long_continuation|high|score=3|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9070,
            candidate_id=870,
            signal_id="sig-vp-score-1",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:20:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1995.0,
            pnl_points=50.0,
            pnl_pct=0.03,
            evaluated_at="2026-03-11T00:21:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_two_best,
                "vp_trade_bias_score": 2,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9071,
            candidate_id=871,
            signal_id="sig-vp-score-2",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:21:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-11T00:22:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_two_best,
                "vp_trade_bias_score": 2,
            },
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9072,
            candidate_id=872,
            signal_id="sig-vp-score-3",
            worker_id="worker-b",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:22:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81990.0,
            pnl_points=-10.0,
            pnl_pct=-0.012,
            evaluated_at="2026-03-11T00:23:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": score_two_worst,
                "vp_trade_bias_score": 2,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9073,
            candidate_id=873,
            signal_id="sig-vp-score-4",
            worker_id="worker-b",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:23:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81970.0,
            pnl_points=-30.0,
            pnl_pct=-0.036,
            evaluated_at="2026-03-11T00:24:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": score_two_worst,
                "vp_trade_bias_score": 2,
            },
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9074,
            candidate_id=874,
            signal_id="sig-vp-score-5",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:24:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=60.0,
            pnl_pct=0.07,
            evaluated_at="2026-03-11T00:25:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": score_three_ignored,
                "vp_trade_bias_score": 3,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9075,
            candidate_id=875,
            signal_id="sig-vp-score-6",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:25:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=20.0,
            pnl_pct=0.02,
            evaluated_at="2026-03-11T00:26:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": score_three_ignored,
                "vp_trade_bias_score": 3,
            },
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2")

    assert response.status_code == 200
    body = response.json()
    _assert_vp_reason_rank_rows(body["leaders"], [
        {
            "vp_policy_reason": score_two_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": score_two_worst,
            "row_count": 2,
            "avg_pnl": -20.0,
            "direction_correct_rate": 0.5,
        },
    ])
    _assert_vp_reason_rank_rows(body["laggards"], [
        {
            "vp_policy_reason": score_two_worst,
            "row_count": 2,
            "avg_pnl": -20.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": score_two_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 0.5,
        },
    ])


def test_execution_outcomes_vp_policy_reason_extremes_by_score_and_side_filters_one_band(client):
    score_two_short_best = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    score_two_short_worst = "short|failed_breakdown|short_continuation|medium|score=2|candidate=1"
    score_two_long_ignored = "long|rotation_up|long_reversion|medium|score=2|candidate=1"
    score_three_short_ignored = "short|continuation_down|short_continuation|high|score=3|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9080,
            candidate_id=880,
            signal_id="sig-vp-side-score-1",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:30:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1995.0,
            pnl_points=50.0,
            pnl_pct=0.03,
            evaluated_at="2026-03-11T00:31:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_two_short_best,
                "vp_trade_bias_score": 2,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9081,
            candidate_id=881,
            signal_id="sig-vp-side-score-2",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:31:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-11T00:32:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_two_short_best,
                "vp_trade_bias_score": 2,
            },
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9082,
            candidate_id=882,
            signal_id="sig-vp-side-score-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:32:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2002.0,
            pnl_points=-20.0,
            pnl_pct=-0.01,
            evaluated_at="2026-03-11T00:33:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_two_short_worst,
                "vp_trade_bias_score": 2,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9083,
            candidate_id=883,
            signal_id="sig-vp-side-score-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:33:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2010.0,
            pnl_points=-10.0,
            pnl_pct=-0.005,
            evaluated_at="2026-03-11T00:34:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_two_short_worst,
                "vp_trade_bias_score": 2,
            },
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9084,
            candidate_id=884,
            signal_id="sig-vp-side-score-5",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:34:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81990.0,
            pnl_points=-10.0,
            pnl_pct=-0.012,
            evaluated_at="2026-03-11T00:35:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": score_two_long_ignored,
                "vp_trade_bias_score": 2,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9085,
            candidate_id=885,
            signal_id="sig-vp-side-score-6",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:35:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81970.0,
            pnl_points=-30.0,
            pnl_pct=-0.036,
            evaluated_at="2026-03-11T00:36:00Z",
            metadata_json={
                "vp_policy_side": "long",
                "vp_policy_reason": score_two_long_ignored,
                "vp_trade_bias_score": 2,
            },
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9086,
            candidate_id=886,
            signal_id="sig-vp-side-score-7",
            worker_id="worker-d",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:36:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1990.0,
            pnl_points=10.0,
            pnl_pct=0.005,
            evaluated_at="2026-03-11T00:37:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_three_short_ignored,
                "vp_trade_bias_score": 3,
            },
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9087,
            candidate_id=887,
            signal_id="sig-vp-side-score-8",
            worker_id="worker-d",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:37:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-11T00:38:00Z",
            metadata_json={
                "vp_policy_side": "short",
                "vp_policy_reason": score_three_short_ignored,
                "vp_trade_bias_score": 3,
            },
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2/short")

    assert response.status_code == 200
    body = response.json()
    _assert_vp_reason_rank_rows(body["leaders"], [
        {
            "vp_policy_reason": score_two_short_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": score_two_short_worst,
            "row_count": 2,
            "avg_pnl": -15.0,
            "direction_correct_rate": 0.5,
        },
    ])
    _assert_vp_reason_rank_rows(body["laggards"], [
        {
            "vp_policy_reason": score_two_short_worst,
            "row_count": 2,
            "avg_pnl": -15.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": score_two_short_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 0.5,
        },
    ])


def test_execution_outcomes_vp_policy_reason_min_count_filters_supported_surfaces(client):
    reason_count_three = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    reason_count_two = "short|failed_breakdown|short_continuation|medium|score=2|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9090,
            candidate_id=890,
            signal_id="sig-vp-min-count-1",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:40:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1995.0,
            pnl_points=50.0,
            pnl_pct=0.03,
            evaluated_at="2026-03-11T00:41:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_count_three, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9091,
            candidate_id=891,
            signal_id="sig-vp-min-count-2",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:41:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2030.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-11T00:42:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_count_three, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9092,
            candidate_id=892,
            signal_id="sig-vp-min-count-3",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:42:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1988.0,
            pnl_points=12.0,
            pnl_pct=0.006,
            evaluated_at="2026-03-11T00:43:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_count_three, "vp_trade_bias_score": 2},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9093,
            candidate_id=893,
            signal_id="sig-vp-min-count-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:43:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2002.0,
            pnl_points=-20.0,
            pnl_pct=-0.01,
            evaluated_at="2026-03-11T00:44:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_count_two, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9094,
            candidate_id=894,
            signal_id="sig-vp-min-count-5",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:44:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2010.0,
            pnl_points=-10.0,
            pnl_pct=-0.005,
            evaluated_at="2026-03-11T00:45:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_count_two, "vp_trade_bias_score": 2},
        )
    )

    leaderboard_default = client.get("/execution_outcomes/vp_policy_reason_leaderboard")
    assert leaderboard_default.status_code == 200
    assert [row["vp_policy_reason"] for row in leaderboard_default.json()["rows"]] == [reason_count_three, reason_count_two]

    leaderboard_filtered = client.get("/execution_outcomes/vp_policy_reason_leaderboard?min_count=3")
    assert leaderboard_filtered.status_code == 200
    _assert_vp_reason_rank_rows(leaderboard_filtered.json()["rows"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])

    laggards_filtered = client.get("/execution_outcomes/vp_policy_reason_laggards?min_count=3")
    assert laggards_filtered.status_code == 200
    _assert_vp_reason_rank_rows(laggards_filtered.json()["rows"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])

    extremes_filtered = client.get("/execution_outcomes/vp_policy_reason_extremes?min_count=3")
    assert extremes_filtered.status_code == 200
    _assert_vp_reason_rank_rows(extremes_filtered.json()["leaders"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])
    _assert_vp_reason_rank_rows(extremes_filtered.json()["laggards"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])

    by_score_filtered = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2?min_count=3")
    assert by_score_filtered.status_code == 200
    _assert_vp_reason_rank_rows(by_score_filtered.json()["leaders"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])
    _assert_vp_reason_rank_rows(by_score_filtered.json()["laggards"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])

    by_score_side_filtered = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2/short?min_count=3")
    assert by_score_side_filtered.status_code == 200
    _assert_vp_reason_rank_rows(by_score_side_filtered.json()["leaders"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])
    _assert_vp_reason_rank_rows(by_score_side_filtered.json()["laggards"], [
        {
            "vp_policy_reason": reason_count_three,
            "row_count": 3,
            "avg_pnl": pytest.approx(30.666666666666668),
            "direction_correct_rate": pytest.approx(2 / 3),
        }
    ])


def test_execution_outcomes_vp_policy_reason_limit_filters_supported_surfaces(client):
    reason_best = "long|continuation_up|long_continuation|high|score=2|candidate=1"
    reason_mid = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    reason_worst = "short|failed_breakdown|short_continuation|medium|score=2|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9095,
            candidate_id=895,
            signal_id="sig-vp-limit-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:45:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82060.0,
            pnl_points=60.0,
            pnl_pct=0.03,
            evaluated_at="2026-03-11T00:46:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_best, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9096,
            candidate_id=896,
            signal_id="sig-vp-limit-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:46:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=20.0,
            pnl_pct=0.01,
            evaluated_at="2026-03-11T00:47:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_best, "vp_trade_bias_score": 2},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9097,
            candidate_id=897,
            signal_id="sig-vp-limit-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:47:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1997.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-11T00:48:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_mid, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9098,
            candidate_id=898,
            signal_id="sig-vp-limit-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:48:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2010.0,
            pnl_points=10.0,
            pnl_pct=0.005,
            evaluated_at="2026-03-11T00:49:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_mid, "vp_trade_bias_score": 2},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9099,
            candidate_id=899,
            signal_id="sig-vp-limit-5",
            worker_id="worker-c",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:49:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2002.0,
            pnl_points=-20.0,
            pnl_pct=-0.01,
            evaluated_at="2026-03-11T00:50:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_worst, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9100,
            candidate_id=900,
            signal_id="sig-vp-limit-6",
            worker_id="worker-c",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:50:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2008.0,
            pnl_points=-10.0,
            pnl_pct=-0.005,
            evaluated_at="2026-03-11T00:51:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_worst, "vp_trade_bias_score": 2},
        )
    )

    leaderboard_limited = client.get("/execution_outcomes/vp_policy_reason_leaderboard?limit=2")
    assert leaderboard_limited.status_code == 200
    _assert_vp_reason_rank_rows(leaderboard_limited.json()["rows"], [
        {
            "vp_policy_reason": reason_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 1.0,
        },
        {
            "vp_policy_reason": reason_mid,
            "row_count": 2,
            "avg_pnl": 20.0,
            "direction_correct_rate": 0.5,
        },
    ])

    laggards_limited = client.get("/execution_outcomes/vp_policy_reason_laggards?limit=2")
    assert laggards_limited.status_code == 200
    _assert_vp_reason_rank_rows(laggards_limited.json()["rows"], [
        {
            "vp_policy_reason": reason_worst,
            "row_count": 2,
            "avg_pnl": -15.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": reason_mid,
            "row_count": 2,
            "avg_pnl": 20.0,
            "direction_correct_rate": 0.5,
        },
    ])

    extremes_limited = client.get("/execution_outcomes/vp_policy_reason_extremes?limit=2")
    assert extremes_limited.status_code == 200
    _assert_vp_reason_rank_rows(extremes_limited.json()["leaders"], [
        {
            "vp_policy_reason": reason_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 1.0,
        },
        {
            "vp_policy_reason": reason_mid,
            "row_count": 2,
            "avg_pnl": 20.0,
            "direction_correct_rate": 0.5,
        },
    ])
    _assert_vp_reason_rank_rows(extremes_limited.json()["laggards"], [
        {
            "vp_policy_reason": reason_worst,
            "row_count": 2,
            "avg_pnl": -15.0,
            "direction_correct_rate": 0.5,
        },
        {
            "vp_policy_reason": reason_mid,
            "row_count": 2,
            "avg_pnl": 20.0,
            "direction_correct_rate": 0.5,
        },
    ])

    by_score_limited = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2?limit=1")
    assert by_score_limited.status_code == 200
    _assert_vp_reason_rank_rows(by_score_limited.json()["leaders"], [
        {
            "vp_policy_reason": reason_best,
            "row_count": 2,
            "avg_pnl": 40.0,
            "direction_correct_rate": 1.0,
        }
    ])
    _assert_vp_reason_rank_rows(by_score_limited.json()["laggards"], [
        {
            "vp_policy_reason": reason_worst,
            "row_count": 2,
            "avg_pnl": -15.0,
            "direction_correct_rate": 0.5,
        }
    ])

    by_score_side_limited = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2/short?limit=1")
    assert by_score_side_limited.status_code == 200
    _assert_vp_reason_rank_rows(by_score_side_limited.json()["leaders"], [
        {
            "vp_policy_reason": reason_mid,
            "row_count": 2,
            "avg_pnl": 20.0,
            "direction_correct_rate": 0.5,
        }
    ])
    _assert_vp_reason_rank_rows(by_score_side_limited.json()["laggards"], [
        {
            "vp_policy_reason": reason_worst,
            "row_count": 2,
            "avg_pnl": -15.0,
            "direction_correct_rate": 0.5,
        }
    ])


def test_execution_outcomes_vp_policy_reason_sort_modes_supported(client):
    reason_pnl_first = "long|continuation_up|long_continuation|high|score=2|candidate=1"
    reason_accuracy_first = "short|rotation_down|short_reversion|medium|score=2|candidate=1"
    reason_last = "long|failed_breakout|long_reversion|medium|score=2|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9101,
            candidate_id=901,
            signal_id="sig-vp-sort-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:52:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82015.0,
            pnl_points=15.0,
            pnl_pct=0.0075,
            evaluated_at="2026-03-11T00:53:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_pnl_first, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9102,
            candidate_id=902,
            signal_id="sig-vp-sort-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:53:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81995.0,
            pnl_points=5.0,
            pnl_pct=0.0025,
            evaluated_at="2026-03-11T00:54:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_pnl_first, "vp_trade_bias_score": 2},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9103,
            candidate_id=903,
            signal_id="sig-vp-sort-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:54:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1991.0,
            pnl_points=9.0,
            pnl_pct=0.0045,
            evaluated_at="2026-03-11T00:55:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_accuracy_first, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9104,
            candidate_id=904,
            signal_id="sig-vp-sort-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:55:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1991.0,
            pnl_points=9.0,
            pnl_pct=0.0045,
            evaluated_at="2026-03-11T00:56:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_accuracy_first, "vp_trade_bias_score": 2},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9105,
            candidate_id=905,
            signal_id="sig-vp-sort-5",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:56:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82018.0,
            pnl_points=8.0,
            pnl_pct=0.004,
            evaluated_at="2026-03-11T00:57:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_last, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9106,
            candidate_id=906,
            signal_id="sig-vp-sort-6",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:57:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82008.0,
            pnl_points=8.0,
            pnl_pct=0.004,
            evaluated_at="2026-03-11T00:58:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_last, "vp_trade_bias_score": 2},
        )
    )

    pnl_sorted = client.get("/execution_outcomes/vp_policy_reason_leaderboard?sort=pnl")
    assert pnl_sorted.status_code == 200
    assert [row["vp_policy_reason"] for row in pnl_sorted.json()["rows"]] == [
        reason_pnl_first,
        reason_accuracy_first,
        reason_last,
    ]

    accuracy_sorted = client.get("/execution_outcomes/vp_policy_reason_leaderboard?sort=accuracy")
    assert accuracy_sorted.status_code == 200
    assert [row["vp_policy_reason"] for row in accuracy_sorted.json()["rows"]] == [
        reason_accuracy_first,
        reason_pnl_first,
        reason_last,
    ]


def test_execution_outcomes_vp_policy_reason_quality_sort_supported(client):
    reason_pnl_first = "long|continuation_up|long_continuation|medium|score=2|candidate=1"
    reason_quality_first = "short|rotation_down|short_reversion|high|score=2|candidate=1"
    reason_last = "long|failed_breakout|long_reversion|low|score=2|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9107,
            candidate_id=907,
            signal_id="sig-vp-quality-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:58:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82017.0,
            pnl_points=17.0,
            pnl_pct=0.0085,
            evaluated_at="2026-03-11T00:59:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_pnl_first, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9108,
            candidate_id=908,
            signal_id="sig-vp-quality-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:59:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81995.0,
            pnl_points=5.0,
            pnl_pct=0.0025,
            evaluated_at="2026-03-11T01:00:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_pnl_first, "vp_trade_bias_score": 2},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9109,
            candidate_id=909,
            signal_id="sig-vp-quality-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T01:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1991.0,
            pnl_points=9.0,
            pnl_pct=0.0045,
            evaluated_at="2026-03-11T01:01:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_quality_first, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9110,
            candidate_id=910,
            signal_id="sig-vp-quality-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T01:01:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1991.0,
            pnl_points=9.0,
            pnl_pct=0.0045,
            evaluated_at="2026-03-11T01:02:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_quality_first, "vp_trade_bias_score": 2},
        )
    )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9111,
            candidate_id=911,
            signal_id="sig-vp-quality-5",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T01:02:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82012.0,
            pnl_points=12.0,
            pnl_pct=0.006,
            evaluated_at="2026-03-11T01:03:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_last, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9112,
            candidate_id=912,
            signal_id="sig-vp-quality-6",
            worker_id="worker-c",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T01:03:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=81996.0,
            pnl_points=4.0,
            pnl_pct=0.002,
            evaluated_at="2026-03-11T01:04:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_last, "vp_trade_bias_score": 2},
        )
    )

    pnl_sorted = client.get("/execution_outcomes/vp_policy_reason_leaderboard?sort=pnl")
    assert pnl_sorted.status_code == 200
    assert [row["vp_policy_reason"] for row in pnl_sorted.json()["rows"]] == [
        reason_pnl_first,
        reason_quality_first,
        reason_last,
    ]

    quality_sorted = client.get("/execution_outcomes/vp_policy_reason_leaderboard?sort=quality")
    assert quality_sorted.status_code == 200
    assert [row["vp_policy_reason"] for row in quality_sorted.json()["rows"]] == [
        reason_quality_first,
        reason_pnl_first,
        reason_last,
    ]


def test_execution_outcomes_vp_policy_reason_quality_score_exposed_on_supported_surfaces(client):
    reason_quality = "short|rotation_down|short_reversion|medium|score=2|candidate=1"

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9113,
            candidate_id=913,
            signal_id="sig-vp-quality-field-1",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T01:04:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=1997.0,
            pnl_points=30.0,
            pnl_pct=0.015,
            evaluated_at="2026-03-11T01:05:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_quality, "vp_trade_bias_score": 2},
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9114,
            candidate_id=914,
            signal_id="sig-vp-quality-field-2",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T01:05:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2010.0,
            pnl_points=10.0,
            pnl_pct=0.005,
            evaluated_at="2026-03-11T01:06:00Z",
            metadata_json={"vp_policy_side": "short", "vp_policy_reason": reason_quality, "vp_trade_bias_score": 2},
        )
    )

    expected_row = {
        "vp_policy_reason": reason_quality,
        "row_count": 2,
        "avg_pnl": 20.0,
        "direction_correct_rate": 0.5,
        "quality_score": 10.0,
    }

    leaderboard = client.get("/execution_outcomes/vp_policy_reason_leaderboard")
    assert leaderboard.status_code == 200
    _assert_vp_reason_rank_rows(leaderboard.json()["rows"], [expected_row])

    laggards = client.get("/execution_outcomes/vp_policy_reason_laggards")
    assert laggards.status_code == 200
    _assert_vp_reason_rank_rows(laggards.json()["rows"], [expected_row])

    extremes = client.get("/execution_outcomes/vp_policy_reason_extremes")
    assert extremes.status_code == 200
    _assert_vp_reason_rank_rows(extremes.json()["leaders"], [expected_row])
    _assert_vp_reason_rank_rows(extremes.json()["laggards"], [expected_row])

    by_score = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2")
    assert by_score.status_code == 200
    _assert_vp_reason_rank_rows(by_score.json()["leaders"], [expected_row])
    _assert_vp_reason_rank_rows(by_score.json()["laggards"], [expected_row])

    by_score_side = client.get("/execution_outcomes/vp_policy_reason_extremes_by_score/2/short")
    assert by_score_side.status_code == 200
    _assert_vp_reason_rank_rows(by_score_side.json()["leaders"], [expected_row])
    _assert_vp_reason_rank_rows(by_score_side.json()["laggards"], [expected_row])


def test_execution_outcomes_vp_policy_reason_best_uses_quality_defaults(client):
    reason_under_min_count = "long|under_min_count|candidate=1"
    reasons_in_order = [
        "best|quality-1|candidate=1",
        "best|quality-2|candidate=1",
        "best|quality-3|candidate=1",
        "best|quality-4|candidate=1",
        "best|quality-5|candidate=1",
        "best|quality-6|candidate=1",
    ]
    seed_rows = [
        (9120, 920, reasons_in_order[0], "long", "long", 30.0),
        (9121, 921, reasons_in_order[0], "long", "long", 10.0),
        (9122, 922, reasons_in_order[1], "short", "short", 18.0),
        (9123, 923, reasons_in_order[1], "short", "short", 18.0),
        (9124, 924, reasons_in_order[2], "short", "short", 40.0),
        (9125, 925, reasons_in_order[2], "short", "long", 20.0),
        (9126, 926, reasons_in_order[3], "long", "long", 12.0),
        (9127, 927, reasons_in_order[3], "long", "long", 12.0),
        (9128, 928, reasons_in_order[4], "short", "short", 30.0),
        (9129, 929, reasons_in_order[4], "short", "long", 10.0),
        (9130, 930, reasons_in_order[5], "long", "long", 6.0),
        (9131, 931, reasons_in_order[5], "long", "long", 6.0),
    ]

    for journal_id, candidate_id, reason, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-best-{journal_id}",
                worker_id="worker-best",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T01:10:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T01:{10 + (journal_id - 9120):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9132,
            candidate_id=932,
            signal_id="sig-vp-best-under-min-count",
            worker_id="worker-best",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T01:22:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82040.0,
            pnl_points=40.0,
            pnl_pct=40.0 / 82000.0,
            evaluated_at="2026-03-11T01:23:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_under_min_count, "vp_trade_bias_score": 2},
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_best")

    assert response.status_code == 200
    body = response.json()
    assert body["best_count"] == 5
    assert body["count"] == 5
    assert [row["vp_policy_reason"] for row in body["rows"]] == reasons_in_order[:5]
    assert [row["quality_score"] for row in body["rows"]] == [20.0, 18.0, 15.0, 12.0, 10.0]
    assert reason_under_min_count not in [row["vp_policy_reason"] for row in body["rows"]]
    assert body["rows"][-1]["vp_policy_reason"] == reasons_in_order[4]


def test_execution_outcomes_vp_policy_reason_worst_uses_quality_defaults(client):
    reason_under_min_count = "long|under_min_count|candidate=1"
    reasons_by_quality_desc = [
        "worst|quality-1|candidate=1",
        "worst|quality-2|candidate=1",
        "worst|quality-3|candidate=1",
        "worst|quality-4|candidate=1",
        "worst|quality-5|candidate=1",
        "worst|quality-6|candidate=1",
    ]
    seed_rows = [
        (9140, 940, reasons_by_quality_desc[0], "long", "long", 30.0),
        (9141, 941, reasons_by_quality_desc[0], "long", "long", 10.0),
        (9142, 942, reasons_by_quality_desc[1], "short", "short", 18.0),
        (9143, 943, reasons_by_quality_desc[1], "short", "short", 18.0),
        (9144, 944, reasons_by_quality_desc[2], "short", "short", 40.0),
        (9145, 945, reasons_by_quality_desc[2], "short", "long", 20.0),
        (9146, 946, reasons_by_quality_desc[3], "long", "long", 12.0),
        (9147, 947, reasons_by_quality_desc[3], "long", "long", 12.0),
        (9148, 948, reasons_by_quality_desc[4], "short", "short", 30.0),
        (9149, 949, reasons_by_quality_desc[4], "short", "long", 10.0),
        (9150, 950, reasons_by_quality_desc[5], "long", "long", 6.0),
        (9151, 951, reasons_by_quality_desc[5], "long", "long", 6.0),
    ]

    for journal_id, candidate_id, reason, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-worst-{journal_id}",
                worker_id="worker-worst",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T01:30:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T01:{30 + (journal_id - 9140):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9152,
            candidate_id=952,
            signal_id="sig-vp-worst-under-min-count",
            worker_id="worker-worst",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T01:42:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82040.0,
            pnl_points=40.0,
            pnl_pct=40.0 / 82000.0,
            evaluated_at="2026-03-11T01:43:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_under_min_count, "vp_trade_bias_score": 2},
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_worst")

    assert response.status_code == 200
    body = response.json()
    assert body["worst_count"] == 5
    assert body["count"] == 5
    assert [row["vp_policy_reason"] for row in body["rows"]] == list(reversed(reasons_by_quality_desc))[0:5]
    assert [row["quality_score"] for row in body["rows"]] == [6.0, 10.0, 12.0, 15.0, 18.0]
    assert reason_under_min_count not in [row["vp_policy_reason"] for row in body["rows"]]
    assert body["rows"][-1]["vp_policy_reason"] == reasons_by_quality_desc[1]


def test_execution_outcomes_vp_policy_reason_best_worst_uses_quality_defaults(client):
    reason_under_min_count = "long|under_min_count|candidate=1"
    reasons_by_quality_desc = [
        "best-worst|quality-1|candidate=1",
        "best-worst|quality-2|candidate=1",
        "best-worst|quality-3|candidate=1",
        "best-worst|quality-4|candidate=1",
        "best-worst|quality-5|candidate=1",
        "best-worst|quality-6|candidate=1",
    ]
    seed_rows = [
        (9160, 960, reasons_by_quality_desc[0], "long", "long", 30.0),
        (9161, 961, reasons_by_quality_desc[0], "long", "long", 10.0),
        (9162, 962, reasons_by_quality_desc[1], "short", "short", 18.0),
        (9163, 963, reasons_by_quality_desc[1], "short", "short", 18.0),
        (9164, 964, reasons_by_quality_desc[2], "short", "short", 40.0),
        (9165, 965, reasons_by_quality_desc[2], "short", "long", 20.0),
        (9166, 966, reasons_by_quality_desc[3], "long", "long", 12.0),
        (9167, 967, reasons_by_quality_desc[3], "long", "long", 12.0),
        (9168, 968, reasons_by_quality_desc[4], "short", "short", 30.0),
        (9169, 969, reasons_by_quality_desc[4], "short", "long", 10.0),
        (9170, 970, reasons_by_quality_desc[5], "long", "long", 6.0),
        (9171, 971, reasons_by_quality_desc[5], "long", "long", 6.0),
    ]

    for journal_id, candidate_id, reason, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-best-worst-{journal_id}",
                worker_id="worker-best-worst",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T01:50:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T01:{50 + (journal_id - 9160):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9172,
            candidate_id=972,
            signal_id="sig-vp-best-worst-under-min-count",
            worker_id="worker-best-worst",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T02:02:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82040.0,
            pnl_points=40.0,
            pnl_pct=40.0 / 82000.0,
            evaluated_at="2026-03-11T02:03:00Z",
            metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason_under_min_count, "vp_trade_bias_score": 2},
        )
    )

    response = client.get("/execution_outcomes/vp_policy_reason_best_worst")

    assert response.status_code == 200
    body = response.json()
    assert body["min_count_applied"] == 2
    assert body["total_reason_cohorts"] == 7
    assert body["eligible_reason_cohorts"] == 6
    assert body["best_count"] == 5
    assert body["worst_count"] == 5
    assert [row["vp_policy_reason"] for row in body["best"]] == reasons_by_quality_desc[:5]
    assert [row["quality_score"] for row in body["best"]] == [20.0, 18.0, 15.0, 12.0, 10.0]
    assert [row["vp_policy_reason"] for row in body["worst"]] == list(reversed(reasons_by_quality_desc))[0:5]
    assert [row["quality_score"] for row in body["worst"]] == [6.0, 10.0, 12.0, 15.0, 18.0]
    assert reason_under_min_count not in [row["vp_policy_reason"] for row in body["best"]]
    assert reason_under_min_count not in [row["vp_policy_reason"] for row in body["worst"]]


def test_execution_outcomes_vp_policy_reason_best_worst_supports_score_and_side_filters(client):
    reason_score_two_short_best = "best-worst-filter|score2|short-best|candidate=1"
    reason_score_two_long_mid = "best-worst-filter|score2|long-mid|candidate=1"
    reason_score_two_short_worst = "best-worst-filter|score2|short-worst|candidate=1"
    reason_score_three_short_ignored = "best-worst-filter|score3|short-ignored|candidate=1"

    seed_rows = [
        (9180, 980, reason_score_two_short_best, 2, "short", "short", 30.0),
        (9181, 981, reason_score_two_short_best, 2, "short", "short", 10.0),
        (9182, 982, reason_score_two_long_mid, 2, "long", "long", 16.0),
        (9183, 983, reason_score_two_long_mid, 2, "long", "long", 8.0),
        (9184, 984, reason_score_two_short_worst, 2, "short", "short", 20.0),
        (9185, 985, reason_score_two_short_worst, 2, "short", "long", 10.0),
        (9186, 986, reason_score_three_short_ignored, 3, "short", "short", 50.0),
        (9187, 987, reason_score_three_short_ignored, 3, "short", "short", 10.0),
    ]

    for journal_id, candidate_id, reason, score, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-best-worst-filter-{journal_id}",
                worker_id="worker-best-worst-filter",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T02:10:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T02:{10 + (journal_id - 9180):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": score},
            )
        )

    by_score = client.get("/execution_outcomes/vp_policy_reason_best_worst?score=2")

    assert by_score.status_code == 200
    assert by_score.json()["min_count_applied"] == 2
    assert by_score.json()["total_reason_cohorts"] == 3
    assert by_score.json()["eligible_reason_cohorts"] == 3
    assert by_score.json()["best_count"] == 3
    assert by_score.json()["worst_count"] == 3
    assert [row["vp_policy_reason"] for row in by_score.json()["best"]] == [
        reason_score_two_short_best,
        reason_score_two_long_mid,
        reason_score_two_short_worst,
    ]
    assert [row["quality_score"] for row in by_score.json()["best"]] == [20.0, 12.0, 7.5]
    assert [row["vp_policy_reason"] for row in by_score.json()["worst"]] == [
        reason_score_two_short_worst,
        reason_score_two_long_mid,
        reason_score_two_short_best,
    ]
    assert [row["quality_score"] for row in by_score.json()["worst"]] == [7.5, 12.0, 20.0]

    by_score_side = client.get("/execution_outcomes/vp_policy_reason_best_worst?score=2&side=short")

    assert by_score_side.status_code == 200
    assert by_score_side.json()["min_count_applied"] == 2
    assert by_score_side.json()["total_reason_cohorts"] == 2
    assert by_score_side.json()["eligible_reason_cohorts"] == 2
    assert by_score_side.json()["best_count"] == 2
    assert by_score_side.json()["worst_count"] == 2
    assert [row["vp_policy_reason"] for row in by_score_side.json()["best"]] == [
        reason_score_two_short_best,
        reason_score_two_short_worst,
    ]
    assert [row["quality_score"] for row in by_score_side.json()["best"]] == [20.0, 7.5]
    assert [row["vp_policy_reason"] for row in by_score_side.json()["worst"]] == [
        reason_score_two_short_worst,
        reason_score_two_short_best,
    ]
    assert [row["quality_score"] for row in by_score_side.json()["worst"]] == [7.5, 20.0]


def test_execution_outcomes_vp_policy_reason_stability_fields(client):
    reason_high_var = "stability|high-var|candidate=1"
    reason_flat = "stability|flat|candidate=1"
    reason_two = "stability|two-sample|candidate=1"
    reason_one = "stability|one-sample|candidate=1"

    seed_rows = [
        (9400, 1200, reason_high_var, 10.0),
        (9401, 1201, reason_high_var, 20.0),
        (9402, 1202, reason_high_var, 30.0),
        (9403, 1203, reason_flat, 5.0),
        (9404, 1204, reason_flat, 5.0),
        (9405, 1205, reason_flat, 5.0),
        (9406, 1206, reason_two, 8.0),
        (9407, 1207, reason_two, 12.0),
        (9408, 1208, reason_one, 7.0),
    ]

    for journal_id, candidate_id, reason, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-stability-{journal_id}",
                worker_id="worker-stability",
                symbol="BTCUSDT.P",
                direction="long",
                entry_price=82000.0,
                reference_timestamp="2026-03-11T03:00:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T03:{journal_id - 9400:02d}:00Z",
                metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    leaderboard_response = client.get("/execution_outcomes/vp_policy_reason_leaderboard?min_count=1&limit=10&sort=quality")
    laggards_response = client.get("/execution_outcomes/vp_policy_reason_laggards?min_count=1&limit=10&sort=quality")
    best_response = client.get("/execution_outcomes/vp_policy_reason_best?min_count=1&limit=10")
    worst_response = client.get("/execution_outcomes/vp_policy_reason_worst?min_count=1&limit=10")
    best_worst_response = client.get("/execution_outcomes/vp_policy_reason_best_worst?min_count=1&limit=10")

    assert leaderboard_response.status_code == 200
    assert laggards_response.status_code == 200
    assert best_response.status_code == 200
    assert worst_response.status_code == 200
    assert best_worst_response.status_code == 200

    leaderboard_rows = {
        row["vp_policy_reason"]: row
        for row in leaderboard_response.json()["rows"]
    }

    high_var = leaderboard_rows[reason_high_var]
    assert high_var["stdev_pnl"] == pytest.approx(10.0)
    expected_margin = 1.96 * (10.0 / (3.0 ** 0.5))
    assert high_var["pnl_ci_low"] == pytest.approx(20.0 - expected_margin)
    assert high_var["pnl_ci_high"] == pytest.approx(20.0 + expected_margin)

    flat = leaderboard_rows[reason_flat]
    assert flat["stdev_pnl"] == pytest.approx(0.0)
    assert flat["pnl_ci_low"] == pytest.approx(5.0)
    assert flat["pnl_ci_high"] == pytest.approx(5.0)

    two_sample = leaderboard_rows[reason_two]
    assert two_sample["stdev_pnl"] == pytest.approx(8.0 ** 0.5)
    assert two_sample["pnl_ci_low"] is None
    assert two_sample["pnl_ci_high"] is None

    one_sample = leaderboard_rows[reason_one]
    assert one_sample["stdev_pnl"] is None
    assert one_sample["pnl_ci_low"] is None
    assert one_sample["pnl_ci_high"] is None

    for row in leaderboard_response.json()["rows"]:
        assert "stdev_pnl" in row
        assert "pnl_ci_low" in row
        assert "pnl_ci_high" in row
    for row in laggards_response.json()["rows"]:
        assert "stdev_pnl" in row
        assert "pnl_ci_low" in row
        assert "pnl_ci_high" in row
    for row in best_response.json()["rows"]:
        assert "stdev_pnl" in row
        assert "pnl_ci_low" in row
        assert "pnl_ci_high" in row
    for row in worst_response.json()["rows"]:
        assert "stdev_pnl" in row
        assert "pnl_ci_low" in row
        assert "pnl_ci_high" in row
    for row in best_worst_response.json()["best"]:
        assert "stdev_pnl" in row
        assert "pnl_ci_low" in row
        assert "pnl_ci_high" in row
    for row in best_worst_response.json()["worst"]:
        assert "stdev_pnl" in row
        assert "pnl_ci_low" in row
        assert "pnl_ci_high" in row


def test_execution_outcomes_vp_policy_reason_time_window_since_days_since_trades(client):
    reason_a = "time-window|reason-a|candidate=1"
    reason_b = "time-window|reason-b|candidate=1"

    seed_rows = [
        (9500, 1300, reason_a, 30.0, "2026-03-11T10:00:00Z"),
        (9501, 1301, reason_b, 20.0, "2026-03-10T10:00:00Z"),
        (9502, 1302, reason_a, 10.0, "2026-03-09T12:00:00Z"),
        (9503, 1303, reason_b, 5.0, "2026-03-08T09:00:00Z"),
        (9504, 1304, reason_a, 1.0, "2026-03-07T09:00:00Z"),
    ]

    for journal_id, candidate_id, reason, pnl_points, evaluated_at in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-time-window-{journal_id}",
                worker_id="worker-time-window",
                symbol="BTCUSDT.P",
                direction="long",
                entry_price=82000.0,
                reference_timestamp="2026-03-11T09:45:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=evaluated_at,
                metadata_json={"vp_policy_side": "long", "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    by_days = client.get("/execution_outcomes/vp_policy_reason_leaderboard?min_count=1&limit=10&since_days=2")
    assert by_days.status_code == 200
    by_days_rows = {row["vp_policy_reason"]: row for row in by_days.json()["rows"]}
    assert by_days_rows[reason_a]["row_count"] == 2
    assert by_days_rows[reason_b]["row_count"] == 1

    by_trades = client.get("/execution_outcomes/vp_policy_reason_leaderboard?min_count=1&limit=10&since_trades=2")
    assert by_trades.status_code == 200
    by_trades_rows = {row["vp_policy_reason"]: row for row in by_trades.json()["rows"]}
    assert by_trades_rows[reason_a]["row_count"] == 1
    assert by_trades_rows[reason_b]["row_count"] == 1

    by_both = client.get(
        "/execution_outcomes/vp_policy_reason_leaderboard?min_count=1&limit=10&since_days=2&since_trades=2"
    )
    assert by_both.status_code == 200
    by_both_rows = {row["vp_policy_reason"]: row for row in by_both.json()["rows"]}
    assert by_both_rows[reason_a]["row_count"] == 1
    assert by_both_rows[reason_b]["row_count"] == 1

    extremes = client.get("/execution_outcomes/vp_policy_reason_extremes?min_count=1&limit=10&since_trades=2")
    assert extremes.status_code == 200
    assert len(extremes.json()["leaders"]) == 2
    assert len(extremes.json()["laggards"]) == 2

    best = client.get("/execution_outcomes/vp_policy_reason_best?min_count=1&limit=10&since_days=2&since_trades=2")
    worst = client.get("/execution_outcomes/vp_policy_reason_worst?min_count=1&limit=10&since_days=2&since_trades=2")
    best_worst = client.get(
        "/execution_outcomes/vp_policy_reason_best_worst?min_count=1&limit=10&since_days=2&since_trades=2"
    )

    assert best.status_code == 200
    assert worst.status_code == 200
    assert best_worst.status_code == 200
    assert best.json()["applied_since_days"] == 2
    assert best.json()["applied_since_trades"] == 2
    assert worst.json()["applied_since_days"] == 2
    assert worst.json()["applied_since_trades"] == 2
    assert best_worst.json()["applied_since_days"] == 2
    assert best_worst.json()["applied_since_trades"] == 2


def test_execution_outcomes_vp_policy_reason_best_and_worst_support_score_and_side_filters(client):
    reason_score_two_short_best = "best-worst-single-filter|score2|short-best|candidate=1"
    reason_score_two_long_mid = "best-worst-single-filter|score2|long-mid|candidate=1"
    reason_score_two_short_worst = "best-worst-single-filter|score2|short-worst|candidate=1"
    reason_score_three_short_ignored = "best-worst-single-filter|score3|short-ignored|candidate=1"

    seed_rows = [
        (9190, 990, reason_score_two_short_best, 2, "short", "short", 30.0),
        (9191, 991, reason_score_two_short_best, 2, "short", "short", 10.0),
        (9192, 992, reason_score_two_long_mid, 2, "long", "long", 16.0),
        (9193, 993, reason_score_two_long_mid, 2, "long", "long", 8.0),
        (9194, 994, reason_score_two_short_worst, 2, "short", "short", 20.0),
        (9195, 995, reason_score_two_short_worst, 2, "short", "long", 10.0),
        (9196, 996, reason_score_three_short_ignored, 3, "short", "short", 50.0),
        (9197, 997, reason_score_three_short_ignored, 3, "short", "short", 10.0),
    ]

    for journal_id, candidate_id, reason, score, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-single-filter-{journal_id}",
                worker_id="worker-single-filter",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T02:20:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T02:{20 + (journal_id - 9190):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": score},
            )
        )

    best_by_score = client.get("/execution_outcomes/vp_policy_reason_best?score=2")
    assert best_by_score.status_code == 200
    assert [row["vp_policy_reason"] for row in best_by_score.json()["rows"]] == [
        reason_score_two_short_best,
        reason_score_two_long_mid,
        reason_score_two_short_worst,
    ]
    assert [row["quality_score"] for row in best_by_score.json()["rows"]] == [20.0, 12.0, 7.5]

    best_by_score_side = client.get("/execution_outcomes/vp_policy_reason_best?score=2&side=short")
    assert best_by_score_side.status_code == 200
    assert [row["vp_policy_reason"] for row in best_by_score_side.json()["rows"]] == [
        reason_score_two_short_best,
        reason_score_two_short_worst,
    ]
    assert [row["quality_score"] for row in best_by_score_side.json()["rows"]] == [20.0, 7.5]

    worst_by_score = client.get("/execution_outcomes/vp_policy_reason_worst?score=2")
    assert worst_by_score.status_code == 200
    assert [row["vp_policy_reason"] for row in worst_by_score.json()["rows"]] == [
        reason_score_two_short_worst,
        reason_score_two_long_mid,
        reason_score_two_short_best,
    ]
    assert [row["quality_score"] for row in worst_by_score.json()["rows"]] == [7.5, 12.0, 20.0]

    worst_by_score_side = client.get("/execution_outcomes/vp_policy_reason_worst?score=2&side=short")
    assert worst_by_score_side.status_code == 200
    assert [row["vp_policy_reason"] for row in worst_by_score_side.json()["rows"]] == [
        reason_score_two_short_worst,
        reason_score_two_short_best,
    ]
    assert [row["quality_score"] for row in worst_by_score_side.json()["rows"]] == [7.5, 20.0]


def test_execution_outcomes_vp_policy_reason_convenience_surfaces_echo_applied_filters(client):
    reason_score_two_short_best = "convenience-metadata|score2|short-best|candidate=1"
    reason_score_two_short_worst = "convenience-metadata|score2|short-worst|candidate=1"
    reason_score_three_ignored = "convenience-metadata|score3|ignored|candidate=1"

    seed_rows = [
        (9200, 1000, reason_score_two_short_best, 2, "short", "short", 30.0),
        (9201, 1001, reason_score_two_short_best, 2, "short", "short", 10.0),
        (9202, 1002, reason_score_two_short_worst, 2, "short", "short", 20.0),
        (9203, 1003, reason_score_two_short_worst, 2, "short", "long", 10.0),
        (9204, 1004, reason_score_three_ignored, 3, "short", "short", 50.0),
        (9205, 1005, reason_score_three_ignored, 3, "short", "short", 10.0),
    ]

    for journal_id, candidate_id, reason, score, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-convenience-metadata-{journal_id}",
                worker_id="worker-convenience-metadata",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T02:30:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T02:{30 + (journal_id - 9200):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": score},
            )
        )

    best_default = client.get("/execution_outcomes/vp_policy_reason_best")
    assert best_default.status_code == 200
    assert best_default.json()["applied_score"] is None
    assert best_default.json()["applied_side"] is None
    assert best_default.json()["is_filtered"] is False

    best_filtered = client.get("/execution_outcomes/vp_policy_reason_best?score=2&side=short")
    assert best_filtered.status_code == 200
    assert best_filtered.json()["applied_score"] == 2
    assert best_filtered.json()["applied_side"] == "short"
    assert best_filtered.json()["is_filtered"] is True

    worst_default = client.get("/execution_outcomes/vp_policy_reason_worst")
    assert worst_default.status_code == 200
    assert worst_default.json()["applied_score"] is None
    assert worst_default.json()["applied_side"] is None
    assert worst_default.json()["is_filtered"] is False

    worst_filtered = client.get("/execution_outcomes/vp_policy_reason_worst?score=2&side=short")
    assert worst_filtered.status_code == 200
    assert worst_filtered.json()["applied_score"] == 2
    assert worst_filtered.json()["applied_side"] == "short"
    assert worst_filtered.json()["is_filtered"] is True

    best_worst_default = client.get("/execution_outcomes/vp_policy_reason_best_worst")
    assert best_worst_default.status_code == 200
    assert best_worst_default.json()["applied_score"] is None
    assert best_worst_default.json()["applied_side"] is None
    assert best_worst_default.json()["is_filtered"] is False
    assert best_worst_default.json()["min_count_applied"] == 2
    assert best_worst_default.json()["total_reason_cohorts"] == 3
    assert best_worst_default.json()["eligible_reason_cohorts"] == 3

    best_worst_filtered = client.get("/execution_outcomes/vp_policy_reason_best_worst?score=2&side=short")
    assert best_worst_filtered.status_code == 200
    assert best_worst_filtered.json()["applied_score"] == 2
    assert best_worst_filtered.json()["applied_side"] == "short"
    assert best_worst_filtered.json()["is_filtered"] is True
    assert best_worst_filtered.json()["min_count_applied"] == 2
    assert best_worst_filtered.json()["total_reason_cohorts"] == 2
    assert best_worst_filtered.json()["eligible_reason_cohorts"] == 2


def test_execution_outcomes_vp_policy_reason_monitor_inherits_filters_and_computes_status(client):
    reason_short_best = "monitor|score2|short-best|candidate=1"
    reason_long_mid = "monitor|score2|long-mid|candidate=1"
    reason_short_worst = "monitor|score2|short-worst|candidate=1"
    reason_score_three_ignored = "monitor|score3|ignored|candidate=1"
    reason_old_high_ignored = "monitor|score2|old-high|candidate=1"

    seed_rows = [
        (9600, 1600, reason_short_best, 2, "short", "short", 30.0, "2026-03-11T06:00:00Z"),
        (9601, 1601, reason_short_best, 2, "short", "short", 10.0, "2026-03-11T06:01:00Z"),
        (9602, 1602, reason_long_mid, 2, "long", "long", 16.0, "2026-03-11T06:02:00Z"),
        (9603, 1603, reason_long_mid, 2, "long", "long", 8.0, "2026-03-11T06:03:00Z"),
        (9604, 1604, reason_short_worst, 2, "short", "short", 20.0, "2026-03-11T06:04:00Z"),
        (9605, 1605, reason_short_worst, 2, "short", "long", 10.0, "2026-03-11T06:05:00Z"),
        (9606, 1606, reason_score_three_ignored, 3, "short", "short", 50.0, "2026-03-11T06:06:00Z"),
        (9607, 1607, reason_score_three_ignored, 3, "short", "short", 10.0, "2026-03-11T06:07:00Z"),
        (9608, 1608, reason_old_high_ignored, 2, "short", "short", 80.0, "2026-03-01T06:00:00Z"),
        (9609, 1609, reason_old_high_ignored, 2, "short", "short", 80.0, "2026-03-01T06:01:00Z"),
    ]

    for journal_id, candidate_id, reason, score, vp_policy_side, row_direction, pnl_points, evaluated_at in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-monitor-{journal_id}",
                worker_id="worker-monitor",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T05:59:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=evaluated_at,
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": score},
            )
        )

    healthy_monitor = client.get(
        "/execution_outcomes/vp_policy_reason_monitor?score=2&since_days=2&since_trades=100"
    )
    healthy_best_worst = client.get(
        "/execution_outcomes/vp_policy_reason_best_worst?score=2&since_days=2&since_trades=100"
    )

    assert healthy_monitor.status_code == 200
    assert healthy_best_worst.status_code == 200

    healthy_body = healthy_monitor.json()
    healthy_best_worst_body = healthy_best_worst.json()

    assert healthy_body["applied_score"] == 2
    assert healthy_body["applied_side"] is None
    assert healthy_body["applied_since_days"] == 2
    assert healthy_body["applied_since_trades"] == 100
    assert healthy_body["min_count_applied"] == 2
    assert healthy_body["total_reason_cohorts"] == 3
    assert healthy_body["eligible_reason_cohorts"] == 3
    assert healthy_body["best_count"] == 3
    assert healthy_body["worst_count"] == 3
    assert healthy_body["best"] == healthy_best_worst_body["best"]
    assert healthy_body["worst"] == healthy_best_worst_body["worst"]
    assert healthy_body["best_count"] == healthy_best_worst_body["best_count"]
    assert healthy_body["worst_count"] == healthy_best_worst_body["worst_count"]
    assert healthy_body["monitor_status"] == "healthy"
    assert healthy_body["top_quality_score"] == healthy_body["best"][0]["quality_score"]
    assert healthy_body["bottom_quality_score"] == healthy_body["worst"][0]["quality_score"]
    assert healthy_body["top_quality_score"] == pytest.approx(20.0)
    assert healthy_body["bottom_quality_score"] == pytest.approx(7.5)
    assert healthy_body["quality_spread"] == pytest.approx(12.5)

    thin_monitor = client.get(
        "/execution_outcomes/vp_policy_reason_monitor?score=2&side=short&since_days=2&since_trades=100"
    )
    thin_best_worst = client.get(
        "/execution_outcomes/vp_policy_reason_best_worst?score=2&side=short&since_days=2&since_trades=100"
    )

    assert thin_monitor.status_code == 200
    assert thin_best_worst.status_code == 200

    thin_body = thin_monitor.json()
    thin_best_worst_body = thin_best_worst.json()

    assert thin_body["best"] == thin_best_worst_body["best"]
    assert thin_body["worst"] == thin_best_worst_body["worst"]
    assert thin_body["total_reason_cohorts"] == 2
    assert thin_body["eligible_reason_cohorts"] == 2
    assert thin_body["monitor_status"] == "thin"
    assert thin_body["top_quality_score"] == pytest.approx(20.0)
    assert thin_body["bottom_quality_score"] == pytest.approx(7.5)
    assert thin_body["quality_spread"] == pytest.approx(12.5)

    empty_monitor = client.get(
        "/execution_outcomes/vp_policy_reason_monitor?score=99&since_days=2&since_trades=100"
    )
    assert empty_monitor.status_code == 200

    empty_body = empty_monitor.json()
    assert empty_body["total_reason_cohorts"] == 0
    assert empty_body["eligible_reason_cohorts"] == 0
    assert empty_body["best_count"] == 0
    assert empty_body["worst_count"] == 0
    assert empty_body["monitor_status"] == "empty"
    assert empty_body["top_quality_score"] is None
    assert empty_body["bottom_quality_score"] is None
    assert empty_body["quality_spread"] is None


def test_execution_outcomes_vp_policy_reason_count_metadata_contract_parity_across_surfaces(client):
    reason_best = "contract-parity|best|candidate=1"
    reason_worst = "contract-parity|worst|candidate=1"

    seed_rows = [
        (9300, 1100, reason_best, "long", "long", 30.0),
        (9301, 1101, reason_best, "long", "long", 10.0),
        (9302, 1102, reason_worst, "short", "short", 20.0),
        (9303, 1103, reason_worst, "short", "long", 10.0),
    ]

    for journal_id, candidate_id, reason, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-contract-parity-{journal_id}",
                worker_id="worker-contract-parity",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T02:45:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T02:{45 + (journal_id - 9300):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    best_response = client.get("/execution_outcomes/vp_policy_reason_best?score=2")
    worst_response = client.get("/execution_outcomes/vp_policy_reason_worst?score=2")
    best_worst_response = client.get("/execution_outcomes/vp_policy_reason_best_worst?score=2")

    assert best_response.status_code == 200
    assert worst_response.status_code == 200
    assert best_worst_response.status_code == 200

    best_body = best_response.json()
    worst_body = worst_response.json()
    best_worst_body = best_worst_response.json()

    assert best_body["best_count"] == len(best_body["rows"])
    assert best_body["count"] == len(best_body["rows"])

    assert worst_body["worst_count"] == len(worst_body["rows"])
    assert worst_body["count"] == len(worst_body["rows"])

    assert best_worst_body["best_count"] == len(best_worst_body["best"])
    assert best_worst_body["worst_count"] == len(best_worst_body["worst"])
    assert best_worst_body["total_reason_cohorts"] >= best_worst_body["best_count"]
    assert best_worst_body["total_reason_cohorts"] >= best_worst_body["worst_count"]
    assert best_worst_body["eligible_reason_cohorts"] <= best_worst_body["total_reason_cohorts"]

    assert best_worst_body["best_count"] == best_body["best_count"]
    assert best_worst_body["worst_count"] == worst_body["worst_count"]


def test_execution_outcomes_vp_policy_reason_policy_rankings_returns_policy_intelligence(client):
    policy_best = "policy-rankings|best|candidate=1"
    policy_mid = "policy-rankings|mid|candidate=1"
    policy_worst = "policy-rankings|worst|candidate=1"

    seed_rows = [
        (9350, 1150, policy_best, "long", "long", 30.0),
        (9351, 1151, policy_best, "long", "long", 10.0),
        (9352, 1152, policy_mid, "long", "long", 10.0),
        (9353, 1153, policy_mid, "long", "short", 10.0),
        (9354, 1154, policy_worst, "long", "short", 8.0),
        (9355, 1155, policy_worst, "long", "short", -2.0),
    ]

    for journal_id, candidate_id, reason, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-policy-rankings-{journal_id}",
                worker_id="worker-policy-rankings",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T03:10:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T03:{10 + (journal_id - 9350):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    response = client.get("/execution_outcomes/vp_policy_reason/policy_rankings?score=2&side=long&min_count=2&limit=3")

    assert response.status_code == 200
    body = response.json()

    assert body["applied_score"] == 2
    assert body["applied_side"] == "long"
    assert body["is_filtered"] is True
    assert body["count"] == 3
    assert body["count"] == len(body["policies"])

    assert [row["policy"] for row in body["policies"]] == [
        policy_best,
        policy_mid,
        policy_worst,
    ]

    assert body["policies"][0]["score"] == pytest.approx(20.0)
    assert body["policies"][1]["score"] == pytest.approx(5.0)
    assert body["policies"][2]["score"] == pytest.approx(1.5)

    assert body["policies"][0]["wins"] == 2
    assert body["policies"][0]["losses"] == 0
    assert body["policies"][1]["wins"] == 1
    assert body["policies"][1]["losses"] == 1
    assert body["policies"][2]["wins"] == 1
    assert body["policies"][2]["losses"] == 1

    assert body["policies"][0]["expectancy"] == pytest.approx(20.0)
    assert body["policies"][1]["expectancy"] == pytest.approx(10.0)
    assert body["policies"][2]["expectancy"] == pytest.approx(3.0)


def test_execution_outcomes_vp_policy_reason_policy_selector_simulation_replays_top_policy(client):
    policy_primary = "policy-sim|primary|candidate=1"
    policy_secondary = "policy-sim|secondary|candidate=1"

    seed_rows = [
        (9360, 1160, policy_primary, "long", "long", 20.0),
        (9361, 1161, policy_primary, "long", "short", -10.0),
        (9362, 1162, policy_primary, "long", "long", 10.0),
        (9363, 1163, policy_secondary, "long", "long", 30.0),
        (9364, 1164, policy_secondary, "long", "long", 30.0),
        (9365, 1165, policy_secondary, "long", "long", 30.0),
    ]

    for journal_id, candidate_id, reason, vp_policy_side, row_direction, pnl_points in seed_rows:
        event_writer.insert_execution_outcome(
            event_writer.ExecutionOutcomeParams(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=f"sig-vp-policy-sim-{journal_id}",
                worker_id="worker-policy-sim",
                symbol="BTCUSDT.P",
                direction=row_direction,
                entry_price=82000.0,
                reference_timestamp="2026-03-11T03:20:00Z",
                evaluation_window_minutes=15,
                outcome_status="evaluated",
                exit_price=82000.0 + pnl_points,
                pnl_points=pnl_points,
                pnl_pct=pnl_points / 82000.0,
                evaluated_at=f"2026-03-11T03:{20 + (journal_id - 9360):02d}:00Z",
                metadata_json={"vp_policy_side": vp_policy_side, "vp_policy_reason": reason, "vp_trade_bias_score": 2},
            )
        )

    response = client.get(
        "/execution_outcomes/vp_policy_reason/policy_selector_simulation"
        "?score=2&side=long&min_count=2&limit=50&worker_id=worker-policy-sim"
    )

    assert response.status_code == 200
    body = response.json()

    assert body["applied_score"] == 2
    assert body["applied_side"] == "long"
    assert body["is_filtered"] is True
    assert body["count"] == 6
    assert body["total_steps"] == 6
    assert body["count"] == len(body["steps"])

    assert [step["selected_policy"] for step in body["steps"]] == [
        None,
        None,
        policy_primary,
        policy_primary,
        policy_primary,
        policy_secondary,
    ]
    assert [step["observed_policy"] for step in body["steps"]] == [
        policy_primary,
        policy_primary,
        policy_primary,
        policy_secondary,
        policy_secondary,
        policy_secondary,
    ]
    assert [step["selected"] for step in body["steps"]] == [
        False,
        False,
        True,
        False,
        False,
        True,
    ]

    assert body["total_selections"] == 2
    assert body["policy_switches"] == 1
    assert body["switch_rate"] == pytest.approx(1.0)
    assert body["baseline_policy"] == policy_secondary
    assert body["baseline_total_selections"] == 3
    assert body["baseline_wins"] == 3
    assert body["baseline_losses"] == 0
    assert body["baseline_expectancy"] == pytest.approx(30.0)
    assert body["baseline_cumulative_pnl_points"] == pytest.approx(90.0)
    assert body["simulated_wins"] == 2
    assert body["simulated_losses"] == 0
    assert body["simulated_expectancy"] == pytest.approx(20.0)
    assert body["cumulative_pnl_points"] == pytest.approx(40.0)
    assert body["expectancy_delta_vs_baseline"] == pytest.approx(-10.0)
    assert body["cumulative_pnl_delta_vs_baseline"] == pytest.approx(-50.0)
    assert body["win_rate_delta_vs_baseline"] == pytest.approx(0.0)
    assert body["selector_outperformed_baseline"] is False
    assert body["selector_outperformance_reason"] == "lower_cumulative_pnl"
    assert body["summary"]["verdict"] == "lower_cumulative_pnl"
    assert body["summary"]["selector_outperformed_baseline"] is False
    assert body["summary"]["adaptive_pnl"] == pytest.approx(40.0)
    assert body["summary"]["baseline_pnl"] == pytest.approx(90.0)
    assert body["summary"]["pnl_delta"] == pytest.approx(-50.0)
    assert body["summary"]["switch_rate"] == pytest.approx(1.0)
    assert body["baseline_cumulative_pnl_points"] > body["cumulative_pnl_points"]


def test_execution_outcomes_summary_returns_compact_metrics(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9101,
            candidate_id=901,
            signal_id="sig-summary-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-10T23:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=100.0,
            pnl_pct=1.0,
            max_favorable_excursion=140.0,
            max_adverse_excursion=-20.0,
            evaluated_at="2026-03-10T23:16:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9102,
            candidate_id=902,
            signal_id="sig-summary-2",
            worker_id="worker-a",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-10T23:01:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2010.0,
            pnl_points=-10.0,
            pnl_pct=-0.5,
            max_favorable_excursion=20.0,
            max_adverse_excursion=-25.0,
            evaluated_at="2026-03-10T23:17:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9103,
            candidate_id=903,
            signal_id="sig-summary-3",
            worker_id="worker-b",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82100.0,
            reference_timestamp="2026-03-10T23:02:00Z",
            evaluation_window_minutes=15,
            outcome_status="insufficient_data",
            evaluated_at="2026-03-10T23:18:00Z",
        )
    )

    response = client.get("/execution_outcomes/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["evaluated_count"] == 2
    assert body["insufficient_data_count"] == 1
    assert body["avg_pnl_points"] == pytest.approx(45.0)
    assert body["avg_pnl_pct"] == pytest.approx(0.25)
    assert body["win_rate"] == pytest.approx(0.5)
    assert body["by_symbol"] == {"BTCUSDT.P": 2, "ETHUSDT.P": 1}
    assert body["latest_evaluated_at"] == "2026-03-10T23:18:00Z"

    filtered_response = client.get("/execution_outcomes/summary?symbol=BTCUSDT.P")
    assert filtered_response.status_code == 200
    filtered_body = filtered_response.json()
    assert filtered_body["total"] == 2
    assert filtered_body["evaluated_count"] == 1


def test_execution_outcome_label_rules_winner_loser_scratch_unknown(monkeypatch):
    monkeypatch.setenv("OUTCOME_WIN_THRESHOLD_PCT", "0.10")
    monkeypatch.setenv("OUTCOME_LOSS_THRESHOLD_PCT", "-0.10")

    assert event_writer.get_execution_outcome_label(outcome_status="insufficient_data", pnl_pct=0.5) == "unknown"
    assert event_writer.get_execution_outcome_label(outcome_status="evaluated", pnl_pct=0.11) == "winner"
    assert event_writer.get_execution_outcome_label(outcome_status="evaluated", pnl_pct=-0.11) == "loser"
    assert event_writer.get_execution_outcome_label(outcome_status="evaluated", pnl_pct=0.10) == "scratch"


def test_execution_outcomes_recent_supports_label_since_until_filters(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9201,
            candidate_id=1001,
            signal_id="sig-labeled-recent-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82100.0,
            pnl_points=100.0,
            pnl_pct=0.12,
            evaluated_at="2026-03-11T00:01:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9202,
            candidate_id=1002,
            signal_id="sig-labeled-recent-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T00:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82020.0,
            pnl_points=20.0,
            pnl_pct=0.05,
            evaluated_at="2026-03-11T00:02:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9203,
            candidate_id=1003,
            signal_id="sig-labeled-recent-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2010.0,
            pnl_points=-10.0,
            pnl_pct=-0.50,
            evaluated_at="2026-03-11T00:03:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9204,
            candidate_id=1004,
            signal_id="sig-labeled-recent-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T00:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="insufficient_data",
            evaluated_at="2026-03-11T00:04:00Z",
        )
    )

    winner_only = client.get("/execution_outcomes/recent?label=winner")
    assert winner_only.status_code == 200
    winner_body = winner_only.json()
    assert winner_body["count"] == 1
    assert winner_body["rows"][0]["signal_id"] == "sig-labeled-recent-1"
    assert winner_body["rows"][0]["label"] == "winner"

    windowed = client.get(
        "/execution_outcomes/recent?since=2026-03-11T00:02:00Z&until=2026-03-11T00:03:30Z"
    )
    assert windowed.status_code == 200
    windowed_rows = windowed.json()["rows"]
    assert len(windowed_rows) == 2
    assert [row["signal_id"] for row in windowed_rows] == ["sig-labeled-recent-3", "sig-labeled-recent-2"]
    assert [row["label"] for row in windowed_rows] == ["loser", "scratch"]


def test_execution_outcomes_scorecard_returns_labeled_metrics(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9301,
            candidate_id=1101,
            signal_id="sig-scorecard-1",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T01:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82150.0,
            pnl_points=15.0,
            pnl_pct=0.30,
            evaluated_at="2026-03-11T01:00:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9302,
            candidate_id=1102,
            signal_id="sig-scorecard-2",
            worker_id="worker-a",
            symbol="BTCUSDT.P",
            direction="short",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T01:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82050.0,
            pnl_points=-5.0,
            pnl_pct=-0.20,
            evaluated_at="2026-03-11T01:01:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9303,
            candidate_id=1103,
            signal_id="sig-scorecard-3",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="long",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T01:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=2001.0,
            pnl_points=1.0,
            pnl_pct=0.05,
            evaluated_at="2026-03-11T01:02:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9304,
            candidate_id=1104,
            signal_id="sig-scorecard-4",
            worker_id="worker-b",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T01:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="insufficient_data",
            evaluated_at="2026-03-11T01:03:00Z",
        )
    )

    response = client.get("/execution_outcomes/scorecard")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 4
    assert body["evaluated_count"] == 3
    assert body["labeled_count"] == 3
    assert body["winner_count"] == 1
    assert body["loser_count"] == 1
    assert body["scratch_count"] == 1
    assert body["unknown_count"] == 1
    assert body["win_rate"] == pytest.approx(1 / 3)
    assert body["loss_rate"] == pytest.approx(1 / 3)
    assert body["scratch_rate"] == pytest.approx(1 / 3)
    assert body["avg_pnl_points"] == pytest.approx((15.0 - 5.0 + 1.0) / 3)
    assert body["avg_pnl_pct"] == pytest.approx((0.30 - 0.20 + 0.05) / 3)
    assert body["expectancy_points"] == pytest.approx((15.0 - 5.0 + 1.0) / 3)
    assert body["expectancy_pct"] == pytest.approx((0.30 - 0.20 + 0.05) / 3)
    assert body["best_pnl_points"] == pytest.approx(15.0)
    assert body["worst_pnl_points"] == pytest.approx(-5.0)
    assert body["by_symbol"] == {"BTCUSDT.P": 2, "ETHUSDT.P": 2}
    assert body["by_direction"] == {"long": 2, "short": 2}
    assert body["latest_evaluated_at"] == "2026-03-11T01:03:00Z"

    winner_filtered = client.get("/execution_outcomes/scorecard?label=winner")
    assert winner_filtered.status_code == 200
    filtered_body = winner_filtered.json()
    assert filtered_body["total"] == 1
    assert filtered_body["winner_count"] == 1
    assert filtered_body["unknown_count"] == 0


def test_execution_outcomes_export_csv_includes_label_column(client):
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9401,
            candidate_id=1201,
            signal_id="sig-export-outcomes-1",
            worker_id="worker-export",
            symbol="BTCUSDT.P",
            direction="long",
            entry_price=82000.0,
            reference_timestamp="2026-03-11T02:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="evaluated",
            exit_price=82120.0,
            pnl_points=120.0,
            pnl_pct=0.20,
            evaluated_at="2026-03-11T02:01:00Z",
        )
    )
    event_writer.insert_execution_outcome(
        event_writer.ExecutionOutcomeParams(
            journal_id=9402,
            candidate_id=1202,
            signal_id="sig-export-outcomes-2",
            worker_id="worker-export",
            symbol="ETHUSDT.P",
            direction="short",
            entry_price=2000.0,
            reference_timestamp="2026-03-11T02:00:00Z",
            evaluation_window_minutes=15,
            outcome_status="insufficient_data",
            evaluated_at="2026-03-11T02:02:00Z",
        )
    )

    response = client.get("/execution_outcomes/export.csv?worker_id=worker-export&symbol=BTCUSDT.P")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "label" in response.text.splitlines()[0]

    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 1
    assert rows[0]["signal_id"] == "sig-export-outcomes-1"
    assert rows[0]["label"] == "winner"


def test_execution_outcomes_leaderboard_groups_by_strategy_source_setup_family_worker(client):
    _seed_cohort_outcomes_dataset()

    by_strategy_resp = client.get("/execution_outcomes/leaderboard?group_by=strategy&limit=10")
    assert by_strategy_resp.status_code == 200
    by_strategy = {row["cohort_key"]: row for row in by_strategy_resp.json()["rows"]}
    assert set(by_strategy.keys()) == {"adaptive-v2", "adaptive-v3", "unknown"}
    assert by_strategy["adaptive-v2"]["total"] == 2
    assert by_strategy["adaptive-v2"]["winner_count"] == 1
    assert by_strategy["adaptive-v2"]["scratch_count"] == 1

    by_source_resp = client.get("/execution_outcomes/leaderboard?group_by=source&limit=10")
    assert by_source_resp.status_code == 200
    by_source = {row["cohort_key"]: row for row in by_source_resp.json()["rows"]}
    assert set(by_source.keys()) == {"scanner-a", "scanner-b", "unknown"}
    assert by_source["scanner-b"]["total"] == 2

    by_setup_resp = client.get("/execution_outcomes/leaderboard?group_by=setup_family&limit=10")
    assert by_setup_resp.status_code == 200
    by_setup = {row["cohort_key"]: row for row in by_setup_resp.json()["rows"]}
    assert set(by_setup.keys()) == {"momentum", "reversal", "unknown"}
    assert by_setup["momentum"]["total"] == 2

    by_worker_resp = client.get("/execution_outcomes/leaderboard?group_by=worker_id&limit=10")
    assert by_worker_resp.status_code == 200
    by_worker = {row["cohort_key"]: row for row in by_worker_resp.json()["rows"]}
    assert set(by_worker.keys()) == {"worker-alpha", "worker-beta"}
    assert by_worker["worker-alpha"]["total"] == 2
    assert by_worker["worker-beta"]["total"] == 3


def test_execution_outcomes_leaderboard_min_samples_filters_cohorts(client):
    _seed_cohort_outcomes_dataset()

    response = client.get("/execution_outcomes/leaderboard?group_by=strategy&min_samples=2&limit=10")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    cohort_keys = {row["cohort_key"] for row in body["rows"]}
    assert cohort_keys == {"adaptive-v2", "adaptive-v3"}


def test_execution_outcomes_compare_returns_both_sides_and_deltas(client):
    _seed_cohort_outcomes_dataset()

    response = client.get(
        "/execution_outcomes/compare?left_group_by=strategy&left_value=adaptive-v2&right_group_by=strategy&right_value=adaptive-v3"
    )

    assert response.status_code == 200
    body = response.json()

    left = body["left"]
    right = body["right"]
    deltas = body["deltas"]

    assert left["group_by"] == "strategy"
    assert left["cohort_key"] == "adaptive-v2"
    assert left["total"] == 2
    assert left["win_rate"] == pytest.approx(0.5)
    assert left["avg_pnl_points"] == pytest.approx(9.0)
    assert left["avg_pnl_pct"] == pytest.approx(0.075)

    assert right["group_by"] == "strategy"
    assert right["cohort_key"] == "adaptive-v3"
    assert right["total"] == 2
    assert right["win_rate"] == pytest.approx(0.5)
    assert right["avg_pnl_points"] == pytest.approx(-3.5)
    assert right["avg_pnl_pct"] == pytest.approx(-0.025)

    assert deltas["delta_win_rate"] == pytest.approx(0.0)
    assert deltas["delta_avg_pnl_points"] == pytest.approx(12.5)
    assert deltas["delta_avg_pnl_pct"] == pytest.approx(0.1)
    assert deltas["delta_expectancy_points"] == pytest.approx(12.5)
    assert deltas["delta_expectancy_pct"] == pytest.approx(0.1)


def test_execution_outcomes_leaderboard_csv_returns_expected_headers_and_rows(client):
    _seed_cohort_outcomes_dataset()

    response = client.get("/execution_outcomes/leaderboard.csv?group_by=worker_id&min_samples=1")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "cohort_key,total,evaluated_count" in response.text

    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 2
    keys = {row["cohort_key"] for row in rows}
    assert keys == {"worker-alpha", "worker-beta"}


def test_execution_outcomes_policy_recommendation_returns_deterministic_top_cohort(client):
    _seed_cohort_outcomes_dataset()

    response = client.get("/execution_outcomes/policy_recommendation?group_by=strategy&symbol=BTCUSDT.P")

    assert response.status_code == 200
    body = response.json()
    assert body["group_by"] == "strategy"
    assert body["selected_count"] == 1
    assert body["rows"][0]["cohort_key"] == "adaptive-v2"
    assert body["rows"][0]["ranking_score"] is not None
    assert "top cohort is adaptive-v2" in body["recommendation_summary"]


def test_execution_outcomes_policy_recommendation_min_samples_affects_selection(client):
    _seed_cohort_outcomes_dataset()

    response = client.get("/execution_outcomes/policy_recommendation?group_by=strategy&min_samples=3")

    assert response.status_code == 200
    body = response.json()
    assert body["selected_count"] == 0
    assert body["rows"] == []


def test_execution_outcomes_policy_recommendation_scoring_mode_changes_ranking(client):
    _insert_outcome_with_candidate_context(
        journal_id=9601,
        signal_id="sig-policy-alpha-win",
        worker_id="worker-alpha",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="alpha",
        source="scanner-a",
        setup_family="momentum",
        outcome_status="evaluated",
        pnl_points=40.0,
        pnl_pct=0.30,
        evaluated_at="2026-03-11T05:01:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9602,
        signal_id="sig-policy-alpha-loss",
        worker_id="worker-alpha",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="alpha",
        source="scanner-a",
        setup_family="momentum",
        outcome_status="evaluated",
        pnl_points=-25.0,
        pnl_pct=-0.20,
        evaluated_at="2026-03-11T05:02:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9603,
        signal_id="sig-policy-beta-scratch-1",
        worker_id="worker-beta",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="beta",
        source="scanner-b",
        setup_family="reversal",
        outcome_status="evaluated",
        pnl_points=6.0,
        pnl_pct=0.06,
        evaluated_at="2026-03-11T05:03:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9604,
        signal_id="sig-policy-beta-scratch-2",
        worker_id="worker-beta",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="beta",
        source="scanner-b",
        setup_family="reversal",
        outcome_status="evaluated",
        pnl_points=7.0,
        pnl_pct=0.07,
        evaluated_at="2026-03-11T05:04:00Z",
    )

    by_expectancy = client.get(
        "/execution_outcomes/policy_recommendation?group_by=strategy&symbol=BTCUSDT.P&top_n=2&scoring_mode=expectancy_pct"
    )
    assert by_expectancy.status_code == 200
    assert by_expectancy.json()["rows"][0]["cohort_key"] == "beta"

    by_win_rate = client.get(
        "/execution_outcomes/policy_recommendation?group_by=strategy&symbol=BTCUSDT.P&top_n=2&scoring_mode=win_rate"
    )
    assert by_win_rate.status_code == 200
    assert by_win_rate.json()["rows"][0]["cohort_key"] == "alpha"


def test_execution_outcomes_policy_matrix_returns_ranked_rows_for_supported_groupings(client):
    _seed_cohort_outcomes_dataset()

    response = client.get("/execution_outcomes/policy_matrix?top_n_per_group=2")

    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"strategy", "source", "setup_family", "worker_id"}
    assert len(body["strategy"]) <= 2
    assert len(body["source"]) <= 2
    assert len(body["setup_family"]) <= 2
    assert len(body["worker_id"]) <= 2
    if body["strategy"]:
        assert "ranking_score" in body["strategy"][0]


def test_execution_outcomes_policy_recommendation_csv_returns_expected_headers_and_rows(client):
    _seed_cohort_outcomes_dataset()

    response = client.get("/execution_outcomes/policy_recommendation.csv?group_by=strategy&top_n=2")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "cohort_key,total,evaluated_count" in response.text
    assert "ranking_score" in response.text.splitlines()[0]

    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 2
    assert rows[0]["cohort_key"] in {"adaptive-v2", "adaptive-v3", "unknown"}


def _seed_policy_audit_strategy_dataset() -> None:
    _insert_outcome_with_candidate_context(
        journal_id=9701,
        signal_id="sig-audit-alpha-win-1",
        worker_id="worker-alpha",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="alpha",
        source="scanner-a",
        setup_family="momentum",
        outcome_status="evaluated",
        pnl_points=20.0,
        pnl_pct=0.20,
        evaluated_at="2026-03-11T06:01:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9702,
        signal_id="sig-audit-beta-scratch-1",
        worker_id="worker-beta",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="beta",
        source="scanner-b",
        setup_family="reversal",
        outcome_status="evaluated",
        pnl_points=5.0,
        pnl_pct=0.05,
        evaluated_at="2026-03-11T06:02:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9703,
        signal_id="sig-audit-alpha-loss-1",
        worker_id="worker-alpha",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="alpha",
        source="scanner-a",
        setup_family="momentum",
        outcome_status="evaluated",
        pnl_points=-30.0,
        pnl_pct=-0.30,
        evaluated_at="2026-03-11T06:03:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9704,
        signal_id="sig-audit-beta-win-1",
        worker_id="worker-beta",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="beta",
        source="scanner-b",
        setup_family="reversal",
        outcome_status="evaluated",
        pnl_points=12.0,
        pnl_pct=0.12,
        evaluated_at="2026-03-11T06:04:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9705,
        signal_id="sig-audit-alpha-win-2",
        worker_id="worker-alpha",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="alpha",
        source="scanner-a",
        setup_family="momentum",
        outcome_status="evaluated",
        pnl_points=15.0,
        pnl_pct=0.15,
        evaluated_at="2026-03-11T06:05:00Z",
    )
    _insert_outcome_with_candidate_context(
        journal_id=9706,
        signal_id="sig-audit-beta-loss-1",
        worker_id="worker-beta",
        symbol="BTCUSDT.P",
        direction="long",
        strategy="beta",
        source="scanner-b",
        setup_family="reversal",
        outcome_status="evaluated",
        pnl_points=-8.0,
        pnl_pct=-0.08,
        evaluated_at="2026-03-11T06:06:00Z",
    )


def test_execution_outcomes_policy_audit_returns_deterministic_path(client):
    _seed_policy_audit_strategy_dataset()

    response = client.get(
        "/execution_outcomes/policy_audit?group_by=strategy&symbol=BTCUSDT.P&audit_horizon_samples=2"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["group_by"] == "strategy"
    assert body["scoring_mode"] == "blended"
    assert body["audit_steps"] == 5

    recommended_path = [row["recommended_cohort"] for row in body["rows"]]
    assert recommended_path == ["alpha", "alpha", "alpha", "beta", "alpha"]

    first_row = body["rows"][0]
    assert first_row["audit_cutoff"] == "2026-03-11T06:01:00Z"
    assert first_row["historical_sample_count"] == 1
    assert first_row["forward_sample_count"] == 1


def test_execution_outcomes_policy_audit_summary_metrics_are_correct(client):
    _seed_policy_audit_strategy_dataset()

    response = client.get(
        "/execution_outcomes/policy_audit_summary?group_by=strategy&symbol=BTCUSDT.P&audit_horizon_samples=2"
    )

    assert response.status_code == 200
    body = response.json()
    assert "rows" not in body
    assert body["audit_steps"] == 5
    summary = body["summary"]
    assert summary["total_steps"] == 5
    assert summary["avg_forward_pnl_points"] == pytest.approx(-13.25)
    assert summary["avg_forward_pnl_pct"] == pytest.approx(-0.1325)
    assert summary["avg_forward_win_rate"] == pytest.approx(0.25)
    assert summary["avg_forward_expectancy_points"] == pytest.approx(-13.25)
    assert summary["avg_forward_expectancy_pct"] == pytest.approx(-0.1325)
    assert summary["recommendation_hit_rate"] == pytest.approx(0.25)


def test_execution_outcomes_policy_audit_csv_returns_expected_headers_and_content(client):
    _seed_policy_audit_strategy_dataset()

    response = client.get(
        "/execution_outcomes/policy_audit.csv?group_by=strategy&symbol=BTCUSDT.P&audit_horizon_samples=2"
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    header = response.text.splitlines()[0]
    assert "audit_cutoff,recommended_cohort,ranking_score" in header
    assert "forward_expectancy_pct" in header

    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 5
    assert rows[0]["recommended_cohort"] == "alpha"


def test_execution_outcomes_policy_audit_scoring_mode_changes_recommendation_path(client):
    _seed_policy_audit_strategy_dataset()

    blended_response = client.get(
        "/execution_outcomes/policy_audit?group_by=strategy&symbol=BTCUSDT.P&audit_horizon_samples=2&scoring_mode=blended"
    )
    expectancy_response = client.get(
        "/execution_outcomes/policy_audit?group_by=strategy&symbol=BTCUSDT.P&audit_horizon_samples=2&scoring_mode=expectancy_pct"
    )

    assert blended_response.status_code == 200
    assert expectancy_response.status_code == 200

    blended_path = [row["recommended_cohort"] for row in blended_response.json()["rows"]]
    expectancy_path = [row["recommended_cohort"] for row in expectancy_response.json()["rows"]]

    assert blended_path == ["alpha", "alpha", "alpha", "beta", "alpha"]
    assert expectancy_path == ["alpha", "alpha", "beta", "beta", "beta"]
