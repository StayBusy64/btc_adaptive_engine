import json
from pathlib import Path

import pytest

from backend import event_writer, outcome_evaluator
from backend.outcome_evaluator import EvaluatorConfig, OutcomeEvaluator


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_outcome_evaluator.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)

    event_writer.init_db()
    return test_db_path


def _insert_filled_journal(*, signal_id: str, symbol: str, direction: str, entry_price: float, timestamp: str) -> int:
    candidate_id = event_writer.insert_trade_candidate(
        {
            "signal_id": signal_id,
            "timestamp": timestamp,
            "symbol": symbol,
            "direction": direction,
            "entry_price": entry_price,
            "confidence": 0.9,
            "setup_family": "momentum",
            "payload_json": {"strategy": "adaptive-v2", "source": "unit-test"},
        }
    )

    return event_writer.insert_execution_journal_entry(
        candidate_id=candidate_id,
        signal_id=signal_id,
        worker_id="worker-eval",
        action="simulation_decision",
        execution_status="filled",
        execution_note="filled-for-evaluation",
        confidence=0.9,
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        created_at="2026-03-10T10:01:00Z",
    )


def _build_evaluator(window_minutes: int = 15) -> OutcomeEvaluator:
    return OutcomeEvaluator(
        EvaluatorConfig(
            evaluation_window_minutes=window_minutes,
            poll_interval_seconds=0.0,
            oneshot=True,
            batch_limit=100,
            symbol_filter=None,
            bar_limit=5000,
        ),
        sleep_fn=lambda _: None,
    )


def test_outcome_evaluator_creates_evaluated_outcome_with_metrics(isolated_db):
    journal_id = _insert_filled_journal(
        signal_id="sig-outcome-001",
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=100.0,
        timestamp="2026-03-10T10:00:00Z",
    )

    event_writer.insert_bar_state(
        timestamp="2026-03-10T10:05:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        payload_json={"high": 102.0, "low": 99.0, "close": 101.0},
    )
    event_writer.insert_bar_state(
        timestamp="2026-03-10T10:10:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        payload_json={"high": 104.0, "low": 100.0, "close": 103.0},
    )

    evaluator = _build_evaluator(window_minutes=15)
    processed = evaluator.evaluate_once()

    assert processed == 1

    outcomes = event_writer.get_recent_execution_outcomes(10)
    assert len(outcomes) == 1

    outcome = outcomes[0]
    assert outcome["journal_id"] == journal_id
    assert outcome["outcome_status"] == "evaluated"
    assert outcome["exit_price"] == pytest.approx(103.0)
    assert outcome["pnl_points"] == pytest.approx(3.0)
    assert outcome["pnl_pct"] == pytest.approx(3.0)
    assert outcome["max_favorable_excursion"] == pytest.approx(4.0)
    assert outcome["max_adverse_excursion"] == pytest.approx(-1.0)


def test_outcome_evaluator_marks_insufficient_data_when_no_bar_points(isolated_db):
    _insert_filled_journal(
        signal_id="sig-outcome-002",
        symbol="BTCUSDT.P",
        direction="long",
        entry_price=100.0,
        timestamp="2026-03-10T11:00:00Z",
    )

    evaluator = _build_evaluator(window_minutes=10)
    processed = evaluator.evaluate_once()

    assert processed == 1

    outcomes = event_writer.get_recent_execution_outcomes(10)
    assert len(outcomes) == 1

    outcome = outcomes[0]
    assert outcome["outcome_status"] == "insufficient_data"
    assert outcome["pnl_points"] is None

    metadata = json.loads(outcome["metadata_json"]) if outcome.get("metadata_json") else {}
    assert metadata.get("reason") == "no_price_points"


def test_outcome_evaluator_skips_duplicate_journal_outcomes(isolated_db):
    _insert_filled_journal(
        signal_id="sig-outcome-003",
        symbol="BTCUSDT.P",
        direction="short",
        entry_price=200.0,
        timestamp="2026-03-10T12:00:00Z",
    )

    event_writer.insert_bar_state(
        timestamp="2026-03-10T12:03:00Z",
        symbol="BTCUSDT.P",
        timeframe="1m",
        payload_json={"high": 201.0, "low": 198.0, "close": 199.0},
    )

    evaluator = _build_evaluator(window_minutes=10)
    first_processed = evaluator.evaluate_once()
    second_processed = evaluator.evaluate_once()

    assert first_processed == 1
    assert second_processed == 0

    outcomes = event_writer.get_recent_execution_outcomes(10)
    assert len(outcomes) == 1


def test_outcome_evaluator_contains_no_broker_sdk_imports():
    source = Path(outcome_evaluator.__file__).read_text(encoding="utf-8")

    forbidden_tokens = ["ccxt", "alpaca", "binance", "bybit", "kraken"]
    for token in forbidden_tokens:
        assert token not in source
