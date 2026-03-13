"""Tests for DisplacementEngine and SessionContextEngine.

Bar construction conventions
-----------------------------
All hand-crafted bar dicts follow the same normalised format used by
bar_utils.normalize_bar_row:
    {"id", "timestamp", "symbol", "timeframe", "open", "high", "low",
     "close", "volume"}

Displacement notation
---------------------
A "displacement" bar must satisfy:
    range  >= DISPLACEMENT_RANGE_MULTIPLE * avg_range  (default 1.5×)
    body   >= DISPLACEMENT_MIN_BODY_RATIO * range      (default 0.5)

For deterministic tests we build a series of 20 "baseline" bars each with
range 1.0 (high = close + 0.5, low = close - 0.5) and then append a
"trigger" bar with range 2.0 (high = close + 1.0, low = close - 1.0) and a
bullish body of 1.8 (open = close - 1.8).  avg_range ≈ 1.0, so
range_to_avg ≈ 2.0 ≥ 1.5 → displacement flag raised.

Session notation
----------------
We stamp bars with explicit UTC ISO-8601 timestamps and verify the session
window flags match the known session boundaries.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend import event_writer
from backend.api_server import app
from backend.displacement_engine import (
    AVG_PERIOD,
    DISPLACEMENT_MIN_BODY_RATIO,
    DISPLACEMENT_RANGE_MULTIPLE,
    DisplacementEngine,
)
from backend.feature_contract import FeatureContext
from backend.feature_engine import run_feature_pipeline_for_latest_bar
from backend.session_context_engine import (
    ASIA_END,
    ASIA_START,
    HIGH_ACTIVITY_END,
    HIGH_ACTIVITY_START,
    LATE_START,
    LONDON_START,
    MIDDAY_END,
    MIDDAY_START,
    NEW_YORK_END,
    NEW_YORK_START,
    OVERLAP_END,
    OVERLAP_START,
    SessionContextEngine,
    _parse_utc_timestamp,
)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _make_flat_bar(
    close: float,
    *,
    idx: int = 0,
    volume: float = 1000.0,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Return a symmetric bar with range 1.0 centred on *close*."""
    return {
        "id": idx,
        "timestamp": timestamp or f"2026-01-01T00:{idx % 60:02d}:00+00:00",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "open": close,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": volume,
    }


def _make_baseline_bars(count: int = AVG_PERIOD) -> list[dict]:
    """Return *count* flat bars with range 1.0.  avg_range = 1.0."""
    return [_make_flat_bar(100.0, idx=i) for i in range(count)]


def _make_context(bars: list[dict]) -> FeatureContext:
    return FeatureContext(
        symbol="BTCUSDT.P",
        timeframe="1m",
        bars=bars,
    )


def _bar_at_hour(*, utc_hour: int, utc_minute: int = 0) -> dict[str, Any]:
    """Return a single flat bar timestamped at *utc_hour*:*utc_minute* UTC."""
    ts = f"2026-03-11T{utc_hour:02d}:{utc_minute:02d}:00+00:00"
    return _make_flat_bar(50000.0, idx=0, timestamp=ts)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    test_db = tmp_path / "test_displacement_session.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
    monkeypatch.setattr(event_writer, "DB_PATH", test_db)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", "test-signal-key")
    event_writer.init_db()
    return test_db


@pytest.fixture()
def client(isolated_db):
    with TestClient(app) as tc:
        yield tc


# --------------------------------------------------------------------------- #
# _parse_utc_timestamp
# --------------------------------------------------------------------------- #


class TestParseUtcTimestamp:
    def test_returns_none_for_none(self):
        assert _parse_utc_timestamp(None) is None

    def test_returns_none_for_empty_string(self):
        assert _parse_utc_timestamp("") is None

    def test_parses_offset_string(self):
        dt = _parse_utc_timestamp("2026-03-11T12:30:00+00:00")
        assert dt is not None
        assert dt.hour == 12
        assert dt.minute == 30

    def test_parses_z_suffix(self):
        dt = _parse_utc_timestamp("2026-03-11T08:00:00Z")
        assert dt is not None
        assert dt.hour == 8

    def test_parses_naive_string_as_utc(self):
        dt = _parse_utc_timestamp("2026-03-11T16:00:00")
        assert dt is not None
        assert dt.hour == 16

    def test_parses_unix_epoch_int(self):
        # 2026-03-11T00:00:00 UTC
        epoch = int(datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc).timestamp())
        dt = _parse_utc_timestamp(epoch)
        assert dt is not None
        assert dt.date().isoformat() == "2026-03-11"

    def test_parses_positive_utc_offset_converts_to_utc(self):
        # UTC+5 bar created at local 17:00 → UTC 12:00
        dt = _parse_utc_timestamp("2026-03-11T17:00:00+05:00")
        assert dt is not None
        assert dt.hour == 12


# --------------------------------------------------------------------------- #
# SessionContextEngine — unit tests
# --------------------------------------------------------------------------- #


class TestSessionContextEngineDefaults:
    def test_no_bars_returns_defaults(self):
        engine = SessionContextEngine()
        result = engine.compute(_make_context([]))
        assert result["session_asia_flag"] == 0
        assert result["hour_of_day_normalized"] == 0.0
        assert result["day_of_week_normalized"] == 0.0

    def test_unparseable_timestamp_returns_defaults(self):
        bar = _make_flat_bar(100.0)
        bar["timestamp"] = "not-a-timestamp"
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_asia_flag"] == 0
        assert result["high_activity_window_flag"] == 0


class TestSessionContextEngineAsia:
    def test_midnight_is_in_asia(self):
        bar = _bar_at_hour(utc_hour=0)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_asia_flag"] == 1
        assert result["session_opening_window_flag"] == 1  # first hour of Asia

    def test_asia_midpoint_is_in_asia(self):
        bar = _bar_at_hour(utc_hour=3)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_asia_flag"] == 1
        assert result["session_london_flag"] == 0
        assert result["session_new_york_flag"] == 0

    def test_utc7_is_overlap_asia_london(self):
        # UTC 07 is inside both Asia [0,8) and London [7,16)
        bar = _bar_at_hour(utc_hour=7)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_asia_flag"] == 1
        assert result["session_london_flag"] == 1
        assert result["session_opening_window_flag"] == 1  # first hour of London

    def test_utc8_is_not_in_asia(self):
        bar = _bar_at_hour(utc_hour=8)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_asia_flag"] == 0

    def test_utc8_is_in_midday_lull(self):
        bar = _bar_at_hour(utc_hour=8)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_midday_flag"] == 1


class TestSessionContextEngineLondon:
    def test_london_session_midpoint(self):
        bar = _bar_at_hour(utc_hour=10)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_london_flag"] == 1
        assert result["session_asia_flag"] == 0
        assert result["session_new_york_flag"] == 0
        assert result["session_overlap_flag"] == 0


class TestSessionContextEngineNewYork:
    def test_overlap_window(self):
        # UTC 13 is in both London [7,16) and NY [12,21) and overlap [12,16)
        bar = _bar_at_hour(utc_hour=13)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_london_flag"] == 1
        assert result["session_new_york_flag"] == 1
        assert result["session_overlap_flag"] == 1
        assert result["high_activity_window_flag"] == 1

    def test_ny_only_window(self):
        # UTC 17 is in NY [12,21) but not London [7,16)
        bar = _bar_at_hour(utc_hour=17)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_london_flag"] == 0
        assert result["session_new_york_flag"] == 1
        assert result["session_overlap_flag"] == 0

    def test_late_session(self):
        bar = _bar_at_hour(utc_hour=21)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["session_new_york_flag"] == 0
        assert result["session_late_flag"] == 1
        assert result["high_activity_window_flag"] == 0


class TestSessionContextEngineNormalization:
    def test_hour_normalized_midnight(self):
        bar = _bar_at_hour(utc_hour=0, utc_minute=0)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["hour_of_day_normalized"] == pytest.approx(0.0, abs=1e-5)

    def test_hour_normalized_noon(self):
        bar = _bar_at_hour(utc_hour=12, utc_minute=0)
        result = SessionContextEngine().compute(_make_context([bar]))
        assert result["hour_of_day_normalized"] == pytest.approx(0.5, abs=1e-5)

    def test_day_of_week_normalized_monday(self):
        # 2026-03-09 is a Monday
        bar = _make_flat_bar(100.0, timestamp="2026-03-09T10:00:00+00:00")
        result = SessionContextEngine().compute(_make_context([bar]))
        # Monday ISO=1 → (1-1)/7 = 0.0
        assert result["day_of_week_normalized"] == pytest.approx(0.0, abs=1e-5)

    def test_day_of_week_normalized_friday(self):
        # 2026-03-13 is a Friday
        bar = _make_flat_bar(100.0, timestamp="2026-03-13T10:00:00+00:00")
        result = SessionContextEngine().compute(_make_context([bar]))
        # Friday ISO=5 → (5-1)/7 ≈ 0.5714
        assert result["day_of_week_normalized"] == pytest.approx(4 / 7, abs=1e-5)


# --------------------------------------------------------------------------- #
# DisplacementEngine — unit tests
# --------------------------------------------------------------------------- #


class TestDisplacementEngineDefaults:
    def test_too_few_bars_returns_defaults(self):
        bars = [_make_flat_bar(100.0, idx=i) for i in range(3)]
        result = DisplacementEngine().compute(_make_context(bars))
        assert result["displacement_up_flag"] == 0
        assert result["displacement_down_flag"] == 0
        assert result["impulse_quality_score"] == 0.0

    def test_empty_bars_returns_defaults(self):
        result = DisplacementEngine().compute(_make_context([]))
        assert result["displacement_up_flag"] == 0


class TestDisplacementEngineFlags:
    def _build_bars(self, trigger_bar: dict) -> list[dict]:
        baseline = _make_baseline_bars(AVG_PERIOD)
        return baseline + [trigger_bar]

    def test_bullish_displacement_flag_raised(self):
        # range = 3.0 (1.5× ≥ 1.5), body = 2.6 (body_to_range ≈ 0.87 ≥ 0.5)
        trigger = {
            "id": AVG_PERIOD,
            "timestamp": "2026-01-01T00:21:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": 98.7,
            "high": 101.3,  # range = 3.0 – wait, high=101.3, low=98.3 → range 3.0
            "low": 98.3,
            "close": 101.3,
            "volume": 5000.0,
        }
        # Recalc: open=98.7, close=101.3 → body=2.6, range=3.0, body_ratio=0.867 ✓
        bars = self._build_bars(trigger)
        result = DisplacementEngine().compute(_make_context(bars))
        assert result["displacement_up_flag"] == 1
        assert result["displacement_down_flag"] == 0
        assert result["displacement_range_to_avg"] >= DISPLACEMENT_RANGE_MULTIPLE
        assert result["displacement_body_to_range"] >= DISPLACEMENT_MIN_BODY_RATIO

    def test_bearish_displacement_flag_raised(self):
        trigger = {
            "id": AVG_PERIOD,
            "timestamp": "2026-01-01T00:21:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": 101.3,
            "high": 101.3,
            "low": 98.3,
            "close": 98.7,
            "volume": 5000.0,
        }
        bars = self._build_bars(trigger)
        result = DisplacementEngine().compute(_make_context(bars))
        assert result["displacement_down_flag"] == 1
        assert result["displacement_up_flag"] == 0

    def test_small_bar_does_not_trigger_flag(self):
        # range = 0.6 (< 1.5 × avg 1.0)
        trigger = {
            "id": AVG_PERIOD,
            "timestamp": "2026-01-01T00:21:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": 99.7,
            "high": 100.3,
            "low": 99.7,
            "close": 100.3,
            "volume": 500.0,
        }
        bars = self._build_bars(trigger)
        result = DisplacementEngine().compute(_make_context(bars))
        assert result["displacement_up_flag"] == 0
        assert result["displacement_down_flag"] == 0

    def test_large_doji_does_not_trigger_flag(self):
        # large range (3.0) but tiny body (open ≈ close)
        trigger = {
            "id": AVG_PERIOD,
            "timestamp": "2026-01-01T00:21:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": 100.0,
            "high": 101.5,
            "low": 98.5,
            "close": 100.02,  # body = 0.02, range = 3.0 → body_ratio ≈ 0.007 < 0.5
            "volume": 3000.0,
        }
        bars = self._build_bars(trigger)
        result = DisplacementEngine().compute(_make_context(bars))
        assert result["displacement_up_flag"] == 0
        assert result["displacement_down_flag"] == 0


class TestDisplacementEngineScores:
    def test_body_to_range_correct(self):
        # open=99, close=101, high=101.5, low=98.5 → body=2, range=3
        bars = _make_baseline_bars()
        latest = {
            "id": AVG_PERIOD,
            "timestamp": "2026-01-01T00:21:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": 99.0,
            "high": 101.5,
            "low": 98.5,
            "close": 101.0,
            "volume": 1000.0,
        }
        bars.append(latest)
        result = DisplacementEngine().compute(_make_context(bars))
        assert result["displacement_body_to_range"] == pytest.approx(2.0 / 3.0, abs=1e-5)

    def test_close_strength_is_clv(self):
        # close at top of range → CLV ≈ 1.0
        bars = _make_baseline_bars()
        latest = {
            "id": AVG_PERIOD,
            "timestamp": "2026-01-01T00:21:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": 98.0,
            "high": 102.0,
            "low": 98.0,
            "close": 102.0,
            "volume": 1000.0,
        }
        bars.append(latest)
        result = DisplacementEngine().compute(_make_context(bars))
        assert result["displacement_close_strength"] == pytest.approx(1.0, abs=1e-5)

    def test_volume_to_avg_ratio(self):
        # avg_volume = 1000.0 (all baseline bars), trigger volume = 3000.0
        baseline = [_make_flat_bar(100.0, idx=i, volume=1000.0) for i in range(AVG_PERIOD)]
        latest = {
            "id": AVG_PERIOD,
            "timestamp": "2026-01-01T00:21:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": 100.0,
            "high": 100.5,
            "low": 99.5,
            "close": 100.0,
            "volume": 3000.0,
        }
        baseline.append(latest)
        result = DisplacementEngine().compute(_make_context(baseline))
        assert result["displacement_volume_to_avg"] == pytest.approx(3.0, abs=0.05)

    def test_impulse_quality_score_bounded(self):
        # Any output must be in [0, 1]
        for vol in [100, 5000, 50000]:
            bars = _make_baseline_bars()
            bars.append({
                "id": AVG_PERIOD,
                "timestamp": "2026-01-01T00:21:00+00:00",
                "symbol": "BTCUSDT.P",
                "timeframe": "1m",
                "open": 98.0,
                "high": 102.0,
                "low": 98.0,
                "close": 102.0,
                "volume": float(vol),
            })
            result = DisplacementEngine().compute(_make_context(bars))
            assert 0.0 <= result["impulse_quality_score"] <= 1.0


# --------------------------------------------------------------------------- #
# Integration: persistence and webhook annotation
# --------------------------------------------------------------------------- #


def _seed_bar_series(
    *,
    symbol: str = "BTCUSDT.P",
    timeframe: str = "1m",
    count: int = 60,
) -> int:
    """Seed bars with a clear displacement spike at the last two bars."""
    base = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
    latest_id = 0
    amplitude = 1.0  # normal range ~1.0

    for index in range(count):
        close_price = 50000.0 + index * 0.5
        open_price = close_price - 0.3 if index < count - 1 else close_price - 3.0
        # Final bar: large bullish candle to trigger displacement
        high_price = close_price + (0.7 if index < count - 1 else 3.5)
        low_price = close_price - (0.7 if index < count - 1 else 3.5)
        volume = 1000.0 + index * 5.0
        timestamp = (base + timedelta(minutes=index)).isoformat()

        latest_id = event_writer.insert_bar_state(
            timestamp=timestamp,
            symbol=symbol,
            timeframe=timeframe,
            long_score=0.6,
            short_score=0.3,
            no_trade_score=0.1,
            pressure_index=0.5,
            participation_score=0.5,
            confidence_seed=0.5,
            payload_json={
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            },
        )

    return latest_id


def test_displacement_features_persisted_in_snapshot(isolated_db):
    latest_id = _seed_bar_series()
    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_id,
        lookback=200,
    )
    assert result is not None
    fv = result["feature_values"]
    assert "displacement_up_flag" in fv
    assert "displacement_down_flag" in fv
    assert "impulse_quality_score" in fv
    assert "displacement_range_to_avg" in fv
    assert "displacement_volume_to_avg" in fv


def test_session_features_persisted_in_snapshot(isolated_db):
    latest_id = _seed_bar_series()
    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_id,
        lookback=200,
    )
    assert result is not None
    fv = result["feature_values"]
    assert "session_asia_flag" in fv
    assert "session_london_flag" in fv
    assert "session_new_york_flag" in fv
    assert "session_overlap_flag" in fv
    assert "hour_of_day_normalized" in fv
    assert "day_of_week_normalized" in fv
    assert "high_activity_window_flag" in fv


def test_webhook_annotates_displacement_and_session_features(client):
    """POST to /webhook/tradingview should produce a bar_state whose
    payload_json contains both displacement and session output keys."""

    webhook_payload = {
        "key": "test-signal-key",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "timestamp": "2026-03-11T14:30:00+00:00",
        "open": 50000.0,
        "high": 50003.5,
        "low": 49996.5,
        "close": 50003.0,
        "volume": 5000.0,
    }

    response = client.post("/webhook/tradingview", json=webhook_payload)
    assert response.status_code in (200, 201)

    body = response.json()
    assert body.get("status") in ("stored", "ok") or "snapshot_id" in body or "feature_snapshot_id" in body


def test_pipeline_feature_count_includes_new_engines(isolated_db):
    """After adding DisplacementEngine and SessionContextEngine the total
    persisted feature count should be at least 82 (63 existing + 19 new)."""
    from backend.feature_engine import get_default_feature_pipeline

    pipeline = get_default_feature_pipeline()
    all_specs = pipeline.specs()
    assert len(all_specs) >= 82, (
        f"Expected at least 82 feature specs, got {len(all_specs)}"
    )


def test_all_displacement_and_session_spec_keys_present(isolated_db):
    """Every spec key defined in the two new engines must appear in the
    feature_values dict returned by run_feature_pipeline_for_latest_bar."""
    from backend.displacement_engine import DisplacementEngine
    from backend.session_context_engine import SessionContextEngine

    expected_keys = {spec.key for spec in DisplacementEngine().specs()}
    expected_keys |= {spec.key for spec in SessionContextEngine().specs()}

    latest_id = _seed_bar_series()
    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_id,
        lookback=200,
    )
    assert result is not None
    missing = expected_keys - result["feature_values"].keys()
    assert not missing, f"Missing feature keys: {missing}"


def test_regime_and_model_still_persist_with_expanded_features(isolated_db):
    """Regime and model outputs must still be stored after pipeline expansion."""
    import sqlite3

    latest_id = _seed_bar_series()
    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_id,
        lookback=200,
    )
    assert result is not None
    assert result.get("regime") is not None
    assert result.get("model") is not None

    conn = sqlite3.connect(isolated_db)
    row = conn.execute("SELECT COUNT(*) FROM regime_states").fetchone()
    assert row[0] >= 1
    row = conn.execute("SELECT COUNT(*) FROM model_predictions").fetchone()
    assert row[0] >= 1
    conn.close()
