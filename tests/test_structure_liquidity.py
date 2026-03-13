"""Tests for StructureEngine, LiquidityEngine, and their pipeline integration.

Bar data conventions used throughout this file:
  high = close + 0.5
  low  = close - 0.5
  open = previous close (or close on first bar)

This yields a symmetric candle body-less bar whose high/low track the close
price exactly, making pivot detection straightforward to verify by hand.

BEARISH ZIGZAG (30 bars)
------------------------
close sequence:
  100 101 102 103 104 105 104 103 102 101   <- swing high at index 5 (high 105.5)
  100  99  98  99 100 101 102 103 104 103   <- swing low at index 12 (low  97.5)
  102 101 100  99  98  97  98  99 100 101   <- swing high at index 18 (high 104.5)
                                             <- swing low at index 25 (low  96.5)

Structure: LH (104.5 < 105.5) + LL (96.5 < 97.5) => bearish

BULLISH ZIGZAG (30 bars)
------------------------
close sequence:
  105 104 103 102 101 100 101 102 103 104   <- swing low at index 5 (low 99.5)
  105 106 107 106 105 104 103 102 101.5 102.5  <- swing high at index 12 (high 107.5)
  103.5 104.5 105.5 106.5 107.5 108.5 107.5 106.5 105.5 104.5  <- swing high at 25 (high 109.0)
                                                                <- swing low at 18 (low 101.0)

Structure: HH (109.0 > 107.5) + HL (101.0 > 99.5) => bullish
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
from backend.feature_contract import FeatureContext
from backend.feature_engine import run_feature_pipeline_for_latest_bar
from backend.liquidity_engine import (
    EQUAL_CLUSTER_MIN,
    EQ_TOLERANCE_PCT,
    LiquidityEngine,
    _has_equal_cluster,
)
from backend.structure_engine import StructureEngine, find_swing_pivots

# --------------------------------------------------------------------------- #
# Shared bar data
# --------------------------------------------------------------------------- #

_BEARISH_CLOSES = [
    100, 101, 102, 103, 104, 105, 104, 103, 102, 101,
    100,  99,  98,  99, 100, 101, 102, 103, 104, 103,
    102, 101, 100,  99,  98,  97,  98,  99, 100, 101,
]

_BULLISH_CLOSES = [
    105.0, 104.0, 103.0, 102.0, 101.0, 100.0, 101.0, 102.0, 103.0, 104.0,
    105.0, 106.0, 107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.5, 102.5,
    103.5, 104.5, 105.5, 106.5, 107.5, 108.5, 107.5, 106.5, 105.5, 104.5,
]

# Two swing highs at 105.5 and 105.4 — within 0.15 % of each other.
# The second trough is at 90.4 (vs 97.5 first), so the two swing LOWS are
# ~7.8 % apart and do NOT form an equal-lows cluster.
_EQUAL_HIGHS_CLOSES = [
    100.0, 101.0, 102.0, 103.0, 104.0, 105.0,   # peak at 5 (h=105.5)
    104.0, 103.0, 102.0, 101.0, 100.0,  99.0,
     98.0,  99.0, 100.0, 101.0, 102.0, 103.0,   # trough at 12 (l=97.5)
    104.9, 103.9, 102.9,                         # second peak at 18 (h=105.4)
    101.9,  95.9,  92.9,  91.9,  90.9,           # deep trough at 25 (l=90.4)
     91.9,  92.9,  93.9,  94.9,
]


def _make_bars(closes: list[float]) -> list[dict[str, Any]]:
    """Build normalised bar dicts from a list of close prices."""
    bars: list[dict[str, Any]] = []
    for i, close in enumerate(closes):
        open_ = float(closes[i - 1]) if i > 0 else float(close)
        bars.append({
            "id": i,
            "timestamp": f"2026-01-01T00:{i:02d}:00+00:00",
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "open": open_,
            "high": float(close) + 0.5,
            "low": float(close) - 0.5,
            "close": float(close),
            "volume": 1000.0,
        })
    return bars


def _make_context(bars: list[dict]) -> FeatureContext:
    return FeatureContext(
        symbol="BTCUSDT.P",
        timeframe="1m",
        bars=bars,
    )


# --------------------------------------------------------------------------- #
# Helpers / fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_structure_liquidity.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", "test-signal-key")

    event_writer.init_db()
    return test_db_path


@pytest.fixture()
def client(isolated_db):
    with TestClient(app) as test_client:
        yield test_client


def _seed_zigzag_bars(
    *,
    symbol: str = "BTCUSDT.P",
    timeframe: str = "1m",
    count: int = 80,
) -> int:
    """Insert bars whose close follows a sine wave with a 16-bar period.

    This produces unambiguous swing pivots roughly every 8 bars.
    Returns the id of the last inserted bar.
    """
    base = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
    latest_id = 0
    period = 16
    amplitude = 200.0
    mid_price = 50000.0

    for index in range(count):
        phase = index * 2 * math.pi / period
        close_price = mid_price + amplitude * math.sin(phase)
        open_price = close_price  # flat body; high/low purely track close
        # Use close ± fixed offset so the high column tracks the close exactly.
        # This avoids the tie that arises when open_next == close_prev at the
        # peak bar, which would make two adjacent bars share the same max(open,
        # close) and thus the same high.
        high_price = close_price + 80.0
        low_price = close_price - 80.0
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
            participation_score=0.6,
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


# =========================================================================== #
# Unit tests: find_swing_pivots
# =========================================================================== #

def test_find_swing_pivots_returns_empty_when_too_few_bars():
    bars = _make_bars([100, 101, 102, 103, 104, 105])  # only 6 bars, need 7
    swing_highs, swing_lows = find_swing_pivots(bars, lookback=3)
    assert swing_highs == []
    assert swing_lows == []


def test_find_swing_pivots_detects_expected_highs_in_bearish_zigzag():
    bars = _make_bars(_BEARISH_CLOSES)
    swing_highs, _ = find_swing_pivots(bars, lookback=3)

    # Expect pivots at index 5 (high=105.5) and index 18 (high=104.5).
    pivot_indices = [idx for idx, _ in swing_highs]
    assert 5 in pivot_indices
    assert 18 in pivot_indices

    prices_by_idx = dict(swing_highs)
    assert abs(prices_by_idx[5] - 105.5) < 1e-6
    assert abs(prices_by_idx[18] - 104.5) < 1e-6


def test_find_swing_pivots_detects_expected_lows_in_bearish_zigzag():
    bars = _make_bars(_BEARISH_CLOSES)
    _, swing_lows = find_swing_pivots(bars, lookback=3)

    pivot_indices = [idx for idx, _ in swing_lows]
    assert 12 in pivot_indices
    assert 25 in pivot_indices

    prices_by_idx = dict(swing_lows)
    assert abs(prices_by_idx[12] - 97.5) < 1e-6
    assert abs(prices_by_idx[25] - 96.5) < 1e-6


def test_find_swing_pivots_detects_expected_highs_in_bullish_zigzag():
    bars = _make_bars(_BULLISH_CLOSES)
    swing_highs, _ = find_swing_pivots(bars, lookback=3)

    prices = [price for _, price in swing_highs]
    # Two swing highs: 107.5 (index 12) and 109.0 (index 25)
    assert any(abs(p - 107.5) < 1e-6 for p in prices)
    assert any(abs(p - 109.0) < 1e-6 for p in prices)


def test_find_swing_pivots_detects_expected_lows_in_bullish_zigzag():
    bars = _make_bars(_BULLISH_CLOSES)
    _, swing_lows = find_swing_pivots(bars, lookback=3)

    prices = [price for _, price in swing_lows]
    # Two swing lows: 99.5 (index 5) and 101.0 (index 18)
    assert any(abs(p - 99.5) < 1e-6 for p in prices)
    assert any(abs(p - 101.0) < 1e-6 for p in prices)


# =========================================================================== #
# Unit tests: StructureEngine
# =========================================================================== #

def test_structure_engine_defaults_when_insufficient_bars():
    bars = _make_bars([100, 101, 102])
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    assert result["swing_high_flag"] == 0
    assert result["swing_low_flag"] == 0
    assert result["most_recent_swing_high"] == 0.0
    assert result["most_recent_swing_low"] == 0.0
    assert result["structure_trend_state"] == 0.0


def test_structure_engine_lh_ll_bearish_structure():
    bars = _make_bars(_BEARISH_CLOSES)
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    assert result["lower_high_flag"] == 1, "Expect LH (104.5 < 105.5)"
    assert result["lower_low_flag"] == 1, "Expect LL (96.5 < 97.5)"
    assert result["higher_high_flag"] == 0
    assert result["higher_low_flag"] == 0
    assert result["structure_trend_state"] == -1.0
    assert abs(float(str(result["most_recent_swing_high"])) - 104.5) < 1e-6
    assert abs(float(str(result["most_recent_swing_low"])) - 96.5) < 1e-6


def test_structure_engine_hh_hl_bullish_structure():
    bars = _make_bars(_BULLISH_CLOSES)
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    assert result["higher_high_flag"] == 1, "Expect HH"
    assert result["higher_low_flag"] == 1, "Expect HL"
    assert result["lower_high_flag"] == 0
    assert result["lower_low_flag"] == 0
    assert result["structure_trend_state"] == 1.0


def test_structure_engine_no_bos_when_close_within_range():
    bars = _make_bars(_BEARISH_CLOSES)
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    # Last bar close = 101, swing high = 104.5, swing low = 96.5 → no BOS
    assert result["structure_break_bullish"] == 0
    assert result["structure_break_bearish"] == 0
    assert result["choch_bullish"] == 0
    assert result["choch_bearish"] == 0


def test_structure_engine_choch_bullish_when_bearish_structure_breaks_up():
    # Append a bar whose close > most_recent_swing_high (104.5) in bearish context
    closes = list(_BEARISH_CLOSES) + [106.0]
    bars = _make_bars(closes)
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    assert result["structure_break_bullish"] == 1
    assert result["choch_bullish"] == 1, "Bearish structure broken upwards = CHoCH bullish"
    assert result["choch_bearish"] == 0


def test_structure_engine_choch_bearish_when_bullish_structure_breaks_down():
    # Append a bar whose close < most_recent_swing_low (101.0) in bullish context
    closes = list(_BULLISH_CLOSES) + [99.0]
    bars = _make_bars(closes)
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    assert result["structure_break_bearish"] == 1
    assert result["choch_bearish"] == 1, "Bullish structure broken downwards = CHoCH bearish"
    assert result["choch_bullish"] == 0


def test_structure_engine_bos_bullish_in_neutral_or_bullish_context():
    # Move close far above the swing high while structure is neutral (only one
    # swing high found — remove earlier bars so only one complete swing exists).
    # Use just enough bars to get a single swing high, then end with a break.
    minimal_up_down = [
        100, 101, 102, 103, 104, 105,   # swing high at index 5 (h=105.5)
        104, 103, 102,                  # 9 bars total
        106,                            # index 9: close > 105.5 → BOS bull
    ]
    bars = _make_bars(minimal_up_down)
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    assert result["structure_break_bullish"] == 1
    # With only one swing high, choch_bullish requires structure_trend == -1.0
    # which requires LH — not satisfied here; only one swing high exists.
    assert result["choch_bullish"] == 0


def test_structure_engine_swing_flag_fires_at_most_recent_confirmable_pivot():
    # The last swing high should be at n - PIVOT_LOOKBACK - 1 = 30 - 3 - 1 = 26.
    # In the bearish dataset the confirmed pivots end before bar 26 so test a
    # custom single-peak series.
    peak_closes = [100, 101, 102, 103, 102, 101, 100]  # 7 bars; peak at idx 3
    bars = _make_bars(peak_closes)
    engine = StructureEngine()
    result = engine.compute(_make_context(bars))

    # n=7, most_recent_confirmable_idx = 7 - 3 - 1 = 3 (the peak)
    assert result["swing_high_flag"] == 1
    assert abs(float(str(result["most_recent_swing_high"])) - 103.5) < 1e-6


# =========================================================================== #
# Unit tests: LiquidityEngine
# =========================================================================== #

def test_liquidity_engine_defaults_when_no_bars():
    engine = LiquidityEngine()
    result = engine.compute(_make_context([]))

    for key in ["equal_highs_cluster_flag", "liquidity_sweep_high", "liquidity_sweep_low",
                "sweep_reclaim_bullish", "sweep_reclaim_bearish"]:
        assert result[key] == 0

    assert result["stop_run_score"] == 0.0
    assert result["wick_rejection_score"] == 0.0
    assert result["liquidity_pressure_bias"] == 0.0


def test_has_equal_cluster_detects_near_equal_prices():
    # 105.5 and 105.4: rel diff = 0.1/105.4 ≈ 0.095 % < 0.15 %
    assert _has_equal_cluster([105.5, 105.4], EQ_TOLERANCE_PCT) is True


def test_has_equal_cluster_rejects_prices_too_far_apart():
    # 105.5 and 104.4: rel diff = 1.1/104.4 ≈ 1.05 % > 0.15 %
    assert _has_equal_cluster([105.5, 104.4], EQ_TOLERANCE_PCT) is False


def test_liquidity_engine_equal_highs_cluster_flag():
    bars = _make_bars(_EQUAL_HIGHS_CLOSES)
    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["equal_highs_cluster_flag"] == 1, (
        "Swing highs at 105.5 and 105.4 should satisfy the equal-highs cluster"
    )


def test_liquidity_engine_no_equal_highs_when_swings_far_apart():
    bars = _make_bars(_BEARISH_CLOSES)  # swing highs at 105.5 and 104.5 (≈0.96% apart)
    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["equal_highs_cluster_flag"] == 0


def test_liquidity_engine_sweep_high_detected():
    # Bearish zigzag ends with most_recent_swing_high = 104.5.
    # Append a bar: high = 105.0 > 104.5, close = 103.0 < 104.5 ⟹ sweep high.
    closes = list(_BEARISH_CLOSES)
    bars = _make_bars(closes)
    # Override the last bar to create the sweep
    bars.append({
        "id": len(bars),
        "timestamp": "2026-01-01T00:30:00+00:00",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "open": 102.0,
        "high": 105.0,   # above swing high 104.5
        "low": 101.5,
        "close": 103.0,  # closed below swing high 104.5
        "volume": 1000.0,
    })

    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["liquidity_sweep_high"] == 1
    assert result["liquidity_sweep_low"] == 0


def test_liquidity_engine_sweep_low_detected():
    # Bearish zigzag ends with most_recent_swing_low = 96.5.
    # Append a bar: low = 95.0 < 96.5, close = 98.0 > 96.5 ⟹ sweep low.
    closes = list(_BEARISH_CLOSES)
    bars = _make_bars(closes)
    bars.append({
        "id": len(bars),
        "timestamp": "2026-01-01T00:31:00+00:00",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "open": 97.0,
        "high": 100.0,
        "low": 95.0,     # below swing low 96.5
        "close": 98.0,   # closed above swing low 96.5
        "volume": 1000.0,
    })

    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["liquidity_sweep_low"] == 1
    assert result["liquidity_sweep_high"] == 0


def test_liquidity_engine_sweep_reclaim_bullish():
    bars = _make_bars(list(_BEARISH_CLOSES))
    # Sweep low with bullish close (close > open)
    bars.append({
        "id": len(bars),
        "timestamp": "2026-01-01T00:32:00+00:00",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "open": 96.0,    # open below sweep level
        "high": 100.0,
        "low": 95.0,     # wick below swing low 96.5
        "close": 99.0,   # closed above swing low AND above open → bullish
        "volume": 1000.0,
    })

    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["liquidity_sweep_low"] == 1
    assert result["sweep_reclaim_bullish"] == 1
    assert result["sweep_reclaim_bearish"] == 0


def test_liquidity_engine_sweep_reclaim_bearish():
    bars = _make_bars(list(_BEARISH_CLOSES))
    # Sweep high with bearish close (close < open)
    bars.append({
        "id": len(bars),
        "timestamp": "2026-01-01T00:33:00+00:00",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "open": 105.5,   # open above swing high
        "high": 106.0,   # wick above swing high 104.5
        "low": 102.5,
        "close": 103.0,  # closed below swing high AND below open → bearish
        "volume": 1000.0,
    })

    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["liquidity_sweep_high"] == 1
    assert result["sweep_reclaim_bearish"] == 1
    assert result["sweep_reclaim_bullish"] == 0


def test_liquidity_engine_stop_run_score_at_least_0_4_when_sweep_occurs():
    bars = _make_bars(list(_BEARISH_CLOSES))
    bars.append({
        "id": len(bars),
        "timestamp": "2026-01-01T00:34:00+00:00",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "open": 96.5,
        "high": 99.0,
        "low": 95.0,
        "close": 98.0,
        "volume": 1000.0,
    })

    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["stop_run_score"] >= 0.4, "A confirmed sweep must contribute at least 0.40"


def test_liquidity_engine_stop_run_score_below_0_4_without_sweep():
    bars = _make_bars(_BEARISH_CLOSES)  # last close = 101, no sweep
    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["stop_run_score"] < 0.4, "No sweep → score must be below the 0.40 sweep threshold"


def test_liquidity_engine_pressure_bias_negative_when_equal_highs_cluster():
    bars = _make_bars(_EQUAL_HIGHS_CLOSES)
    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    # Equal highs above → potential downside reversal after sweep → negative bias
    assert result["liquidity_pressure_bias"] < 0.0


def test_liquidity_engine_wick_rejection_score_positive_on_sweep_low():
    bars = _make_bars(list(_BEARISH_CLOSES))
    # Lower wick = min(open, close) - low = 97.0 - 95.0 = 2.0
    # Body = abs(close - open) = abs(99.0 - 97.0) = 2.0
    # wick_rejection = 2.0 / 2.0 = 1.0
    bars.append({
        "id": len(bars),
        "timestamp": "2026-01-01T00:35:00+00:00",
        "symbol": "BTCUSDT.P",
        "timeframe": "1m",
        "open": 97.0,
        "high": 100.0,
        "low": 95.0,
        "close": 99.0,
        "volume": 1000.0,
    })

    engine = LiquidityEngine()
    result = engine.compute(_make_context(bars))

    assert result["wick_rejection_score"] > 0.0
    assert result["wick_rejection_score"] <= 10.0  # cap respected


# =========================================================================== #
# Integration tests (isolated SQLite database)
# =========================================================================== #

def test_structure_liquidity_features_persisted_by_pipeline(isolated_db):
    latest_bar_id = _seed_zigzag_bars(count=80)

    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_bar_id,
        lookback=300,
    )

    assert result is not None
    assert result["snapshot_id"] > 0

    feature_values = result["feature_values"]

    # Structure keys must be present
    for key in [
        "swing_high_flag", "swing_low_flag",
        "most_recent_swing_high", "most_recent_swing_low",
        "higher_high_flag", "higher_low_flag",
        "lower_high_flag", "lower_low_flag",
        "structure_break_bullish", "structure_break_bearish",
        "choch_bullish", "choch_bearish", "structure_trend_state",
    ]:
        assert key in feature_values, f"Missing structure feature key: {key}"

    # Liquidity keys must be present
    for key in [
        "equal_highs_cluster_flag", "equal_lows_cluster_flag",
        "liquidity_sweep_high", "liquidity_sweep_low",
        "sweep_reclaim_bullish", "sweep_reclaim_bearish",
        "stop_run_score", "wick_rejection_score", "liquidity_pressure_bias",
    ]:
        assert key in feature_values, f"Missing liquidity feature key: {key}"

    # With 80 bars of a sine-wave series, at least one confirmed swing must exist.
    swing_high_price = float(str(feature_values["most_recent_swing_high"]))
    swing_low_price = float(str(feature_values["most_recent_swing_low"]))
    assert swing_high_price > 0.0, "Sine-wave data must produce at least one swing high"
    assert swing_low_price > 0.0, "Sine-wave data must produce at least one swing low"

    # Verify persistence in DB
    with event_writer.get_connection() as conn:
        value_count = int(conn.execute("SELECT COUNT(*) AS c FROM feature_snapshot_values").fetchone()["c"])
        registry_count = int(conn.execute("SELECT COUNT(*) AS c FROM feature_registry").fetchone()["c"])
        snapshot_row = conn.execute(
            "SELECT feature_json FROM feature_snapshots WHERE id = ?",
            (result["snapshot_id"],),
        ).fetchone()

    # 41 prior + 13 structure + 9 liquidity = 63 total features
    assert registry_count >= 60, f"Expected >= 60 registry entries, got {registry_count}"
    assert value_count >= 60, f"Expected >= 60 snapshot values, got {value_count}"

    persisted_features = json.loads(snapshot_row["feature_json"])
    assert "most_recent_swing_high" in persisted_features
    assert "liquidity_sweep_low" in persisted_features


def test_structure_liquidity_features_attached_to_bar_state(isolated_db):
    latest_bar_id = _seed_zigzag_bars(count=80)

    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_bar_id,
        lookback=300,
    )
    assert result is not None

    with event_writer.get_connection() as conn:
        row = conn.execute(
            "SELECT payload_json FROM bar_states WHERE id = ?",
            (latest_bar_id,),
        ).fetchone()

    payload = json.loads(row["payload_json"])
    computed_features = payload.get("computed_features", {})

    assert "most_recent_swing_high" in computed_features
    assert "most_recent_swing_low" in computed_features
    assert "liquidity_pressure_bias" in computed_features
    assert "stop_run_score" in computed_features

    # regime and model outputs still written correctly
    assert "computed_regime" in payload
    assert "computed_model" in payload
    assert "regime_id" in payload["computed_regime"]
    assert "long_probability" in payload["computed_model"]


def test_webhook_pipeline_includes_structure_liquidity_outputs(client):
    base_timestamp = datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc)
    period = 16
    amplitude = 150.0
    mid_price = 30000.0
    last_response = None

    for index in range(60):
        phase = index * 2 * math.pi / period
        close_price = mid_price + amplitude * math.sin(phase)
        open_price = close_price  # flat body so high tracks close exactly
        high_price = close_price + 60.0
        low_price = close_price - 60.0

        payload = {
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "timestamp": (base_timestamp + timedelta(minutes=index)).isoformat(),
            "long_score": 0.6,
            "short_score": 0.3,
            "no_trade_score": 0.1,
            "pressure_index": 0.55,
            "participation_score": 0.6,
            "confidence_seed": 0.5,
            "payload_json": {
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": 1000.0 + index * 5,
            },
        }

        response = client.post("/webhook/tradingview", json=payload)
        assert response.status_code == 201
        last_response = response.json()

    assert last_response is not None
    assert "feature_snapshot_id" in last_response

    with event_writer.get_connection() as conn:
        latest_bar_row = conn.execute(
            "SELECT payload_json FROM bar_states ORDER BY id DESC LIMIT 1"
        ).fetchone()
        value_rows = conn.execute(
            "SELECT feature_key FROM feature_snapshot_values"
        ).fetchall()

    keys_in_db = {row["feature_key"] for row in value_rows}
    assert "most_recent_swing_high" in keys_in_db
    assert "most_recent_swing_low" in keys_in_db
    assert "liquidity_sweep_high" in keys_in_db
    assert "stop_run_score" in keys_in_db
    assert "structure_trend_state" in keys_in_db

    payload_dict = json.loads(latest_bar_row["payload_json"])
    computed = payload_dict.get("computed_features", {})
    assert "swing_high_flag" in computed
    assert "equal_highs_cluster_flag" in computed
