"""Tests for OrderFlowEngine.

Bar construction conventions
-----------------------------
All hand-crafted bar dicts use the normalised format:
    {"open", "high", "low", "close", "volume"}

OrderFlow notation
------------------
The Close Location Value (CLV) used internally maps the close position
within the bar range to [-1, +1]:
    CLV = (2 * close - high - low) / (high - low)

A bar closing at its high yields CLV = +1 (full buying pressure).
A bar closing at its low  yields CLV = -1 (full selling pressure).
A doji (high == low) falls back to the open-vs-close sign.
"""
from __future__ import annotations

from typing import Any

import pytest

from backend.feature_contract import FeatureContext
from backend.orderflow_engine import (
    DELTA_LOOKBACK,
    EXHAUSTION_BODY_RATIO_MAX,
    EXHAUSTION_RANGE_MULTIPLE,
    EXHAUSTION_VOLUME_MULTIPLE,
    MIN_BARS_REQUIRED,
    OrderFlowEngine,
    _clv,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _bar(
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float = 1000.0,
    ts: str = "2026-01-01T00:00:00+00:00",
) -> dict[str, Any]:
    return {
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "timestamp": ts,
    }


def _bullish_bar(*, volume: float = 1000.0) -> dict[str, Any]:
    """Bar closing at its high (CLV = +1, full buying pressure)."""
    return _bar(open_=100.0, high=102.0, low=99.0, close=102.0, volume=volume)


def _bearish_bar(*, volume: float = 1000.0) -> dict[str, Any]:
    """Bar closing at its low (CLV = -1, full selling pressure)."""
    return _bar(open_=102.0, high=103.0, low=99.0, close=99.0, volume=volume)


def _neutral_bar(*, volume: float = 1000.0) -> dict[str, Any]:
    """Bar closing at its midpoint (CLV ≈ 0)."""
    return _bar(open_=100.0, high=102.0, low=98.0, close=100.0, volume=volume)


def _flat_bar(*, close: float = 100.0, volume: float = 1000.0) -> dict[str, Any]:
    """Doji: high == low == open == close."""
    return _bar(open_=close, high=close, low=close, close=close, volume=volume)


def _make_context(bars: list[dict[str, Any]]) -> FeatureContext:
    return FeatureContext(symbol="BTCUSDT.P", timeframe="1m", bars=bars)


def _default_bars(n: int = 25, *, close: float = 100.0) -> list[dict[str, Any]]:
    """Return *n* neutral baseline bars."""
    return [_neutral_bar() for _ in range(n)]


# --------------------------------------------------------------------------- #
# _clv unit tests
# --------------------------------------------------------------------------- #


def test_clv_close_at_high():
    assert _clv(99.0, 102.0, 99.0, 102.0) == pytest.approx(1.0)


def test_clv_close_at_low():
    assert _clv(102.0, 102.0, 99.0, 99.0) == pytest.approx(-1.0)


def test_clv_close_at_mid():
    assert _clv(99.0, 102.0, 96.0, 99.0) == pytest.approx(0.0)


def test_clv_flat_bar_bullish():
    # high == low, close > open → +1
    assert _clv(100.0, 100.0, 100.0, 101.0) == pytest.approx(1.0)


def test_clv_flat_bar_bearish():
    # high == low, close < open → -1
    assert _clv(101.0, 101.0, 101.0, 100.0) == pytest.approx(-1.0)


def test_clv_flat_bar_neutral():
    # high == low == open == close → 0
    assert _clv(100.0, 100.0, 100.0, 100.0) == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# Engine spec / contract tests
# --------------------------------------------------------------------------- #


def test_engine_has_name():
    engine = OrderFlowEngine()
    assert engine.name == "orderflow_engine"


def test_engine_specs_keys():
    engine = OrderFlowEngine()
    keys = {s.key for s in engine.specs()}
    expected = {
        "buying_pressure",
        "selling_pressure",
        "bid_ask_imbalance",
        "volume_delta_proxy",
        "cumulative_delta_10",
        "cumulative_delta_slope",
        "volume_exhaustion_flag",
        "delta_divergence_flag",
    }
    assert keys == expected


def test_engine_specs_engine_field():
    engine = OrderFlowEngine()
    for spec in engine.specs():
        assert spec.engine == "orderflow_engine"


# --------------------------------------------------------------------------- #
# Default / edge-case behaviour
# --------------------------------------------------------------------------- #


def test_returns_defaults_for_empty_bars():
    engine = OrderFlowEngine()
    result = engine.compute(_make_context([]))
    assert result["buying_pressure"] == 0.5
    assert result["selling_pressure"] == 0.5
    assert result["bid_ask_imbalance"] == 0.0
    assert result["volume_exhaustion_flag"] == 0
    assert result["delta_divergence_flag"] == 0


def test_returns_defaults_below_min_bars():
    engine = OrderFlowEngine()
    bars = [_neutral_bar() for _ in range(MIN_BARS_REQUIRED - 1)]
    result = engine.compute(_make_context(bars))
    assert result["buying_pressure"] == 0.5


# --------------------------------------------------------------------------- #
# Buying / selling pressure & bid_ask_imbalance
# --------------------------------------------------------------------------- #


def test_bullish_bar_has_high_buying_pressure():
    engine = OrderFlowEngine()
    bars = _default_bars(20) + [_bullish_bar()]
    result = engine.compute(_make_context(bars))
    assert result["buying_pressure"] == pytest.approx(1.0)
    assert result["selling_pressure"] == pytest.approx(0.0)
    assert result["bid_ask_imbalance"] == pytest.approx(1.0)


def test_bearish_bar_has_high_selling_pressure():
    engine = OrderFlowEngine()
    bars = _default_bars(20) + [_bearish_bar()]
    result = engine.compute(_make_context(bars))
    assert result["selling_pressure"] == pytest.approx(1.0)
    assert result["buying_pressure"] == pytest.approx(0.0)
    assert result["bid_ask_imbalance"] == pytest.approx(-1.0)


def test_neutral_bar_has_balanced_pressures():
    engine = OrderFlowEngine()
    bars = _default_bars(20) + [_neutral_bar()]
    result = engine.compute(_make_context(bars))
    # buying + selling should sum to 1.0
    assert result["buying_pressure"] + result["selling_pressure"] == pytest.approx(1.0)


def test_buying_selling_pressure_sum_to_one():
    engine = OrderFlowEngine()
    for bar in [_bullish_bar(), _bearish_bar(), _neutral_bar()]:
        bars = _default_bars(20) + [bar]
        result = engine.compute(_make_context(bars))
        assert result["buying_pressure"] + result["selling_pressure"] == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# Volume delta proxy
# --------------------------------------------------------------------------- #


def test_volume_delta_proxy_positive_for_bullish_bar():
    engine = OrderFlowEngine()
    bars = _default_bars(20) + [_bullish_bar(volume=500.0)]
    result = engine.compute(_make_context(bars))
    # CLV=1, volume=500 → delta = 500
    assert result["volume_delta_proxy"] == pytest.approx(500.0)


def test_volume_delta_proxy_negative_for_bearish_bar():
    engine = OrderFlowEngine()
    bars = _default_bars(20) + [_bearish_bar(volume=500.0)]
    result = engine.compute(_make_context(bars))
    # CLV=-1, volume=500 → delta = -500
    assert result["volume_delta_proxy"] == pytest.approx(-500.0)


def test_volume_delta_proxy_zero_for_neutral_bar():
    engine = OrderFlowEngine()
    bars = _default_bars(20) + [_neutral_bar()]
    result = engine.compute(_make_context(bars))
    assert result["volume_delta_proxy"] == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# Cumulative delta
# --------------------------------------------------------------------------- #


def test_cumulative_delta_positive_for_all_bullish_window():
    engine = OrderFlowEngine()
    # Fill the window entirely with bullish bars
    bars = [_bullish_bar() for _ in range(DELTA_LOOKBACK + 5)]
    result = engine.compute(_make_context(bars))
    assert result["cumulative_delta_10"] > 0.0


def test_cumulative_delta_negative_for_all_bearish_window():
    engine = OrderFlowEngine()
    bars = [_bearish_bar() for _ in range(DELTA_LOOKBACK + 5)]
    result = engine.compute(_make_context(bars))
    assert result["cumulative_delta_10"] < 0.0


def test_cumulative_delta_near_zero_for_mixed_window():
    engine = OrderFlowEngine()
    # Alternating bullish / bearish bars with equal volume → net ~0
    alternating = []
    for _ in range(DELTA_LOOKBACK // 2 + 3):
        alternating.append(_bullish_bar(volume=1000.0))
        alternating.append(_bearish_bar(volume=1000.0))
    result = engine.compute(_make_context(alternating))
    assert abs(result["cumulative_delta_10"]) < 0.1


# --------------------------------------------------------------------------- #
# Cumulative delta slope
# --------------------------------------------------------------------------- #


def test_slope_positive_when_delta_rising():
    engine = OrderFlowEngine()
    # First half: bearish low delta; second half: bullish high delta
    bars = [_bearish_bar(volume=100.0) for _ in range(6)] + [
        _bullish_bar(volume=100.0) for _ in range(6)
    ]
    result = engine.compute(_make_context(bars))
    assert result["cumulative_delta_slope"] == 1.0


def test_slope_negative_when_delta_falling():
    engine = OrderFlowEngine()
    bars = [_bullish_bar(volume=100.0) for _ in range(6)] + [
        _bearish_bar(volume=100.0) for _ in range(6)
    ]
    result = engine.compute(_make_context(bars))
    assert result["cumulative_delta_slope"] == -1.0


# --------------------------------------------------------------------------- #
# Volume exhaustion flag
# --------------------------------------------------------------------------- #


def _exhaustion_bar(*, volume_multiple: float = 2.0) -> dict[str, Any]:
    """Return a bar that should trigger the exhaustion flag.

    Properties:
        range  = 3.0  (≥ 1.5 × avg_range of 1.0 baseline bars)
        body   = 0.1  (≤ 25 % of range → body_ratio ≈ 0.033)
        volume = 1000 * volume_multiple  (≥ 1.2 × avg_volume of 1000)
    """
    return _bar(
        open_=100.05,
        high=101.5,
        low=98.5,  # range = 3.0
        close=100.15,  # body ≈ 0.1
        volume=1000.0 * volume_multiple,
    )


def _flat_range_baseline(n: int = 20) -> list[dict[str, Any]]:
    """Baseline bars with range 1.0, volume 1000."""
    return [_bar(open_=99.5, high=100.5, low=99.5, close=100.0, volume=1000.0) for _ in range(n)]


def test_volume_exhaustion_flag_raised():
    engine = OrderFlowEngine()
    bars = _flat_range_baseline(20) + [_exhaustion_bar(volume_multiple=2.0)]
    result = engine.compute(_make_context(bars))
    assert result["volume_exhaustion_flag"] == 1


def test_volume_exhaustion_not_raised_for_normal_bar():
    engine = OrderFlowEngine()
    bars = _flat_range_baseline(20) + [_neutral_bar()]
    result = engine.compute(_make_context(bars))
    assert result["volume_exhaustion_flag"] == 0


def test_volume_exhaustion_not_raised_when_large_body():
    engine = OrderFlowEngine()
    # Large body (close near high) should NOT trigger exhaustion
    big_body = _bar(open_=99.5, high=104.5, low=99.5, close=104.0, volume=3000.0)
    bars = _flat_range_baseline(20) + [big_body]
    result = engine.compute(_make_context(bars))
    assert result["volume_exhaustion_flag"] == 0


# --------------------------------------------------------------------------- #
# Delta divergence flag
# --------------------------------------------------------------------------- #


def test_delta_divergence_flag_raised_when_price_rises_delta_falls():
    engine = OrderFlowEngine()
    # Build a window where the first half has strong positive delta (bullish bars)
    # and the second half turns strongly negative (bearish bars), while the
    # latest bar is *slightly* bullish (close > open) → price_rising=+1 conflicts
    # with delta_slope=-1.
    #
    # Window size = DELTA_LOOKBACK = 10.
    # Layout (last 10 bars):
    #   idx 0-4 : 5 heavily bullish bars  → first_half_sum >> 0
    #   idx 5-8 : 4 heavily bearish bars  → second_half partial sum << 0
    #   idx 9   : slightly bullish bar    → price_rising=+1; adds small positive
    #             but second_half still < first_half → slope = -1
    n_baseline = 20
    baseline = _default_bars(n_baseline)
    first_half = [_bullish_bar(volume=2000.0) for _ in range(5)]
    second_half_bearish = [_bearish_bar(volume=2000.0) for _ in range(4)]
    # Slightly bullish: close marginally above open, range 3 → body_ratio < 0.1
    latest = _bar(open_=100.0, high=103.0, low=99.0, close=100.2, volume=100.0)
    bars = baseline + first_half + second_half_bearish + [latest]
    result = engine.compute(_make_context(bars))
    assert result["delta_divergence_flag"] == 1


def test_delta_divergence_not_raised_when_price_and_delta_agree():
    engine = OrderFlowEngine()
    # All bullish: price rises AND delta rises → no divergence
    bars = [_bullish_bar() for _ in range(15)]
    result = engine.compute(_make_context(bars))
    assert result["delta_divergence_flag"] == 0


# --------------------------------------------------------------------------- #
# Output range / type safety
# --------------------------------------------------------------------------- #


def test_output_values_are_within_valid_ranges():
    engine = OrderFlowEngine()
    bars = _default_bars(30)
    result = engine.compute(_make_context(bars))

    assert 0.0 <= result["buying_pressure"] <= 1.0
    assert 0.0 <= result["selling_pressure"] <= 1.0
    assert -1.0 <= result["bid_ask_imbalance"] <= 1.0
    assert result["volume_exhaustion_flag"] in (0, 1)
    assert result["delta_divergence_flag"] in (0, 1)
    assert result["cumulative_delta_slope"] in (-1.0, 0.0, 1.0)


def test_all_keys_present_in_output():
    engine = OrderFlowEngine()
    bars = _default_bars(30)
    result = engine.compute(_make_context(bars))
    expected_keys = {s.key for s in engine.specs()}
    assert set(result.keys()) == expected_keys
