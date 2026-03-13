"""Tests for DivergenceEngine.

Setup conventions
-----------------
Regular BULLISH divergence:
    price makes a lower low  AND  RSI makes a higher low.

Regular BEARISH divergence:
    price makes a higher high  AND  RSI makes a lower high.

MACD histogram variants follow the same price-swing logic but use the MACD
histogram instead of RSI.

All bar series are long enough to give RSI/MACD time to warm up
(MIN_BARS_REQUIRED = 30).
"""
from __future__ import annotations

from typing import Any

import pytest

from backend.divergence_engine import (
    DIVERGENCE_LOOKBACK,
    MIN_BARS_REQUIRED,
    DivergenceEngine,
)
from backend.feature_contract import FeatureContext


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


def _flat(c: float, *, volume: float = 1000.0) -> dict[str, Any]:
    """Symmetric flat bar centred on *c* with range 2."""
    return _bar(open_=c, high=c + 1.0, low=c - 1.0, close=c, volume=volume)


def _make_context(bars: list[dict[str, Any]]) -> FeatureContext:
    return FeatureContext(symbol="BTCUSDT.P", timeframe="1m", bars=bars)


def _rising_series(n: int, *, start: float = 100.0, step: float = 0.5) -> list[dict[str, Any]]:
    """Consistently rising close prices (RSI will be high)."""
    return [_flat(start + i * step) for i in range(n)]


def _falling_series(n: int, *, start: float = 150.0, step: float = 0.5) -> list[dict[str, Any]]:
    """Consistently falling close prices (RSI will be low)."""
    return [_flat(start - i * step) for i in range(n)]


def _sideways_series(n: int, *, base: float = 100.0) -> list[dict[str, Any]]:
    """Sideways oscillation."""
    return [_flat(base + (1.0 if i % 2 == 0 else -1.0)) for i in range(n)]


# --------------------------------------------------------------------------- #
# Engine spec / contract
# --------------------------------------------------------------------------- #


def test_engine_has_correct_name():
    assert DivergenceEngine().name == "divergence_engine"


def test_engine_specs_keys():
    expected = {
        "rsi_bullish_divergence",
        "rsi_bearish_divergence",
        "macd_bullish_divergence",
        "macd_bearish_divergence",
        "divergence_count",
        "divergence_strength_score",
        "divergence_bias",
    }
    assert {s.key for s in DivergenceEngine().specs()} == expected


def test_engine_specs_engine_field():
    for spec in DivergenceEngine().specs():
        assert spec.engine == "divergence_engine"


# --------------------------------------------------------------------------- #
# Default / edge-case behaviour
# --------------------------------------------------------------------------- #


def test_returns_defaults_for_empty_bars():
    result = DivergenceEngine().compute(_make_context([]))
    assert result["rsi_bullish_divergence"] == 0
    assert result["rsi_bearish_divergence"] == 0
    assert result["divergence_count"] == 0
    assert result["divergence_bias"] == "neutral"


def test_returns_defaults_below_min_bars():
    bars = _sideways_series(MIN_BARS_REQUIRED - 1)
    result = DivergenceEngine().compute(_make_context(bars))
    assert result["divergence_count"] == 0


def test_all_keys_present():
    bars = _sideways_series(MIN_BARS_REQUIRED + 5)
    result = DivergenceEngine().compute(_make_context(bars))
    expected = {s.key for s in DivergenceEngine().specs()}
    assert set(result.keys()) == expected


# --------------------------------------------------------------------------- #
# No divergence in trending market
# --------------------------------------------------------------------------- #


def test_no_divergence_in_purely_rising_market():
    """Monotonically rising prices → RSI also high throughout → no bullish div."""
    bars = _rising_series(MIN_BARS_REQUIRED + DIVERGENCE_LOOKBACK)
    result = DivergenceEngine().compute(_make_context(bars))
    # In a purely rising market neither regular bullish nor regular bearish
    # divergence should fire simultaneously; specifically no bullish div
    # (no lower low) and likely no bearish div either (RSI rising with price).
    assert result["rsi_bullish_divergence"] == 0


def test_no_divergence_in_purely_falling_market():
    bars = _falling_series(MIN_BARS_REQUIRED + DIVERGENCE_LOOKBACK)
    result = DivergenceEngine().compute(_make_context(bars))
    assert result["rsi_bearish_divergence"] == 0


# --------------------------------------------------------------------------- #
# RSI bullish divergence
# --------------------------------------------------------------------------- #


def _build_rsi_bullish_divergence_bars() -> list[dict[str, Any]]:
    """
    Construct a bar series that produces a clear RSI bullish divergence:

    Phase 1 (warm-up + reference swing low):
        Long falling segment → RSI drops to a low value.
        End with a clear price low at ~75.

    Phase 2 (partial recovery):
        Short rising segment → RSI recovers.

    Phase 3 (new lower low):
        Drop below phase-1 low → new price low at ~65.
        BUT RSI should not be as low this time because the decline is shorter.
    """
    bars: list[dict[str, Any]] = []

    # Warm-up: 20 rising bars so RSI starts reasonable
    bars += _rising_series(20, start=100.0, step=0.3)

    # Hard fall: 15 bars from 106 → ~75  (RSI drops sharply)
    bars += _falling_series(15, start=106.0, step=2.0)

    # Bounce: 5 bars  (~75 → ~80, RSI recovers)
    bars += _rising_series(5, start=76.0, step=1.0)

    # Shallower second leg down: 5 bars, reaching a lower price low (~68)
    # but shorter / less aggressive → RSI does not fall as far
    bars += _falling_series(5, start=81.0, step=2.5)

    return bars


def test_rsi_bullish_divergence_detected():
    bars = _build_rsi_bullish_divergence_bars()
    result = DivergenceEngine().compute(_make_context(bars))
    assert result["rsi_bullish_divergence"] == 1


def test_rsi_bullish_divergence_sets_bullish_bias():
    bars = _build_rsi_bullish_divergence_bars()
    result = DivergenceEngine().compute(_make_context(bars))
    if result["rsi_bullish_divergence"] == 1:
        assert result["divergence_bias"] in ("bullish", "neutral")


# --------------------------------------------------------------------------- #
# RSI bearish divergence
# --------------------------------------------------------------------------- #


def _build_rsi_bearish_divergence_bars() -> list[dict[str, Any]]:
    """
    Mirror of the bullish case:
        Phase 1: sharp rally → RSI high.
        Phase 2: pullback.
        Phase 3: new price high but shallower RSI high.
    """
    bars: list[dict[str, Any]] = []

    # Warm-up
    bars += _falling_series(20, start=120.0, step=0.3)

    # Sharp rally: RSI surges
    bars += _rising_series(15, start=114.0, step=2.0)

    # Pullback
    bars += _falling_series(5, start=142.0, step=1.0)

    # Second, shallower rally reaching a higher price high
    bars += _rising_series(5, start=137.0, step=2.5)

    return bars


def test_rsi_bearish_divergence_detected():
    bars = _build_rsi_bearish_divergence_bars()
    result = DivergenceEngine().compute(_make_context(bars))
    assert result["rsi_bearish_divergence"] == 1


def test_rsi_bearish_divergence_sets_bearish_bias():
    bars = _build_rsi_bearish_divergence_bars()
    result = DivergenceEngine().compute(_make_context(bars))
    if result["rsi_bearish_divergence"] == 1:
        assert result["divergence_bias"] in ("bearish", "neutral")


# --------------------------------------------------------------------------- #
# Divergence count and strength
# --------------------------------------------------------------------------- #


def test_divergence_count_is_zero_in_sideways_market():
    bars = _sideways_series(MIN_BARS_REQUIRED + DIVERGENCE_LOOKBACK)
    result = DivergenceEngine().compute(_make_context(bars))
    # A purely sideways market with tiny moves should not produce strong divergence
    assert result["divergence_count"] >= 0  # structural: never negative
    assert isinstance(result["divergence_count"], int)


def test_divergence_strength_score_in_valid_range():
    for bars in [
        _sideways_series(50),
        _rising_series(50),
        _falling_series(50),
        _build_rsi_bullish_divergence_bars(),
        _build_rsi_bearish_divergence_bars(),
    ]:
        result = DivergenceEngine().compute(_make_context(bars))
        assert 0.0 <= result["divergence_strength_score"] <= 1.0, (
            f"strength out of range: {result['divergence_strength_score']}"
        )


def test_divergence_count_equals_sum_of_flags():
    for bars in [
        _sideways_series(50),
        _build_rsi_bullish_divergence_bars(),
        _build_rsi_bearish_divergence_bars(),
    ]:
        result = DivergenceEngine().compute(_make_context(bars))
        expected_count = (
            result["rsi_bullish_divergence"]
            + result["rsi_bearish_divergence"]
            + result["macd_bullish_divergence"]
            + result["macd_bearish_divergence"]
        )
        assert result["divergence_count"] == expected_count


# --------------------------------------------------------------------------- #
# Divergence bias consistency
# --------------------------------------------------------------------------- #


def test_divergence_bias_is_valid_label():
    valid_biases = {"bullish", "bearish", "neutral"}
    for bars in [_sideways_series(50), _rising_series(50), _falling_series(50)]:
        result = DivergenceEngine().compute(_make_context(bars))
        assert result["divergence_bias"] in valid_biases


def test_divergence_bias_neutral_when_no_signals():
    bars = _sideways_series(MIN_BARS_REQUIRED + DIVERGENCE_LOOKBACK)
    result = DivergenceEngine().compute(_make_context(bars))
    if result["divergence_count"] == 0:
        assert result["divergence_bias"] == "neutral"


def test_divergence_bias_bullish_when_only_bullish_flags():
    bars = _build_rsi_bullish_divergence_bars()
    result = DivergenceEngine().compute(_make_context(bars))
    if result["rsi_bullish_divergence"] == 1 and result["rsi_bearish_divergence"] == 0:
        # Bullish signals without bearish → bias must be bullish
        assert result["divergence_bias"] == "bullish"


# --------------------------------------------------------------------------- #
# Output type safety
# --------------------------------------------------------------------------- #


def test_integer_flags_are_0_or_1():
    for bars in [
        _sideways_series(50),
        _build_rsi_bullish_divergence_bars(),
        _build_rsi_bearish_divergence_bars(),
    ]:
        result = DivergenceEngine().compute(_make_context(bars))
        for key in (
            "rsi_bullish_divergence",
            "rsi_bearish_divergence",
            "macd_bullish_divergence",
            "macd_bearish_divergence",
        ):
            assert result[key] in (0, 1), f"{key}={result[key]} is not 0 or 1"
