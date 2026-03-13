from __future__ import annotations

import math
from typing import Any

from backend.feature_math import clamp, to_optional_float


def score_state(features: dict[str, Any]) -> dict[str, float]:
    trend_alignment = to_optional_float(features.get("trend_alignment_score")) or 0.0
    trend_strength_pct = to_optional_float(features.get("trend_strength_pct")) or 0.0
    macd_hist = to_optional_float(features.get("macd_hist")) or 0.0
    rsi_14 = to_optional_float(features.get("rsi_14")) or 50.0
    momentum_10_pct = to_optional_float(features.get("momentum_10_pct")) or 0.0
    range_expansion_ratio = to_optional_float(features.get("range_expansion_ratio")) or 1.0
    atr_pct_14 = to_optional_float(features.get("atr_pct_14")) or 0.0
    atr_14 = to_optional_float(features.get("atr_14")) or 0.0
    volatility_state_score = to_optional_float(features.get("volatility_state_score")) or 0.0

    # Normalize indicator scales before combining into directional logits.
    trend_strength = trend_strength_pct / 5.0
    momentum_strength = momentum_10_pct / 2.0
    rsi_bias = (rsi_14 - 50.0) / 50.0
    expansion_bias = range_expansion_ratio - 1.0
    volatility_penalty = max(0.0, atr_pct_14 - 1.5) / 2.0

    long_logit = (
        (0.55 * trend_alignment)
        + (0.35 * trend_strength)
        + (0.35 * macd_hist)
        + (0.20 * momentum_strength)
        + (0.15 * rsi_bias)
        + (0.20 * expansion_bias)
        - (0.20 * volatility_penalty)
        - (0.10 * max(0.0, volatility_state_score))
    )

    short_logit = (
        (-0.55 * trend_alignment)
        + (-0.35 * trend_strength)
        + (-0.35 * macd_hist)
        + (-0.20 * momentum_strength)
        + (-0.15 * rsi_bias)
        + (0.20 * expansion_bias)
        - (0.20 * volatility_penalty)
        - (0.10 * max(0.0, volatility_state_score))
    )

    no_trade_logit = (
        0.25
        + (0.15 * abs(volatility_state_score))
        + (0.10 * max(0.0, 1.0 - range_expansion_ratio))
        - (0.25 * abs(long_logit - short_logit))
    )

    long_score = math.exp(clamp(long_logit, -8.0, 8.0))
    short_score = math.exp(clamp(short_logit, -8.0, 8.0))
    no_trade_score = math.exp(clamp(no_trade_logit, -8.0, 8.0))

    denominator = long_score + short_score + no_trade_score
    if abs(denominator) <= 1e-12:
        return {
            "long_probability": 0.0,
            "short_probability": 0.0,
            "no_trade_probability": 1.0,
            "expected_excursion": 0.0,
            "setup_trust_score": 0.0,
        }

    long_probability = long_score / denominator
    short_probability = short_score / denominator
    no_trade_probability = no_trade_score / denominator

    expected_excursion = atr_14 * (long_probability - short_probability)
    setup_trust_score = clamp(max(long_probability, short_probability) - (0.25 * no_trade_probability), 0.0, 1.0)

    return {
        "long_probability": long_probability,
        "short_probability": short_probability,
        "no_trade_probability": no_trade_probability,
        "expected_excursion": expected_excursion,
        "setup_trust_score": setup_trust_score,
    }
