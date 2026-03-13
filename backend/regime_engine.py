from __future__ import annotations

from typing import Any

from backend.feature_math import clamp, to_optional_float


def classify_regime(features: dict[str, Any]) -> dict[str, float | str]:
    trend_alignment = to_optional_float(features.get("trend_alignment_score")) or 0.0
    trend_strength_pct = to_optional_float(features.get("trend_strength_pct")) or 0.0
    trend_slope = to_optional_float(features.get("trend_slope_21")) or 0.0
    range_expansion_ratio = to_optional_float(features.get("range_expansion_ratio")) or 1.0
    volatility_zscore = to_optional_float(features.get("volatility_zscore_20")) or 0.0
    momentum_10_pct = to_optional_float(features.get("momentum_10_pct")) or 0.0

    trend_strength = abs(trend_strength_pct) / 5.0
    slope_strength = abs(trend_slope)
    momentum_strength = abs(momentum_10_pct) / 2.0
    directional_energy = trend_strength + (0.5 * slope_strength) + (0.5 * abs(trend_alignment)) + (0.4 * momentum_strength)

    expansion = range_expansion_ratio
    high_volatility = volatility_zscore > 0.75
    compressed = expansion < 0.85
    expanded = expansion > 1.20

    if directional_energy >= 1.25 and expanded and high_volatility:
        regime_id = "trend_expansion"
    elif directional_energy >= 0.90 and not expanded:
        regime_id = "trend_compression"
    elif directional_energy < 0.60 and high_volatility:
        regime_id = "volatile_rotation"
    elif directional_energy < 0.60 and compressed:
        regime_id = "balanced_compression"
    else:
        regime_id = "balanced"

    regime_confidence = clamp(
        0.20
        + (0.30 * min(directional_energy, 1.5))
        + (0.20 * min(abs(volatility_zscore), 2.0))
        + (0.15 * min(abs(expansion - 1.0), 1.0)),
        0.0,
        0.99,
    )

    transition_risk = clamp(
        (1.0 - regime_confidence) + (0.15 if abs(momentum_10_pct) < 0.08 else 0.0),
        0.0,
        1.0,
    )

    return {
        "regime_id": regime_id,
        "regime_confidence": regime_confidence,
        "transition_risk": transition_risk,
    }
