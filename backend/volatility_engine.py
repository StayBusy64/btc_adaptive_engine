from __future__ import annotations

import math

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import atr, mean, rolling_std, safe_div


class VolatilityEngine:
    name = "volatility_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "atr_14", "float", "Average true range over 14 bars", "points"),
            FeatureSpec(self.name, "atr_pct_14", "float", "ATR as percentage of latest close", "pct"),
            FeatureSpec(self.name, "realized_vol_10", "float", "Rolling realized volatility over 10 returns", "ratio"),
            FeatureSpec(self.name, "realized_vol_30", "float", "Rolling realized volatility over 30 returns", "ratio"),
            FeatureSpec(self.name, "volatility_zscore_20", "float", "Z-score of ATR versus trailing ATR baseline", "zscore"),
            FeatureSpec(self.name, "volatility_state_score", "float", "Volatility state score: -1 low, 0 neutral, 1 high", "state"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars
        if not bars:
            return {
                "atr_14": 0.0,
                "atr_pct_14": 0.0,
                "realized_vol_10": 0.0,
                "realized_vol_30": 0.0,
                "volatility_zscore_20": 0.0,
                "volatility_state_score": 0.0,
            }

        highs = [float(bar["high"]) for bar in bars]
        lows = [float(bar["low"]) for bar in bars]
        closes = [float(bar["close"]) for bar in bars]

        atr_14 = atr(highs, lows, closes, period=14) or 0.0
        latest_close = closes[-1] if closes else 0.0
        atr_pct_14 = safe_div(atr_14, latest_close, default=0.0) * 100.0

        returns: list[float] = []
        for index in range(1, len(closes)):
            previous_close = closes[index - 1]
            if abs(previous_close) <= 1e-12:
                continue
            returns.append((closes[index] - previous_close) / previous_close)

        realized_vol_10 = rolling_std(returns, 10) * math.sqrt(10) if returns else 0.0
        realized_vol_30 = rolling_std(returns, 30) * math.sqrt(30) if returns else 0.0

        atr_history = [max(highs[i] - lows[i], 0.0) for i in range(len(closes))]
        baseline_mean = mean(atr_history[-20:]) or 0.0
        baseline_std = rolling_std(atr_history, 20)

        if abs(baseline_std) <= 1e-12:
            zscore = 0.0
        else:
            zscore = (atr_14 - baseline_mean) / baseline_std

        if zscore > 0.75:
            volatility_state_score = 1.0
        elif zscore < -0.75:
            volatility_state_score = -1.0
        else:
            volatility_state_score = 0.0

        return {
            "atr_14": atr_14,
            "atr_pct_14": atr_pct_14,
            "realized_vol_10": realized_vol_10,
            "realized_vol_30": realized_vol_30,
            "volatility_zscore_20": zscore,
            "volatility_state_score": volatility_state_score,
        }
