from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import ema, linear_regression_slope, safe_div, sma


class TrendEngine:
    name = "trend_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "ema_9", "float", "9-period EMA of close", "price"),
            FeatureSpec(self.name, "ema_21", "float", "21-period EMA of close", "price"),
            FeatureSpec(self.name, "ema_55", "float", "55-period EMA of close", "price"),
            FeatureSpec(self.name, "sma_20", "float", "20-period SMA of close", "price"),
            FeatureSpec(self.name, "trend_alignment_score", "float", "Directional alignment score from EMA stack", "score"),
            FeatureSpec(self.name, "trend_slope_21", "float", "Linear regression slope of close over 21 bars", "slope"),
            FeatureSpec(self.name, "trend_strength_pct", "float", "Spread between fast and medium EMA as percentage", "pct"),
            FeatureSpec(self.name, "price_above_ema21", "int", "1 when close is above EMA21", "flag"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars
        if not bars:
            return {
                "ema_9": 0.0,
                "ema_21": 0.0,
                "ema_55": 0.0,
                "sma_20": 0.0,
                "trend_alignment_score": 0.0,
                "trend_slope_21": 0.0,
                "trend_strength_pct": 0.0,
                "price_above_ema21": 0,
            }

        closes = [float(bar["close"]) for bar in bars]
        latest_close = closes[-1]

        ema_9 = ema(closes, 9) or latest_close
        ema_21 = ema(closes, 21) or latest_close
        ema_55 = ema(closes, 55) or latest_close
        sma_20 = sma(closes, 20) or latest_close

        if ema_9 > ema_21 > ema_55:
            alignment = 1.0
        elif ema_9 < ema_21 < ema_55:
            alignment = -1.0
        else:
            alignment = 0.0

        slope_21 = linear_regression_slope(closes, period=21)
        trend_strength_pct = safe_div(ema_9 - ema_21, latest_close, default=0.0) * 100.0

        return {
            "ema_9": ema_9,
            "ema_21": ema_21,
            "ema_55": ema_55,
            "sma_20": sma_20,
            "trend_alignment_score": alignment,
            "trend_slope_21": slope_21,
            "trend_strength_pct": trend_strength_pct,
            "price_above_ema21": int(latest_close > ema_21),
        }
