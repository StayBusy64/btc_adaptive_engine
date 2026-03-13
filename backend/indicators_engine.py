from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import macd, rsi, safe_div, stochastic_d, stochastic_k


class IndicatorsEngine:
    name = "indicators_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "rsi_14", "float", "Relative strength index over 14 closes", "index"),
            FeatureSpec(self.name, "macd_line", "float", "MACD line (12,26)", "value"),
            FeatureSpec(self.name, "macd_signal", "float", "MACD signal line (9)", "value"),
            FeatureSpec(self.name, "macd_hist", "float", "MACD histogram", "value"),
            FeatureSpec(self.name, "stochastic_k_14", "float", "Stochastic %K over 14 bars", "index"),
            FeatureSpec(self.name, "stochastic_d_3", "float", "Smoothed stochastic %D over 3 bars", "index"),
            FeatureSpec(self.name, "momentum_10_pct", "float", "10-bar momentum percentage", "pct"),
            FeatureSpec(self.name, "roc_5_pct", "float", "5-bar rate of change percentage", "pct"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars
        if not bars:
            return {
                "rsi_14": 50.0,
                "macd_line": 0.0,
                "macd_signal": 0.0,
                "macd_hist": 0.0,
                "stochastic_k_14": 50.0,
                "stochastic_d_3": 50.0,
                "momentum_10_pct": 0.0,
                "roc_5_pct": 0.0,
            }

        closes = [float(bar["close"]) for bar in bars]
        highs = [float(bar["high"]) for bar in bars]
        lows = [float(bar["low"]) for bar in bars]

        latest_close = closes[-1]
        close_10 = closes[-11] if len(closes) > 10 else closes[0]
        close_5 = closes[-6] if len(closes) > 5 else closes[0]

        rsi_14 = rsi(closes, period=14)
        macd_line, macd_signal, macd_hist = macd(closes, fast_period=12, slow_period=26, signal_period=9)
        k_14 = stochastic_k(highs, lows, closes, period=14)
        d_3 = stochastic_d(highs, lows, closes, period=14, smooth=3)

        return {
            "rsi_14": rsi_14 if rsi_14 is not None else 50.0,
            "macd_line": macd_line if macd_line is not None else 0.0,
            "macd_signal": macd_signal if macd_signal is not None else 0.0,
            "macd_hist": macd_hist if macd_hist is not None else 0.0,
            "stochastic_k_14": k_14 if k_14 is not None else 50.0,
            "stochastic_d_3": d_3 if d_3 is not None else 50.0,
            "momentum_10_pct": safe_div(latest_close - close_10, close_10, default=0.0) * 100.0,
            "roc_5_pct": safe_div(latest_close - close_5, close_5, default=0.0) * 100.0,
        }
