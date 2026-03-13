from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import mean, safe_div


class CandleFeatureEngine:
    name = "candle_feature_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(
                self.name,
                "candle_body_points",
                "float",
                "Absolute body size of latest candle",
                "points",
            ),
            FeatureSpec(
                self.name,
                "candle_range_points",
                "float",
                "High-low range of latest candle",
                "points",
            ),
            FeatureSpec(
                self.name,
                "candle_body_to_range",
                "float",
                "Body divided by full range",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "candle_upper_wick_to_range",
                "float",
                "Upper wick divided by full range",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "candle_lower_wick_to_range",
                "float",
                "Lower wick divided by full range",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "candle_clv",
                "float",
                "Close location value within candle range",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "candle_bullish_ratio_5",
                "float",
                "Share of bullish closes over trailing 5 candles",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "candle_body_efficiency_5",
                "float",
                "Trailing 5 average body-to-range efficiency",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "candle_wick_imbalance_5",
                "float",
                "Trailing 5 average wick imbalance",
                "ratio",
            ),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars
        if not bars:
            return {
                "candle_body_points": 0.0,
                "candle_range_points": 0.0,
                "candle_body_to_range": 0.0,
                "candle_upper_wick_to_range": 0.0,
                "candle_lower_wick_to_range": 0.0,
                "candle_clv": 0.5,
                "candle_bullish_ratio_5": 0.0,
                "candle_body_efficiency_5": 0.0,
                "candle_wick_imbalance_5": 0.0,
            }

        latest = bars[-1]
        latest_parts = _candle_parts(latest)

        lookback = bars[-5:]
        efficiencies = [
            safe_div(parts["body"], parts["range"], default=0.0)
            for parts in (_candle_parts(bar) for bar in lookback)
        ]
        wick_imbalances = [
            safe_div(parts["upper_wick"] - parts["lower_wick"], parts["range"], default=0.0)
            for parts in (_candle_parts(bar) for bar in lookback)
        ]
        bullish_ratio = safe_div(
            sum(1 for bar in lookback if float(bar["close"]) >= float(bar["open"])),
            len(lookback),
            default=0.0,
        )

        return {
            "candle_body_points": latest_parts["body"],
            "candle_range_points": latest_parts["range"],
            "candle_body_to_range": safe_div(
                latest_parts["body"],
                latest_parts["range"],
                default=0.0,
            ),
            "candle_upper_wick_to_range": safe_div(
                latest_parts["upper_wick"],
                latest_parts["range"],
                default=0.0,
            ),
            "candle_lower_wick_to_range": safe_div(
                latest_parts["lower_wick"],
                latest_parts["range"],
                default=0.0,
            ),
            "candle_clv": safe_div(
                float(latest["close"]) - float(latest["low"]),
                latest_parts["range"],
                default=0.5,
            ),
            "candle_bullish_ratio_5": bullish_ratio,
            "candle_body_efficiency_5": mean(efficiencies) or 0.0,
            "candle_wick_imbalance_5": mean(wick_imbalances) or 0.0,
        }


def _candle_parts(bar: dict[str, float]) -> dict[str, float]:
    open_price = float(bar["open"])
    high_price = float(bar["high"])
    low_price = float(bar["low"])
    close_price = float(bar["close"])

    body = abs(close_price - open_price)
    range_points = max(high_price - low_price, 0.0)
    upper_wick = max(high_price - max(open_price, close_price), 0.0)
    lower_wick = max(min(open_price, close_price) - low_price, 0.0)

    return {
        "body": body,
        "range": range_points,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
    }
