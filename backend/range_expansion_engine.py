from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import mean, rolling_std, safe_div, true_range_series


class RangeExpansionEngine:
    name = "range_expansion_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "range_current", "float", "Current candle range", "points"),
            FeatureSpec(self.name, "range_avg_5", "float", "Trailing 5 average range", "points"),
            FeatureSpec(self.name, "range_expansion_ratio", "float", "Current range divided by trailing average range", "ratio"),
            FeatureSpec(self.name, "range_compression_ratio_10", "float", "StdDev(range)/mean(range) over trailing 10", "ratio"),
            FeatureSpec(self.name, "range_breakout_above_prev_high", "int", "1 when close is above previous high", "flag"),
            FeatureSpec(self.name, "range_breakout_below_prev_low", "int", "1 when close is below previous low", "flag"),
            FeatureSpec(self.name, "range_inside_bar", "int", "1 when current bar is inside previous bar", "flag"),
            FeatureSpec(self.name, "range_outside_bar", "int", "1 when current bar is outside previous bar", "flag"),
            FeatureSpec(self.name, "range_displacement_points", "float", "Close-to-close displacement versus previous bar", "points"),
            FeatureSpec(self.name, "true_range_current", "float", "Current true range", "points"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars
        if not bars:
            return {
                "range_current": 0.0,
                "range_avg_5": 0.0,
                "range_expansion_ratio": 0.0,
                "range_compression_ratio_10": 0.0,
                "range_breakout_above_prev_high": 0,
                "range_breakout_below_prev_low": 0,
                "range_inside_bar": 0,
                "range_outside_bar": 0,
                "range_displacement_points": 0.0,
                "true_range_current": 0.0,
            }

        ranges = [max(float(bar["high"]) - float(bar["low"]), 0.0) for bar in bars]
        current_range = ranges[-1]
        avg_range_5 = mean(ranges[-5:]) or 0.0
        expansion_ratio = safe_div(current_range, avg_range_5, default=0.0)
        compression_ratio_10 = safe_div(rolling_std(ranges, 10), mean(ranges[-10:]) or 0.0, default=0.0)

        highs = [float(bar["high"]) for bar in bars]
        lows = [float(bar["low"]) for bar in bars]
        closes = [float(bar["close"]) for bar in bars]
        tr_values = true_range_series(highs, lows, closes)

        if len(bars) < 2:
            return {
                "range_current": current_range,
                "range_avg_5": avg_range_5,
                "range_expansion_ratio": expansion_ratio,
                "range_compression_ratio_10": compression_ratio_10,
                "range_breakout_above_prev_high": 0,
                "range_breakout_below_prev_low": 0,
                "range_inside_bar": 0,
                "range_outside_bar": 0,
                "range_displacement_points": 0.0,
                "true_range_current": tr_values[-1] if tr_values else current_range,
            }

        latest = bars[-1]
        previous = bars[-2]

        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        latest_close = float(latest["close"])

        prev_high = float(previous["high"])
        prev_low = float(previous["low"])
        prev_close = float(previous["close"])

        return {
            "range_current": current_range,
            "range_avg_5": avg_range_5,
            "range_expansion_ratio": expansion_ratio,
            "range_compression_ratio_10": compression_ratio_10,
            "range_breakout_above_prev_high": int(latest_close > prev_high),
            "range_breakout_below_prev_low": int(latest_close < prev_low),
            "range_inside_bar": int(latest_high <= prev_high and latest_low >= prev_low),
            "range_outside_bar": int(latest_high >= prev_high and latest_low <= prev_low),
            "range_displacement_points": latest_close - prev_close,
            "true_range_current": tr_values[-1] if tr_values else current_range,
        }
