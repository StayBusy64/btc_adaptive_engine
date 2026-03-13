from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import safe_div
from backend.structure_engine import PIVOT_LOOKBACK, find_swing_pivots

# --------------------------------------------------------------------------- #
# Module-level thresholds — change here to tune detection sensitivity.
# --------------------------------------------------------------------------- #

# Maximum relative price difference for two swing levels to be classified as
# "equal" (a liquidity cluster candidate).  0.0015 == 0.15 %.
EQ_TOLERANCE_PCT: float = 0.0015

# Number of swing highs / lows to inspect when looking for an equal cluster.
EQUAL_CLUSTER_LOOKBACK: int = 6

# Minimum count of swings within EQ_TOLERANCE_PCT of each other to raise the
# equal-cluster flag.
EQUAL_CLUSTER_MIN: int = 2

# Lookback period (in bars) used when computing average range for the
# stop-run score normalisation step.
AVG_RANGE_PERIOD: int = 14

# Cap applied to the wick-rejection ratio to prevent extreme outliers.
WICK_REJECTION_CAP: float = 10.0


class LiquidityEngine:
    name = "liquidity_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "equal_highs_cluster_flag", "int", "1 when two or more recent swing highs are within EQ_TOLERANCE_PCT of each other", "flag"),
            FeatureSpec(self.name, "equal_lows_cluster_flag", "int", "1 when two or more recent swing lows are within EQ_TOLERANCE_PCT of each other", "flag"),
            FeatureSpec(self.name, "liquidity_sweep_high", "int", "1 when current bar wicked above a recent swing high and closed below it", "flag"),
            FeatureSpec(self.name, "liquidity_sweep_low", "int", "1 when current bar wicked below a recent swing low and closed above it", "flag"),
            FeatureSpec(self.name, "sweep_reclaim_bullish", "int", "1 when a low sweep occurred and the bar closed bullishly (reclaim)", "flag"),
            FeatureSpec(self.name, "sweep_reclaim_bearish", "int", "1 when a high sweep occurred and the bar closed bearishly (reclaim)", "flag"),
            FeatureSpec(self.name, "stop_run_score", "float", "Composite 0-1 score for stop-run likelihood on the latest bar", "score"),
            FeatureSpec(self.name, "wick_rejection_score", "float", "Relevant wick size divided by body on sweep bars (capped at 10)", "ratio"),
            FeatureSpec(self.name, "liquidity_pressure_bias", "float", "Net liquidity bias: +1 upward, -1 downward", "score"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars

        _defaults: FeatureMap = {
            "equal_highs_cluster_flag": 0,
            "equal_lows_cluster_flag": 0,
            "liquidity_sweep_high": 0,
            "liquidity_sweep_low": 0,
            "sweep_reclaim_bullish": 0,
            "sweep_reclaim_bearish": 0,
            "stop_run_score": 0.0,
            "wick_rejection_score": 0.0,
            "liquidity_pressure_bias": 0.0,
        }

        if not bars:
            return _defaults

        swing_highs, swing_lows = find_swing_pivots(bars, lookback=PIVOT_LOOKBACK)

        latest = bars[-1]
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        latest_close = float(latest["close"])
        latest_open = float(latest["open"])
        latest_range = max(latest_high - latest_low, 1e-10)

        # ------------------------------------------------------------------ #
        # Equal highs / lows cluster detection
        # ------------------------------------------------------------------ #
        recent_shs = [price for _, price in swing_highs[-EQUAL_CLUSTER_LOOKBACK:]]
        recent_sls = [price for _, price in swing_lows[-EQUAL_CLUSTER_LOOKBACK:]]

        eq_highs_flag: bool = _has_equal_cluster(recent_shs, EQ_TOLERANCE_PCT)
        eq_lows_flag: bool = _has_equal_cluster(recent_sls, EQ_TOLERANCE_PCT)

        # ------------------------------------------------------------------ #
        # Sweep detection (latest bar only)
        # A "sweep high" means the current bar's wick pierced above a recent
        # swing high but the bar closed back below it.
        # A "sweep low" is the mirror.
        # ------------------------------------------------------------------ #
        most_recent_sh = swing_highs[-1][1] if swing_highs else None
        most_recent_sl = swing_lows[-1][1] if swing_lows else None

        sweep_high = 0
        sweep_low = 0

        if most_recent_sh is not None:
            sweep_high = int(latest_high > most_recent_sh and latest_close < most_recent_sh)

        if most_recent_sl is not None:
            sweep_low = int(latest_low < most_recent_sl and latest_close > most_recent_sl)

        # ------------------------------------------------------------------ #
        # Reclaim: directional close after a sweep
        # ------------------------------------------------------------------ #
        bullish_close = latest_close > latest_open
        bearish_close = latest_close < latest_open

        sweep_reclaim_bull = int(sweep_low == 1 and bullish_close)
        sweep_reclaim_bear = int(sweep_high == 1 and bearish_close)

        # ------------------------------------------------------------------ #
        # Wick rejection score
        # On a sweep bar the relevant wick is compared to the body size.
        # Capped at WICK_REJECTION_CAP to prevent extreme outliers.
        # ------------------------------------------------------------------ #
        body = abs(latest_close - latest_open)

        if sweep_low:
            lower_wick = min(latest_open, latest_close) - latest_low
            if body > 1e-10:
                wick_rejection = safe_div(lower_wick, body)
            else:
                wick_rejection = safe_div(lower_wick, latest_range)
        elif sweep_high:
            upper_wick = latest_high - max(latest_open, latest_close)
            if body > 1e-10:
                wick_rejection = safe_div(upper_wick, body)
            else:
                wick_rejection = safe_div(upper_wick, latest_range)
        else:
            wick_rejection = 0.0

        wick_rejection = min(wick_rejection, WICK_REJECTION_CAP)

        # ------------------------------------------------------------------ #
        # Stop-run score (0 to 1)
        # Four independent signals contribute addively; each is capped so the
        # total cannot exceed 1.0.
        #
        #   0.40  sweep occurred on this bar
        #   0.25  range expanded above average (stop-hunt momentum)
        #   0.20  strong wick rejection (normalised, capped at 1 contribution)
        #   0.15  equal-high / equal-low cluster exists (pre-run condition)
        # ------------------------------------------------------------------ #
        avg_rng = _avg_range(bars, AVG_RANGE_PERIOD)
        range_expansion = safe_div(latest_range, avg_rng, default=1.0)

        sweep_any = int(sweep_high == 1 or sweep_low == 1)

        expansion_contrib = (
            0.25 * min(range_expansion - 1.0, 1.0)
            if range_expansion > 1.0
            else 0.0
        )
        wick_contrib = 0.20 * min(safe_div(wick_rejection, 3.0), 1.0)
        cluster_contrib = 0.15 * int(eq_highs_flag or eq_lows_flag)

        stop_run_score = min(
            (0.40 * sweep_any) + expansion_contrib + wick_contrib + cluster_contrib,
            1.0,
        )

        # ------------------------------------------------------------------ #
        # Liquidity pressure bias (-1.0 to +1.0)
        #
        # Equal highs above price → potential upside sweep bait → bearish bias
        # Equal lows below price → potential downside sweep bait → bullish bias
        # After a reclaim the bias flips toward the sweep survivor direction.
        # ------------------------------------------------------------------ #
        bias = 0.0
        if eq_highs_flag:
            bias -= 0.5
        if eq_lows_flag:
            bias += 0.5
        if sweep_reclaim_bull:
            bias += 0.5
        if sweep_reclaim_bear:
            bias -= 0.5

        bias = max(-1.0, min(1.0, bias))

        return {
            "equal_highs_cluster_flag": int(eq_highs_flag),
            "equal_lows_cluster_flag": int(eq_lows_flag),
            "liquidity_sweep_high": sweep_high,
            "liquidity_sweep_low": sweep_low,
            "sweep_reclaim_bullish": sweep_reclaim_bull,
            "sweep_reclaim_bearish": sweep_reclaim_bear,
            "stop_run_score": stop_run_score,
            "wick_rejection_score": wick_rejection,
            "liquidity_pressure_bias": bias,
        }


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #

def _has_equal_cluster(prices: list[float], tolerance_pct: float) -> bool:
    """Return True when at least EQUAL_CLUSTER_MIN prices are within
    ``tolerance_pct`` (relative) of at least one reference price."""
    if len(prices) < EQUAL_CLUSTER_MIN:
        return False

    for i in range(len(prices)):
        ref = prices[i]
        if ref <= 0.0:
            continue
        count = 1
        for j in range(len(prices)):
            if i == j:
                continue
            if prices[j] > 0.0 and abs(prices[j] - ref) / ref <= tolerance_pct:
                count += 1
        if count >= EQUAL_CLUSTER_MIN:
            return True

    return False


def _avg_range(bars: list[dict], period: int) -> float:
    """Return the simple mean of high-low ranges over the last ``period`` bars."""
    window = bars[-period:]
    if not window:
        return 1.0

    ranges = [max(float(b["high"]) - float(b["low"]), 0.0) for b in window]
    total = sum(ranges)
    return total / len(ranges) if ranges else 1.0
