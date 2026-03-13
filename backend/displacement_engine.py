from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import clamp, mean, safe_div

# --------------------------------------------------------------------------- #
# Module-level thresholds — change here to tune detection sensitivity.
# --------------------------------------------------------------------------- #

# A bar's range must exceed this multiple of the trailing average range to
# qualify as a displacement candle.
DISPLACEMENT_RANGE_MULTIPLE: float = 1.5

# Minimum body-to-range ratio for the bar to be considered a "real" impulse
# (filters doji / indecision bars from the displacement flag).
DISPLACEMENT_MIN_BODY_RATIO: float = 0.5

# Number of bars used to compute the "average range" and "average volume"
# baselines.
AVG_PERIOD: int = 20

# Minimum number of trailing bars required to attempt displacement scoring.
MIN_BARS_REQUIRED: int = 5

# Follow-through lookback: how many bars after the impulse bar to inspect for
# continuation (only counted on the bars already in the context window,
# i.e. bars before the latest bar).
FOLLOW_THROUGH_LOOKBACK: int = 3

# Decay lookback: how many consecutive bars to inspect for retracement against
# the impulse direction.
DECAY_LOOKBACK: int = 3


class DisplacementEngine:
    name = "displacement_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "displacement_up_flag", "int", "1 when the latest bar is a bullish displacement candle", "flag"),
            FeatureSpec(self.name, "displacement_down_flag", "int", "1 when the latest bar is a bearish displacement candle", "flag"),
            FeatureSpec(self.name, "displacement_body_to_range", "float", "Body size divided by full range of latest bar", "ratio"),
            FeatureSpec(self.name, "displacement_range_to_avg", "float", "Latest bar range divided by trailing average range", "ratio"),
            FeatureSpec(self.name, "displacement_volume_to_avg", "float", "Latest bar volume divided by trailing average volume", "ratio"),
            FeatureSpec(self.name, "displacement_close_strength", "float", "CLV (close-location value) of latest bar in its range", "ratio"),
            FeatureSpec(self.name, "displacement_follow_through_score", "float", "0-1 score: share of FOLLOW_THROUGH_LOOKBACK bars that continued in displacement direction", "score"),
            FeatureSpec(self.name, "displacement_decay_score", "float", "0-1 score: how much the displacement has decayed (1 = fully retraced, 0 = intact)", "score"),
            FeatureSpec(self.name, "impulse_quality_score", "float", "Composite 0-1 impulse quality combining body ratio, range multiple, and close strength", "score"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars

        _defaults: FeatureMap = {
            "displacement_up_flag": 0,
            "displacement_down_flag": 0,
            "displacement_body_to_range": 0.0,
            "displacement_range_to_avg": 0.0,
            "displacement_volume_to_avg": 0.0,
            "displacement_close_strength": 0.5,
            "displacement_follow_through_score": 0.0,
            "displacement_decay_score": 0.0,
            "impulse_quality_score": 0.0,
        }

        if len(bars) < MIN_BARS_REQUIRED:
            return _defaults

        latest = bars[-1]
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_volume = float(latest["volume"])

        latest_range = latest_high - latest_low
        body = abs(latest_close - latest_open)

        # Baseline window excludes the latest bar itself so the ratio is
        # always relative to prior behaviour.
        baseline_bars = bars[-(AVG_PERIOD + 1) : -1]

        avg_range_values = [float(b["high"]) - float(b["low"]) for b in baseline_bars]
        avg_vol_values = [float(b["volume"]) for b in baseline_bars]

        avg_range = mean(avg_range_values) or 0.0
        avg_volume = mean(avg_vol_values) or 0.0

        body_to_range = safe_div(body, latest_range, default=0.0) if latest_range > 0 else 0.0
        range_to_avg = safe_div(latest_range, avg_range, default=0.0)
        volume_to_avg = safe_div(latest_volume, avg_volume, default=0.0)

        # Close location value (CLV): 1.0 = closed at high, 0.0 = closed at low.
        if latest_range > 0:
            clv = safe_div(latest_close - latest_low, latest_range, default=0.5)
        else:
            clv = 0.5

        is_bullish = latest_close > latest_open
        is_bearish = latest_close < latest_open

        is_large = range_to_avg >= DISPLACEMENT_RANGE_MULTIPLE
        has_body = body_to_range >= DISPLACEMENT_MIN_BODY_RATIO

        displacement_up = int(is_bullish and is_large and has_body)
        displacement_down = int(is_bearish and is_large and has_body)

        # Determine primary direction of the latest bar for follow-through /
        # decay calculations (neutral bars return 0 for both).
        displacement_direction = 0  # +1 up, -1 down, 0 neutral
        if displacement_up:
            displacement_direction = 1
        elif displacement_down:
            displacement_direction = -1

        follow_through_score = _compute_follow_through(bars, displacement_direction, lookback=FOLLOW_THROUGH_LOOKBACK)
        decay_score = _compute_decay(bars, displacement_direction, latest_close, lookback=DECAY_LOOKBACK)

        # Composite impulse quality: average of body ratio, range-multiple
        # component, and direction-appropriate close strength, all clamped 0-1.
        range_component = clamp(safe_div(range_to_avg, DISPLACEMENT_RANGE_MULTIPLE * 2.0, default=0.0), 0.0, 1.0)
        close_strength_component = clv if is_bullish else (1.0 - clv) if is_bearish else 0.0
        impulse_quality = clamp((body_to_range + range_component + close_strength_component) / 3.0, 0.0, 1.0)

        return {
            "displacement_up_flag": displacement_up,
            "displacement_down_flag": displacement_down,
            "displacement_body_to_range": round(body_to_range, 6),
            "displacement_range_to_avg": round(range_to_avg, 6),
            "displacement_volume_to_avg": round(volume_to_avg, 6),
            "displacement_close_strength": round(clv, 6),
            "displacement_follow_through_score": round(follow_through_score, 6),
            "displacement_decay_score": round(decay_score, 6),
            "impulse_quality_score": round(impulse_quality, 6),
        }


# --------------------------------------------------------------------------- #
# Private helpers
# --------------------------------------------------------------------------- #

def _compute_follow_through(
    bars: list[dict],
    direction: int,
    lookback: int,
) -> float:
    """Return the fraction of the N bars *preceding* the latest bar that moved
    in the given direction.

    Parameters
    ----------
    bars:
        Full normalised bar list.
    direction:
        +1 for bullish, -1 for bearish, 0 for no direction (returns 0.0).
    lookback:
        Number of bars before the latest bar to inspect.
    """
    if direction == 0 or len(bars) < 2:
        return 0.0

    # The "follow-through" window is the bars immediately before the latest
    # bar.  We look at whether each bar continued in the impulse direction.
    window = bars[-(lookback + 1) : -1]
    if not window:
        return 0.0

    count = 0
    for bar in window:
        close = float(bar["close"])
        open_ = float(bar["open"])
        if direction == 1 and close > open_:
            count += 1
        elif direction == -1 and close < open_:
            count += 1

    return safe_div(count, len(window), default=0.0)


def _compute_decay(
    bars: list[dict],
    direction: int,
    impulse_close: float,
    lookback: int,
) -> float:
    """Return a 0-1 score representing how much the displacement has decayed.

    A score of 1.0 means price has fully retraced to the pre-impulse level.
    A score of 0.0 means no retracement at all.

    The calculation uses the close prices of the bars immediately *before*
    the latest bar as the retracement reference.

    Parameters
    ----------
    bars:
        Full normalised bar list.
    direction:
        +1 for bullish (we check drawdown below impulse close),
        -1 for bearish (we check rally above impulse close).
    impulse_close:
        The close of the displacement (latest) bar.
    lookback:
        Number of bars before the latest bar to inspect.
    """
    if direction == 0 or len(bars) < 2 + lookback:
        return 0.0

    # Reference: the close of the bar just before the impulse bar.
    precursor_idx = -(lookback + 2)
    if abs(precursor_idx) > len(bars):
        return 0.0

    pre_impulse_close = float(bars[precursor_idx]["close"])
    total_move = impulse_close - pre_impulse_close
    if abs(total_move) < 1e-9:
        return 0.0

    # Closing prices in the window BEFORE the current bar (the decay window).
    window = bars[-(lookback + 1) : -1]
    if not window:
        return 0.0

    latest_window_close = float(window[-1]["close"])

    # How much of the original move has been given back?
    retracement = latest_window_close - impulse_close  # negative if declined from bullish impulse
    if direction == 1:
        decay = clamp(safe_div(-retracement, abs(total_move), default=0.0), 0.0, 1.0)
    else:  # bearish impulse: price should have dropped; retracement = rally
        decay = clamp(safe_div(retracement, abs(total_move), default=0.0), 0.0, 1.0)

    return decay
