from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec

# Number of bars required on each side of a candidate bar to confirm it as a
# swing pivot.  A value of 3 means 6 additional bars are consumed per pivot
# (3 left + 3 right), which is a standard short-period pivot definition.
PIVOT_LOOKBACK = 3


class StructureEngine:
    name = "structure_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "swing_high_flag", "int", "1 when the most-recently confirmed pivot is a swing high", "flag"),
            FeatureSpec(self.name, "swing_low_flag", "int", "1 when the most-recently confirmed pivot is a swing low", "flag"),
            FeatureSpec(self.name, "most_recent_swing_high", "float", "Price of the most recent confirmed swing high", "price"),
            FeatureSpec(self.name, "most_recent_swing_low", "float", "Price of the most recent confirmed swing low", "price"),
            FeatureSpec(self.name, "higher_high_flag", "int", "1 when the latest swing high is higher than the previous swing high", "flag"),
            FeatureSpec(self.name, "higher_low_flag", "int", "1 when the latest swing low is higher than the previous swing low", "flag"),
            FeatureSpec(self.name, "lower_high_flag", "int", "1 when the latest swing high is lower than the previous swing high", "flag"),
            FeatureSpec(self.name, "lower_low_flag", "int", "1 when the latest swing low is lower than the previous swing low", "flag"),
            FeatureSpec(self.name, "structure_break_bullish", "int", "1 when the latest close is above the most recent swing high", "flag"),
            FeatureSpec(self.name, "structure_break_bearish", "int", "1 when the latest close is below the most recent swing low", "flag"),
            FeatureSpec(self.name, "choch_bullish", "int", "1 when price breaks above swing high while in bearish structure (CHoCH)", "flag"),
            FeatureSpec(self.name, "choch_bearish", "int", "1 when price breaks below swing low while in bullish structure (CHoCH)", "flag"),
            FeatureSpec(self.name, "structure_trend_state", "float", "Structural trend: 1=bullish HH/HL, -1=bearish LH/LL, 0=neutral", "state"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars

        _defaults: FeatureMap = {
            "swing_high_flag": 0,
            "swing_low_flag": 0,
            "most_recent_swing_high": 0.0,
            "most_recent_swing_low": 0.0,
            "higher_high_flag": 0,
            "higher_low_flag": 0,
            "lower_high_flag": 0,
            "lower_low_flag": 0,
            "structure_break_bullish": 0,
            "structure_break_bearish": 0,
            "choch_bullish": 0,
            "choch_bearish": 0,
            "structure_trend_state": 0.0,
        }

        if not bars:
            return _defaults

        swing_highs, swing_lows = find_swing_pivots(bars, lookback=PIVOT_LOOKBACK)

        if not swing_highs and not swing_lows:
            return _defaults

        latest_close = float(bars[-1]["close"])
        n = len(bars)

        most_recent_sh = swing_highs[-1][1] if swing_highs else 0.0
        most_recent_sl = swing_lows[-1][1] if swing_lows else 0.0

        # --- HH / HL / LH / LL from the last two swings of each type ---
        higher_high = 0
        lower_high = 0
        higher_low = 0
        lower_low = 0

        if len(swing_highs) >= 2:
            if swing_highs[-1][1] > swing_highs[-2][1]:
                higher_high = 1
            elif swing_highs[-1][1] < swing_highs[-2][1]:
                lower_high = 1

        if len(swing_lows) >= 2:
            if swing_lows[-1][1] > swing_lows[-2][1]:
                higher_low = 1
            elif swing_lows[-1][1] < swing_lows[-2][1]:
                lower_low = 1

        # --- Structural trend state ---
        if higher_high and higher_low:
            structure_trend = 1.0
        elif lower_high and lower_low:
            structure_trend = -1.0
        else:
            structure_trend = 0.0

        # --- Break of structure ---
        bos_bull = int(most_recent_sh > 0.0 and latest_close > most_recent_sh)
        bos_bear = int(most_recent_sl > 0.0 and latest_close < most_recent_sl)

        # --- Change of character ---
        # Bullish CHoCH: market was in bearish structure and now breaks above last swing high
        choch_bull = int(bos_bull == 1 and structure_trend == -1.0)
        # Bearish CHoCH: market was in bullish structure and now breaks below last swing low
        choch_bear = int(bos_bear == 1 and structure_trend == 1.0)

        # --- swing_high_flag / swing_low_flag ---
        # These fire when the most recently CONFIRMED pivot is at the most-recent
        # confirmable bar index: n - PIVOT_LOOKBACK - 1
        most_recent_confirmable_idx = n - PIVOT_LOOKBACK - 1
        sh_flag = int(bool(swing_highs) and swing_highs[-1][0] == most_recent_confirmable_idx)
        sl_flag = int(bool(swing_lows) and swing_lows[-1][0] == most_recent_confirmable_idx)

        return {
            "swing_high_flag": sh_flag,
            "swing_low_flag": sl_flag,
            "most_recent_swing_high": most_recent_sh,
            "most_recent_swing_low": most_recent_sl,
            "higher_high_flag": higher_high,
            "higher_low_flag": higher_low,
            "lower_high_flag": lower_high,
            "lower_low_flag": lower_low,
            "structure_break_bullish": bos_bull,
            "structure_break_bearish": bos_bear,
            "choch_bullish": choch_bull,
            "choch_bearish": choch_bear,
            "structure_trend_state": structure_trend,
        }


def find_swing_pivots(
    bars: list[dict],
    lookback: int = PIVOT_LOOKBACK,
) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Return (swing_highs, swing_lows) as lists of (bar_index, price).

    A swing high at index ``i`` requires that ``bars[i].high`` is strictly
    greater than the high of every bar in the window
    ``[i - lookback, i + lookback]`` (excluding ``i`` itself).

    A swing low uses the same rule with ``low`` and strict less-than.

    Requires at least ``2 * lookback + 1`` bars.  Bars that cannot have a
    full window to both sides are silently skipped.
    """
    swing_highs: list[tuple[int, float]] = []
    swing_lows: list[tuple[int, float]] = []

    n = len(bars)
    if n < 2 * lookback + 1:
        return swing_highs, swing_lows

    # Only bars with full left + right confirmation windows are eligible.
    for i in range(lookback, n - lookback):
        high_i = float(bars[i]["high"])
        low_i = float(bars[i]["low"])

        is_swing_high = all(
            float(bars[j]["high"]) < high_i
            for j in range(i - lookback, i + lookback + 1)
            if j != i
        )
        is_swing_low = all(
            float(bars[j]["low"]) > low_i
            for j in range(i - lookback, i + lookback + 1)
            if j != i
        )

        if is_swing_high:
            swing_highs.append((i, high_i))
        if is_swing_low:
            swing_lows.append((i, low_i))

    return swing_highs, swing_lows
