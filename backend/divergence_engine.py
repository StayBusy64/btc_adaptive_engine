from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import macd, rsi, safe_div

# --------------------------------------------------------------------------- #
# Tuning constants
# --------------------------------------------------------------------------- #

# How many bars to look back when scanning for a prior swing high/low to
# compare against the current bar for divergence detection.
DIVERGENCE_LOOKBACK: int = 14

# Minimum price move (as a fraction of price) for the swing to be considered
# meaningful.  Filters out flat / noise swings.
MIN_SWING_MOVE_PCT: float = 0.002  # 0.2 %

# Minimum oscillator delta for the divergence leg to be meaningful.
MIN_RSI_DELTA: float = 2.0
MIN_MACD_HIST_DELTA: float = 0.0  # any directional disagreement counts

# Minimum number of bars required to compute divergences.
MIN_BARS_REQUIRED: int = 30


class DivergenceEngine:
    """Detect classic and hidden price / oscillator divergences.

    All signals are computed against a look-back swing using the most recent
    ``DIVERGENCE_LOOKBACK`` bars.  The engine is intentionally lightweight:
    it uses the same RSI and MACD helpers as ``IndicatorsEngine`` so there is
    no duplicate code.
    """

    name = "divergence_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(
                self.name,
                "rsi_bullish_divergence",
                "int",
                "1 when price makes a lower low but RSI makes a higher low (regular bullish divergence)",
                "flag",
            ),
            FeatureSpec(
                self.name,
                "rsi_bearish_divergence",
                "int",
                "1 when price makes a higher high but RSI makes a lower high (regular bearish divergence)",
                "flag",
            ),
            FeatureSpec(
                self.name,
                "macd_bullish_divergence",
                "int",
                "1 when price makes a lower low but MACD histogram makes a higher low",
                "flag",
            ),
            FeatureSpec(
                self.name,
                "macd_bearish_divergence",
                "int",
                "1 when price makes a higher high but MACD histogram makes a lower high",
                "flag",
            ),
            FeatureSpec(
                self.name,
                "divergence_count",
                "int",
                "Number of active divergence signals (0–4)",
                "count",
            ),
            FeatureSpec(
                self.name,
                "divergence_strength_score",
                "float",
                "Composite divergence strength 0.0–1.0",
                "score",
            ),
            FeatureSpec(
                self.name,
                "divergence_bias",
                "str",
                "Dominant divergence direction: 'bullish', 'bearish', or 'neutral'",
                "label",
            ),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars

        _defaults: FeatureMap = {
            "rsi_bullish_divergence": 0,
            "rsi_bearish_divergence": 0,
            "macd_bullish_divergence": 0,
            "macd_bearish_divergence": 0,
            "divergence_count": 0,
            "divergence_strength_score": 0.0,
            "divergence_bias": "neutral",
        }

        if len(bars) < MIN_BARS_REQUIRED:
            return _defaults

        closes = [float(b["close"]) for b in bars]
        highs = [float(b["high"]) for b in bars]
        lows = [float(b["low"]) for b in bars]

        # ------------------------------------------------------------------ #
        # Current-bar oscillator values (full history → most recent output)
        # ------------------------------------------------------------------ #
        current_rsi = rsi(closes, period=14)
        _, _, current_hist = macd(closes, fast_period=12, slow_period=26, signal_period=9)

        if current_rsi is None or current_hist is None:
            return _defaults

        current_close = closes[-1]
        current_low = lows[-1]
        current_high = highs[-1]

        # ------------------------------------------------------------------ #
        # Find the "reference bar" within the lookback window.
        # For bullish divergence we search for the lowest close (prior swing low).
        # For bearish divergence we search for the highest close (prior swing high).
        # We exclude the current bar itself.
        # ------------------------------------------------------------------ #
        lookback = min(DIVERGENCE_LOOKBACK, len(bars) - 1)
        window_start = len(bars) - 1 - lookback  # inclusive
        window_end = len(bars) - 1  # exclusive (current bar)

        ref_closes = closes[window_start:window_end]
        ref_lows = lows[window_start:window_end]
        ref_highs = highs[window_start:window_end]

        if not ref_closes:
            return _defaults

        # ------------------------------------------------------------------ #
        # RSI values for each bar in the lookback window
        # ------------------------------------------------------------------ #
        rsi_bullish_div = 0
        rsi_bearish_div = 0
        macd_bullish_div = 0
        macd_bearish_div = 0

        rsi_bull_strength = 0.0
        rsi_bear_strength = 0.0
        macd_bull_strength = 0.0
        macd_bear_strength = 0.0

        for offset, (ref_close, ref_low, ref_high) in enumerate(
            zip(ref_closes, ref_lows, ref_highs)
        ):
            ref_bar_abs_idx = window_start + offset  # absolute index in `closes`

            # Compute RSI up to (and including) this reference bar
            ref_rsi = rsi(closes[: ref_bar_abs_idx + 1], period=14)
            if ref_rsi is None:
                continue

            # Compute MACD histogram up to (and including) this reference bar
            _, _, ref_hist = macd(
                closes[: ref_bar_abs_idx + 1],
                fast_period=12,
                slow_period=26,
                signal_period=9,
            )
            if ref_hist is None:
                continue

            # -------------------------------------------------------------- #
            # Regular BULLISH divergence:
            #   current low < reference low  (price lower low)
            #   current RSI > reference RSI  (oscillator higher low)
            # -------------------------------------------------------------- #
            price_lower_low = current_low < ref_low
            rsi_higher_low = (current_rsi - ref_rsi) >= MIN_RSI_DELTA

            if price_lower_low and rsi_higher_low:
                # Confirm the price move is meaningful
                price_move_pct = safe_div(
                    abs(ref_low - current_low), max(abs(ref_close), 1e-9), default=0.0
                )
                if price_move_pct >= MIN_SWING_MOVE_PCT:
                    rsi_bullish_div = 1
                    strength = safe_div(current_rsi - ref_rsi, 100.0, default=0.0)
                    rsi_bull_strength = max(rsi_bull_strength, strength)

            # -------------------------------------------------------------- #
            # Regular BEARISH divergence:
            #   current high > reference high  (price higher high)
            #   current RSI < reference RSI    (oscillator lower high)
            # -------------------------------------------------------------- #
            price_higher_high = current_high > ref_high
            rsi_lower_high = (ref_rsi - current_rsi) >= MIN_RSI_DELTA

            if price_higher_high and rsi_lower_high:
                price_move_pct = safe_div(
                    abs(current_high - ref_high), max(abs(ref_close), 1e-9), default=0.0
                )
                if price_move_pct >= MIN_SWING_MOVE_PCT:
                    rsi_bearish_div = 1
                    strength = safe_div(ref_rsi - current_rsi, 100.0, default=0.0)
                    rsi_bear_strength = max(rsi_bear_strength, strength)

            # -------------------------------------------------------------- #
            # MACD histogram BULLISH divergence
            # -------------------------------------------------------------- #
            macd_hist_higher = (current_hist - ref_hist) > MIN_MACD_HIST_DELTA
            if price_lower_low and macd_hist_higher:
                price_move_pct = safe_div(
                    abs(ref_low - current_low), max(abs(ref_close), 1e-9), default=0.0
                )
                if price_move_pct >= MIN_SWING_MOVE_PCT:
                    macd_bullish_div = 1
                    raw_delta = abs(current_hist - ref_hist)
                    scale = max(abs(ref_hist), abs(current_hist), 1e-9)
                    macd_bull_strength = max(
                        macd_bull_strength, min(safe_div(raw_delta, scale, default=0.0), 1.0)
                    )

            # -------------------------------------------------------------- #
            # MACD histogram BEARISH divergence
            # -------------------------------------------------------------- #
            macd_hist_lower = (ref_hist - current_hist) > MIN_MACD_HIST_DELTA
            if price_higher_high and macd_hist_lower:
                price_move_pct = safe_div(
                    abs(current_high - ref_high), max(abs(ref_close), 1e-9), default=0.0
                )
                if price_move_pct >= MIN_SWING_MOVE_PCT:
                    macd_bearish_div = 1
                    raw_delta = abs(ref_hist - current_hist)
                    scale = max(abs(ref_hist), abs(current_hist), 1e-9)
                    macd_bear_strength = max(
                        macd_bear_strength, min(safe_div(raw_delta, scale, default=0.0), 1.0)
                    )

        # ------------------------------------------------------------------ #
        # Composite divergence count and strength score
        # ------------------------------------------------------------------ #
        divergence_count = rsi_bullish_div + rsi_bearish_div + macd_bullish_div + macd_bearish_div

        bullish_strength = (rsi_bull_strength + macd_bull_strength) / 2.0
        bearish_strength = (rsi_bear_strength + macd_bear_strength) / 2.0
        divergence_strength_score = round(max(bullish_strength, bearish_strength), 6)

        if bullish_strength > bearish_strength and divergence_count > 0:
            divergence_bias: str = "bullish"
        elif bearish_strength > bullish_strength and divergence_count > 0:
            divergence_bias = "bearish"
        else:
            divergence_bias = "neutral"

        return {
            "rsi_bullish_divergence": rsi_bullish_div,
            "rsi_bearish_divergence": rsi_bearish_div,
            "macd_bullish_divergence": macd_bullish_div,
            "macd_bearish_divergence": macd_bearish_div,
            "divergence_count": divergence_count,
            "divergence_strength_score": divergence_strength_score,
            "divergence_bias": divergence_bias,
        }
