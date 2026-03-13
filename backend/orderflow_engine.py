from __future__ import annotations

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec
from backend.feature_math import clamp, mean, safe_div

# --------------------------------------------------------------------------- #
# Tuning constants
# --------------------------------------------------------------------------- #

# Rolling window for cumulative delta and volume-average baselines.
DELTA_LOOKBACK: int = 10

# Minimum number of bars required to produce any non-default output.
MIN_BARS_REQUIRED: int = 3

# A bar is considered "exhaustion" when it has a large range relative to
# average AND a small body relative to its range AND volume above average.
EXHAUSTION_RANGE_MULTIPLE: float = 1.5
EXHAUSTION_BODY_RATIO_MAX: float = 0.25
EXHAUSTION_VOLUME_MULTIPLE: float = 1.2


def _clv(open_: float, high: float, low: float, close: float) -> float:
    """Close Location Value mapped to [-1, +1].

    +1  → close at the high (pure buying pressure)
    -1  → close at the low  (pure selling pressure)
     0  → close at bar midpoint

    Falls back to the open-vs-close sign when high == low (doji / flat tick).
    """
    bar_range = high - low
    if bar_range <= 1e-9:
        # Doji / flat tick – use open-close direction
        if close > open_:
            return 1.0
        if close < open_:
            return -1.0
        return 0.0
    return (2.0 * close - high - low) / bar_range


class OrderFlowEngine:
    """Proxy order-flow metrics derived from OHLCV bars.

    Without Level-2 data these are estimates, but they capture the
    *imprint* of buy/sell pressure visible in price and volume action.
    """

    name = "orderflow_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(
                self.name,
                "buying_pressure",
                "float",
                "Fraction of latest bar range attributed to buying (0–1)",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "selling_pressure",
                "float",
                "Fraction of latest bar range attributed to selling (0–1)",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "bid_ask_imbalance",
                "float",
                "buying_pressure minus selling_pressure (-1 to +1)",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "volume_delta_proxy",
                "float",
                "Signed-volume proxy: CLV × volume for the latest bar",
                "value",
            ),
            FeatureSpec(
                self.name,
                "cumulative_delta_10",
                "float",
                "Rolling sum of volume_delta_proxy over last 10 bars, normalised by avg volume",
                "ratio",
            ),
            FeatureSpec(
                self.name,
                "cumulative_delta_slope",
                "float",
                "Direction of cumulative delta: +1 rising, -1 falling, 0 flat",
                "flag",
            ),
            FeatureSpec(
                self.name,
                "volume_exhaustion_flag",
                "int",
                "1 when large-range, thin-body, high-volume bar signals trapped participants",
                "flag",
            ),
            FeatureSpec(
                self.name,
                "delta_divergence_flag",
                "int",
                "1 when price direction and cumulative delta direction disagree",
                "flag",
            ),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        bars = context.bars

        _defaults: FeatureMap = {
            "buying_pressure": 0.5,
            "selling_pressure": 0.5,
            "bid_ask_imbalance": 0.0,
            "volume_delta_proxy": 0.0,
            "cumulative_delta_10": 0.0,
            "cumulative_delta_slope": 0.0,
            "volume_exhaustion_flag": 0,
            "delta_divergence_flag": 0,
        }

        if len(bars) < MIN_BARS_REQUIRED:
            return _defaults

        latest = bars[-1]
        o = float(latest["open"])
        h = float(latest["high"])
        lo = float(latest["low"])
        c = float(latest["close"])
        v = float(latest.get("volume") or 0.0)

        # ------------------------------------------------------------------ #
        # Buying / selling pressure for the latest bar
        # ------------------------------------------------------------------ #
        clv_value = _clv(o, h, lo, c)
        # Map CLV [-1, +1] to buying [0, 1] and selling [0, 1]
        buying_pressure = clamp((clv_value + 1.0) / 2.0, 0.0, 1.0)
        selling_pressure = 1.0 - buying_pressure
        bid_ask_imbalance = clv_value  # already in [-1, +1]

        # ------------------------------------------------------------------ #
        # Volume delta proxy: CLV × volume (signed volume)
        # ------------------------------------------------------------------ #
        volume_delta_proxy = clv_value * v

        # ------------------------------------------------------------------ #
        # Rolling cumulative delta over DELTA_LOOKBACK bars
        # ------------------------------------------------------------------ #
        window = bars[-DELTA_LOOKBACK:]
        delta_values: list[float] = []
        vol_values: list[float] = []
        for bar in window:
            bh = float(bar["high"])
            bl = float(bar["low"])
            bo = float(bar["open"])
            bc = float(bar["close"])
            bv = float(bar.get("volume") or 0.0)
            delta_values.append(_clv(bo, bh, bl, bc) * bv)
            vol_values.append(bv)

        avg_vol = mean(vol_values) or 1.0
        cumulative_delta = sum(delta_values)
        # Normalise by (window_size × avg_volume) so the value is scale-invariant
        normaliser = len(delta_values) * avg_vol
        cumulative_delta_10 = safe_div(cumulative_delta, normaliser, default=0.0)

        # ------------------------------------------------------------------ #
        # Cumulative delta slope: compare first half vs second half of window
        # ------------------------------------------------------------------ #
        mid = len(delta_values) // 2
        if mid > 0:
            first_half_sum = sum(delta_values[:mid])
            second_half_sum = sum(delta_values[mid:])
            if second_half_sum > first_half_sum + 1e-9:
                cumulative_delta_slope = 1.0
            elif second_half_sum < first_half_sum - 1e-9:
                cumulative_delta_slope = -1.0
            else:
                cumulative_delta_slope = 0.0
        else:
            cumulative_delta_slope = 0.0

        # ------------------------------------------------------------------ #
        # Volume exhaustion flag
        # ------------------------------------------------------------------ #
        baseline_bars = bars[-(21): -1]  # up to 20 bars before the latest
        bar_range = h - lo
        body = abs(c - o)
        body_ratio = safe_div(body, bar_range, default=1.0) if bar_range > 1e-9 else 1.0

        avg_range_vals = [float(b["high"]) - float(b["low"]) for b in baseline_bars]
        avg_vol_vals = [float(b.get("volume") or 0.0) for b in baseline_bars]
        avg_range = mean(avg_range_vals) or 0.0
        avg_vol_base = mean(avg_vol_vals) or 1.0

        range_to_avg = safe_div(bar_range, avg_range, default=0.0) if avg_range > 0 else 0.0
        vol_to_avg = safe_div(v, avg_vol_base, default=0.0)

        volume_exhaustion_flag = int(
            range_to_avg >= EXHAUSTION_RANGE_MULTIPLE
            and body_ratio <= EXHAUSTION_BODY_RATIO_MAX
            and vol_to_avg >= EXHAUSTION_VOLUME_MULTIPLE
        )

        # ------------------------------------------------------------------ #
        # Delta divergence flag: price trend vs cumulative delta trend disagree
        # ------------------------------------------------------------------ #
        price_rising = 1 if c > o else (-1 if c < o else 0)
        delta_slope_int = int(cumulative_delta_slope)
        delta_divergence_flag = int(
            price_rising != 0
            and delta_slope_int != 0
            and price_rising != delta_slope_int
        )

        return {
            "buying_pressure": round(buying_pressure, 6),
            "selling_pressure": round(selling_pressure, 6),
            "bid_ask_imbalance": round(bid_ask_imbalance, 6),
            "volume_delta_proxy": round(volume_delta_proxy, 4),
            "cumulative_delta_10": round(cumulative_delta_10, 6),
            "cumulative_delta_slope": cumulative_delta_slope,
            "volume_exhaustion_flag": volume_exhaustion_flag,
            "delta_divergence_flag": delta_divergence_flag,
        }
