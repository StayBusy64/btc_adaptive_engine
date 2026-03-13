from __future__ import annotations

import math
import os
from typing import Any, Sequence


def to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def safe_div(numerator: float, denominator: float, *, default: float = 0.0) -> float:
    if denominator == 0:
        return default
    return numerator / denominator


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def sma(values: Sequence[float], period: int) -> float | None:
    if not values:
        return None
    safe_period = max(1, int(period))
    window = values[-safe_period:]
    return mean(window)


def ema_series(values: Sequence[float], period: int) -> list[float]:
    if not values:
        return []

    safe_period = max(1, int(period))
    alpha = 2.0 / (safe_period + 1.0)

    result: list[float] = [float(values[0])]
    for value in values[1:]:
        current = (alpha * float(value)) + ((1.0 - alpha) * result[-1])
        result.append(current)

    return result


def ema(values: Sequence[float], period: int) -> float | None:
    if not values:
        return None
    series = ema_series(values, period)
    return series[-1] if series else None


def rolling_std(values: Sequence[float], period: int) -> float:
    if not values:
        return 0.0

    safe_period = max(1, int(period))
    window = values[-safe_period:]
    if len(window) < 2:
        return 0.0

    avg = sum(window) / len(window)
    variance = sum((value - avg) ** 2 for value in window) / len(window)
    return math.sqrt(variance)


def true_range_series(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]) -> list[float]:
    size = min(len(highs), len(lows), len(closes))
    if size == 0:
        return []

    tr_values: list[float] = []
    previous_close = float(closes[0])

    for index in range(size):
        high = float(highs[index])
        low = float(lows[index])

        if index == 0:
            tr = high - low
        else:
            tr = max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )

        tr_values.append(tr)
        previous_close = float(closes[index])

    return tr_values


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> float | None:
    tr_values = true_range_series(highs, lows, closes)
    if not tr_values:
        return None
    return sma(tr_values, period)


def rsi(closes: Sequence[float], period: int = 14) -> float | None:
    if len(closes) < 2:
        return None

    deltas = [float(closes[i]) - float(closes[i - 1]) for i in range(1, len(closes))]
    safe_period = max(1, min(int(period), len(deltas)))

    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]

    avg_gain = sum(gains[:safe_period]) / safe_period
    avg_loss = sum(losses[:safe_period]) / safe_period

    for delta in deltas[safe_period:]:
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = ((avg_gain * (safe_period - 1)) + gain) / safe_period
        avg_loss = ((avg_loss * (safe_period - 1)) + loss) / safe_period

    if abs(avg_loss) <= 1e-12:
        return 100.0 if avg_gain > 0.0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def macd(
    closes: Sequence[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[float | None, float | None, float | None]:
    if not closes:
        return None, None, None

    fast_series = ema_series(closes, fast_period)
    slow_series = ema_series(closes, slow_period)
    if not fast_series or not slow_series:
        return None, None, None

    macd_series = [fast_series[i] - slow_series[i] for i in range(min(len(fast_series), len(slow_series)))]
    if not macd_series:
        return None, None, None

    signal_series = ema_series(macd_series, signal_period)
    if not signal_series:
        return macd_series[-1], None, None

    macd_value = macd_series[-1]
    signal_value = signal_series[-1]
    hist_value = macd_value - signal_value
    return macd_value, signal_value, hist_value


def get_env_bool(name: str, *, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def get_env_float(name: str, default: float, *, minimum: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default

    try:
        parsed = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc

    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")

    return parsed


def get_env_int(name: str, default: int, *, minimum: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default

    try:
        parsed = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc

    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")

    return parsed


def stochastic_k(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> float | None:
    size = min(len(highs), len(lows), len(closes))
    if size == 0:
        return None

    safe_period = max(1, min(int(period), size))
    window_high = max(float(value) for value in highs[-safe_period:])
    window_low = min(float(value) for value in lows[-safe_period:])
    close_value = float(closes[-1])

    if window_high == window_low:
        return 50.0

    return ((close_value - window_low) / (window_high - window_low)) * 100.0


def stochastic_d(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
    smooth: int = 3,
) -> float | None:
    size = min(len(highs), len(lows), len(closes))
    if size == 0:
        return None

    safe_smooth = max(1, int(smooth))
    k_values: list[float] = []
    for offset in range(safe_smooth):
        end = size - offset
        if end <= 0:
            break
        k_value = stochastic_k(highs[:end], lows[:end], closes[:end], period=period)
        if k_value is not None:
            k_values.append(k_value)

    if not k_values:
        return None

    return sum(k_values) / len(k_values)


def linear_regression_slope(values: Sequence[float], period: int) -> float:
    if not values:
        return 0.0

    safe_period = max(2, min(int(period), len(values)))
    window = [float(value) for value in values[-safe_period:]]

    x_values = list(range(safe_period))
    x_mean = sum(x_values) / safe_period
    y_mean = sum(window) / safe_period

    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, window))
    denominator = sum((x - x_mean) ** 2 for x in x_values)

    if abs(denominator) <= 1e-12:
        return 0.0

    return numerator / denominator
