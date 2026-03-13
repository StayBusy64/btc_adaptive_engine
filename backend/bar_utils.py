from __future__ import annotations

import json
from typing import Any

from backend.feature_math import to_optional_float


def decode_json_payload(raw_payload: Any) -> Any:
    if raw_payload is None:
        return None

    if isinstance(raw_payload, dict):
        return raw_payload

    if not isinstance(raw_payload, str):
        return None

    try:
        return json.loads(raw_payload)
    except json.JSONDecodeError:
        return None


def first_float(payload: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        if key not in payload:
            continue
        numeric = to_optional_float(payload.get(key))
        if numeric is not None:
            return numeric
    return None


def normalize_bar_row(row: dict[str, Any], previous_close: float | None = None) -> dict[str, Any] | None:
    payload = decode_json_payload(row.get("payload_json"))
    if not isinstance(payload, dict):
        return None

    open_price = first_float(payload, ["open", "o", "bar_open"])
    high_price = first_float(payload, ["high", "h", "bar_high"])
    low_price = first_float(payload, ["low", "l", "bar_low"])
    close_price = first_float(payload, ["close", "c", "last", "price"])
    volume = first_float(payload, ["volume", "v", "vol"]) or 0.0

    if close_price is None and high_price is not None and low_price is not None:
        close_price = (high_price + low_price) / 2.0

    if close_price is None:
        return None

    if open_price is None:
        open_price = previous_close if previous_close is not None else close_price

    if high_price is None:
        high_price = max(open_price, close_price)

    if low_price is None:
        low_price = min(open_price, close_price)

    if high_price < low_price:
        high_price, low_price = low_price, high_price

    return {
        "id": row.get("id"),
        "timestamp": row.get("timestamp"),
        "symbol": row.get("symbol"),
        "timeframe": row.get("timeframe"),
        "open": float(open_price),
        "high": float(high_price),
        "low": float(low_price),
        "close": float(close_price),
        "volume": float(volume),
    }


def normalize_bar_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    previous_close: float | None = None

    for row in rows:
        normalized_row = normalize_bar_row(row, previous_close=previous_close)
        if normalized_row is None:
            continue

        normalized.append(normalized_row)
        previous_close = normalized_row["close"]

    return normalized
