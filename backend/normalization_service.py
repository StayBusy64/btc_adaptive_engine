from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional


_SIDE_ALIASES = {
    "buy": "long",
    "bull": "long",
    "bullish": "long",
    "entry_long": "long",
    "go_long_now": "long",
    "long": "long",
    "sell": "short",
    "bear": "short",
    "bearish": "short",
    "entry_short": "short",
    "go_short_now": "short",
    "short": "short",
}

_TIMEFRAME_ALIASES = {
    "1": "1m",
    "1m": "1m",
    "3": "3m",
    "3m": "3m",
    "5": "5m",
    "5m": "5m",
    "15": "15m",
    "15m": "15m",
    "30": "30m",
    "30m": "30m",
    "60": "1h",
    "1h": "1h",
    "240": "4h",
    "4h": "4h",
    "1d": "1d",
    "d": "1d",
}

_NUMERIC_FEATURE_KEYS = (
    "atr",
    "volume_ratio",
    "pressure_index",
    "participation_score",
    "confidence_seed",
    "price",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "fast_ema",
    "slow_ema",
    "rsi",
    "bar_index",
)

_RESEARCH_NUMERIC_FEATURE_KEYS = (
    "signal_quality_score",
    "trend_slope_score",
    "continuation_confidence",
    "mean_reversion_risk",
    "regime_bias_score",
    "contradiction_pressure",
    "macro_bias_score",
    "cohort_alignment_score",
)

_PASSTHROUGH_FEATURE_KEYS = (
    "signal_family",
    "setup_family",
    "signal_name",
    "strategy_id",
    "session",
    "regime",
    "bar_time",
    "release_id",
    "release_version",
    "release_channel",
    "contract_version",
    "telemetry_schema_version",
)


def _to_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise ValueError("symbol is required")
    return symbol


def _normalize_timeframe(value: Any) -> str:
    cleaned = str(value or "").strip().lower()
    if not cleaned:
        raise ValueError("timeframe is required")
    return _TIMEFRAME_ALIASES.get(cleaned, cleaned)


def _normalize_side(value: Any) -> str:
    cleaned = str(value or "").strip().lower()
    if not cleaned:
        raise ValueError("side is required")
    normalized = _SIDE_ALIASES.get(cleaned, cleaned)
    if normalized not in {"long", "short"}:
        raise ValueError("side must normalize to 'long' or 'short'")
    return normalized


def _to_optional_iso(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.isoformat()


def _normalize_text_field(value: Any, default: str) -> str:
    cleaned = str(value or "").strip()
    return cleaned if cleaned else default


def _build_features(payload: Dict[str, Any]) -> Dict[str, Any]:
    features: Dict[str, Any] = {}
    for key in _NUMERIC_FEATURE_KEYS:
        if key in payload:
            numeric = _to_optional_float(payload.get(key))
            features[key] = numeric if numeric is not None else payload.get(key)

    for key in _PASSTHROUGH_FEATURE_KEYS:
        value = payload.get(key)
        if value is not None:
            features[key] = value

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        for key, value in metadata.items():
            features[str(key)] = value

    research = payload.get("research")
    if isinstance(research, dict):
        normalized_research: Dict[str, Any] = {}
        for key, value in research.items():
            key_text = str(key)
            if key_text in _RESEARCH_NUMERIC_FEATURE_KEYS:
                numeric = _to_optional_float(value)
                normalized_research[key_text] = numeric if numeric is not None else value
                features[key_text] = normalized_research[key_text]
            else:
                normalized_research[key_text] = value

        if normalized_research:
            features["research_context"] = normalized_research

    release_context = {
        key: features[key]
        for key in (
            "release_id",
            "release_version",
            "release_channel",
            "contract_version",
            "telemetry_schema_version",
        )
        if key in features
    }
    if release_context:
        features["release_context"] = release_context

    return features


def normalize_tradingview_alert(
    *,
    event_id: str,
    payload: Dict[str, Any],
    received_at: Optional[str] = None,
) -> Dict[str, Any]:
    source = str(payload.get("source") or "tradingview").strip().lower() or "tradingview"
    symbol = _normalize_symbol(payload.get("symbol"))
    timeframe = _normalize_timeframe(payload.get("timeframe"))
    side = _normalize_side(payload.get("side") or payload.get("direction"))
    signal_name = _normalize_text_field(payload.get("signal_name") or payload.get("signal"), "unknown_signal")
    strategy_id = _normalize_text_field(payload.get("strategy_id") or payload.get("strategy"), "unknown_strategy")

    score = _to_optional_float(payload.get("score"))
    bar_time = _to_optional_iso(payload.get("bar_time") or payload.get("timestamp"))
    market_price = _to_optional_float(payload.get("price") or payload.get("market_price"))
    features = _build_features(payload)

    if score is None:
        score = _to_optional_float(features.get("signal_quality_score"))

    normalized = {
        "event_id": event_id,
        "source": source,
        "symbol": symbol,
        "broker_symbol": symbol,
        "timeframe": timeframe,
        "side": side,
        "signal_name": signal_name,
        "strategy_id": strategy_id,
        "score": score,
        "received_at": received_at,
        "bar_time": bar_time,
        "market_price": market_price,
        "features": features,
    }

    return normalized
