from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.tradingview_ingest_storage import ensure_ingest_directories, get_ingest_paths

_SIGNAL_JOURNAL_FILE = "signal_journal.jsonl"
_SIGNAL_OUTCOMES_FILE = "signal_outcomes.jsonl"

DEFAULT_MIN_FUTURE_BARS = 5
DEFAULT_CONTINUATION_THRESHOLD_PCT = 0.0015
DEFAULT_HORIZON_BARS = (1, 3, 5, 10)
_EPSILON = 1e-12


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_iso_from_epoch_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_epoch_ms(value: Any) -> Optional[int]:
    parsed_int = _safe_int(value)
    if parsed_int is not None and parsed_int > 0:
        return parsed_int

    if isinstance(value, datetime):
        parsed_dt = value
    else:
        text = str(value or "").strip()
        if not text:
            return None

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            parsed_dt = datetime.fromisoformat(text)
        except ValueError:
            return None

    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)

    epoch_ms = int(parsed_dt.timestamp() * 1000)
    return epoch_ms if epoch_ms > 0 else None


def _safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None

    cleaned = str(value).strip()
    return cleaned or None


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _is_non_zero(value: Optional[float]) -> bool:
    return value is not None and abs(value) > _EPSILON


def _extract_float_from_context(contexts: tuple[dict[str, Any], ...], keys: tuple[str, ...]) -> Optional[float]:
    for context in contexts:
        for key in keys:
            if key not in context:
                continue
            parsed = _safe_float(context.get(key))
            if parsed is not None:
                return parsed
    return None


def _extract_any_from_context(contexts: tuple[dict[str, Any], ...], keys: tuple[str, ...]) -> Any:
    for context in contexts:
        for key in keys:
            if key in context:
                return context.get(key)
    return None


def _normalize_side(value: Any) -> str:
    cleaned = str(value or "").strip().lower()
    if cleaned in {"buy", "long"}:
        return "long"
    if cleaned in {"sell", "short"}:
        return "short"
    return "long"


def _journal_path() -> Path:
    ensure_ingest_directories()
    return get_ingest_paths().state_outcomes / _SIGNAL_JOURNAL_FILE


def _outcome_path() -> Path:
    ensure_ingest_directories()
    return get_ingest_paths().state_outcomes / _SIGNAL_OUTCOMES_FILE


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False))
        handle.write("\n")


def load_signal_journal_rows() -> list[dict[str, Any]]:
    rows = _load_jsonl(_journal_path())
    rows.sort(key=lambda row: int(row.get("event_time_ms") or 0))
    return rows


def load_signal_outcome_rows() -> list[dict[str, Any]]:
    rows = _load_jsonl(_outcome_path())
    rows.sort(key=lambda row: str(row.get("evaluated_at") or ""), reverse=True)
    return rows


def _build_signal_snapshot(normalized_event: dict[str, Any]) -> Optional[dict[str, Any]]:
    signal_id = str(normalized_event.get("event_id") or "").strip()
    if not signal_id:
        return None

    micro_context = _as_dict(normalized_event.get("micro_context"))
    macro_context = _as_dict(normalized_event.get("macro_context"))
    release_context = _as_dict(normalized_event.get("release_context"))
    research_context = _as_dict(normalized_event.get("research_context"))
    research_unknown_context = _as_dict(normalized_event.get("research_unknown_context"))
    contexts = (micro_context, macro_context)

    event_time_ms = _resolve_event_time_ms(normalized_event)
    fields = _extract_signal_fields(normalized_event, contexts)
    derived = _derive_signal_metrics(fields)

    bar_index = _safe_int(_extract_any_from_context(contexts, ("bar_index", "index")))
    bar_time_raw = _extract_any_from_context(contexts, ("bar_time", "time", "timestamp"))

    return {
        "signal_id": signal_id,
        "event_id": signal_id,
        "batch_id": str(normalized_event.get("batch_id") or ""),
        "event_order": _safe_int(normalized_event.get("event_order")) or 0,
        "recorded_at": _utc_now_iso(),
        "timestamp": _to_iso_from_epoch_ms(event_time_ms),
        "event_time_ms": event_time_ms,
        "symbol": str(normalized_event.get("symbol") or ""),
        "timeframe": str(normalized_event.get("chart_tf") or ""),
        "side": fields["side"],
        "signal_name": fields["signal_name"],
        "signal_family": fields["signal_family"],
        "strategy_id": _safe_text(normalized_event.get("strategy_id")) or _safe_text(micro_context.get("strategy_id")),
        "release_id": _safe_text(normalized_event.get("release_id")) or _safe_text(release_context.get("release_id")),
        "release_version": _safe_text(normalized_event.get("release_version")) or _safe_text(release_context.get("release_version")),
        "release_channel": _safe_text(normalized_event.get("release_channel")) or _safe_text(release_context.get("release_channel")),
        "contract_version": _safe_text(normalized_event.get("contract_version")) or _safe_text(release_context.get("contract_version")),
        "telemetry_schema_version": _safe_text(normalized_event.get("telemetry_schema_version")) or _safe_text(release_context.get("telemetry_schema_version")),
        # --- OHLCV ---
        "price": fields["price"],
        "open": fields["open"],
        "high": fields["high"],
        "low": fields["low"],
        "close": fields["close"],
        "volume": fields["volume"],
        "hl2": fields["hl2"],
        "hlc3": fields["hlc3"],
        "ohlc4": fields["ohlc4"],
        # --- EMAs / indicators (Tier 1) ---
        "fast_ema": fields["fast_ema"],
        "slow_ema": fields["slow_ema"],
        "ema_trend": fields["ema_trend"],
        "rsi": fields["rsi"],
        "atr": fields["atr"],
        "atr_pct": fields["atr_pct"],
        "volume_sma": fields["volume_sma"],
        "volume_ratio": fields["volume_ratio"],
        # --- candle anatomy (Tier 1 Pine-logged) ---
        "body_size": fields["body_size"],
        "range_size": fields["range_size"],
        "upper_wick": fields["upper_wick"],
        "lower_wick": fields["lower_wick"],
        "body_pct_of_range": fields["body_pct_of_range"],
        "upper_wick_pct": fields["upper_wick_pct"],
        "lower_wick_pct": fields["lower_wick_pct"],
        # --- trend / regime (Tier 1 categorical) ---
        "ema_bull_stack": fields["ema_bull_stack"],
        "ema_bear_stack": fields["ema_bear_stack"],
        "trend_direction": fields["trend_direction"],
        "price_vs_trend": fields["price_vs_trend"],
        "momentum_regime": fields["momentum_regime"],
        "volatility_regime": fields["volatility_regime"],
        "volume_regime": fields["volume_regime"],
        "candle_bias": fields["candle_bias"],
        "wick_bias": fields["wick_bias"],
        # --- volumatic S/R (Tier 1) ---
        "volumatic_upper_level": fields["volumatic_upper_level"],
        "volumatic_lower_level": fields["volumatic_lower_level"],
        "volumatic_n_vol": fields["volumatic_n_vol"],
        "volumatic_upper_band_high": fields["volumatic_upper_band_high"],
        "volumatic_upper_band_low": fields["volumatic_upper_band_low"],
        "volumatic_lower_band_high": fields["volumatic_lower_band_high"],
        "volumatic_lower_band_low": fields["volumatic_lower_band_low"],
        # --- swing structure (Tier 1) ---
        "internal_swing_high": fields["internal_swing_high"],
        "internal_swing_low": fields["internal_swing_low"],
        "major_swing_high": fields["major_swing_high"],
        "major_swing_low": fields["major_swing_low"],
        # --- prediction map (Tier 1) ---
        "prediction_swing_level": fields["prediction_swing_level"],
        "inducement_level": fields["inducement_level"],
        "continuation_level": fields["continuation_level"],
        "invalidation_level": fields["invalidation_level"],
        "displacement_origin": fields["displacement_origin"],
        "displacement_far_edge": fields["displacement_far_edge"],
        "bull_displacement": fields["bull_displacement"],
        "bear_displacement": fields["bear_displacement"],
        "probability_score": fields["probability_score"],
        # --- Tier 2: existing EMA-derived ---
        "ema_spread": derived["ema_spread"],
        "ema_spread_pct": derived["ema_spread_pct"],
        "distance_from_fast": derived["distance_from_fast"],
        "distance_from_fast_pct": derived["distance_from_fast_pct"],
        "distance_from_slow": derived["distance_from_slow"],
        "distance_from_slow_pct": derived["distance_from_slow_pct"],
        "candle_range": derived["candle_range"],
        "body": derived["body"],
        "wick_ratio": derived["wick_ratio"],
        # --- Tier 2: trend EMA distance ---
        "distance_to_ema_trend": derived["distance_to_ema_trend"],
        "distance_to_ema_trend_pct": derived["distance_to_ema_trend_pct"],
        "distance_to_ema_trend_atr": derived["distance_to_ema_trend_atr"],
        # --- Tier 2: candle position ---
        "close_position_in_range": derived["close_position_in_range"],
        # --- Tier 2: volumatic distances ---
        "distance_to_volumatic_upper": derived["distance_to_volumatic_upper"],
        "distance_to_volumatic_upper_atr": derived["distance_to_volumatic_upper_atr"],
        "distance_to_volumatic_lower": derived["distance_to_volumatic_lower"],
        "distance_to_volumatic_lower_atr": derived["distance_to_volumatic_lower_atr"],
        # --- Tier 2: prediction structure distances ---
        "distance_to_prediction_swing": derived["distance_to_prediction_swing"],
        "distance_to_prediction_swing_atr": derived["distance_to_prediction_swing_atr"],
        "distance_to_inducement": derived["distance_to_inducement"],
        "distance_to_inducement_atr": derived["distance_to_inducement_atr"],
        "distance_to_continuation": derived["distance_to_continuation"],
        "distance_to_continuation_atr": derived["distance_to_continuation_atr"],
        "distance_to_invalidation": derived["distance_to_invalidation"],
        "distance_to_invalidation_atr": derived["distance_to_invalidation_atr"],
        # --- Tier 2: R-multiples & confluence ---
        "rr_to_target": derived["rr_to_target"],
        "rr_to_continuation": derived["rr_to_continuation"],
        "confluence_count": derived["confluence_count"],
        "confluence_score": derived["confluence_score"],
        "signal_alignment_score": derived["signal_alignment_score"],
        # --- Tier 2: swing distances ---
        "distance_to_internal_high": derived["distance_to_internal_high"],
        "distance_to_internal_high_atr": derived["distance_to_internal_high_atr"],
        "distance_to_internal_low": derived["distance_to_internal_low"],
        "distance_to_internal_low_atr": derived["distance_to_internal_low_atr"],
        "distance_to_major_high": derived["distance_to_major_high"],
        "distance_to_major_high_atr": derived["distance_to_major_high_atr"],
        "distance_to_major_low": derived["distance_to_major_low"],
        "distance_to_major_low_atr": derived["distance_to_major_low_atr"],
        # --- raw contexts ---
        "bar_index": bar_index,
        "bar_time": str(bar_time_raw) if bar_time_raw is not None else None,
        "micro_context": micro_context,
        "macro_context": macro_context,
        "release_context": release_context,
        "research_context": research_context,
        "research_unknown_context": research_unknown_context,
    }


def _resolve_normalized_signal_id(normalized_signal: dict[str, Any]) -> str:
    """Extract and normalise the signal ID from a normalized-signal dict."""
    return str(
        normalized_signal.get("event_id")
        or normalized_signal.get("signal_id")
        or normalized_signal.get("normalized_id")
        or ""
    ).strip()


def _resolve_normalized_price(normalized_signal: dict[str, Any], feature_context: dict[str, Any]) -> Any:
    """Resolve the market price from the normalized signal with feature-context fallbacks."""
    price_value = normalized_signal.get("market_price")
    if price_value is None:
        price_value = feature_context.get("price")
    if price_value is None:
        price_value = feature_context.get("close")
    return price_value


def _resolve_normalized_event_time_ms(normalized_signal: dict[str, Any]) -> int:
    """Resolve event time (epoch ms) from a normalized signal with fallbacks."""
    event_time_ms = _safe_epoch_ms(normalized_signal.get("bar_time"))
    if event_time_ms is None:
        event_time_ms = _safe_epoch_ms(normalized_signal.get("received_at"))
    if event_time_ms is None:
        event_time_ms = _resolve_event_time_ms({})
    return event_time_ms


def build_signal_snapshot_from_normalized_signal(normalized_signal: dict[str, Any]) -> Optional[dict[str, Any]]:
    signal_id = _resolve_normalized_signal_id(normalized_signal)
    if not signal_id:
        return None

    feature_context = _as_dict(normalized_signal.get("features"))
    release_context = _as_dict(feature_context.get("release_context"))
    research_context = _as_dict(feature_context.get("research_context"))
    research_unknown_context = _as_dict(feature_context.get("research_unknown_context"))
    contexts = (feature_context,)
    side_value = normalized_signal.get("side")
    signal_name = normalized_signal.get("signal_name") or feature_context.get("signal_name")
    signal_family = feature_context.get("signal_family") or feature_context.get("setup_family") or "phase_one"
    price_value = _resolve_normalized_price(normalized_signal, feature_context)

    signal_event = {
        "side": side_value,
        "signal_type": signal_name,
        "signal_family": signal_family,
        "price": price_value,
    }
    fields = _extract_signal_fields(signal_event, contexts)
    derived = _derive_signal_metrics(fields)

    event_time_ms = _resolve_normalized_event_time_ms(normalized_signal)

    bar_index = _safe_int(feature_context.get("bar_index"))
    bar_time_raw = normalized_signal.get("bar_time") or feature_context.get("bar_time")

    return {
        "signal_id": signal_id,
        "event_id": str(normalized_signal.get("event_id") or signal_id),
        "batch_id": str(normalized_signal.get("batch_id") or "phase_one"),
        "event_order": 0,
        "recorded_at": _utc_now_iso(),
        "timestamp": _to_iso_from_epoch_ms(event_time_ms),
        "event_time_ms": event_time_ms,
        "symbol": str(normalized_signal.get("symbol") or ""),
        "timeframe": str(normalized_signal.get("timeframe") or ""),
        "side": fields["side"],
        "signal_name": fields["signal_name"],
        "signal_family": fields["signal_family"],
        "strategy_id": _safe_text(normalized_signal.get("strategy_id")) or _safe_text(feature_context.get("strategy_id")),
        "release_id": _safe_text(feature_context.get("release_id")) or _safe_text(release_context.get("release_id")),
        "release_version": _safe_text(feature_context.get("release_version")) or _safe_text(release_context.get("release_version")),
        "release_channel": _safe_text(feature_context.get("release_channel")) or _safe_text(release_context.get("release_channel")),
        "contract_version": _safe_text(feature_context.get("contract_version")) or _safe_text(release_context.get("contract_version")),
        "telemetry_schema_version": _safe_text(feature_context.get("telemetry_schema_version")) or _safe_text(release_context.get("telemetry_schema_version")),
        # --- OHLCV ---
        "price": fields["price"],
        "open": fields["open"],
        "high": fields["high"],
        "low": fields["low"],
        "close": fields["close"],
        "volume": fields["volume"],
        "hl2": fields["hl2"],
        "hlc3": fields["hlc3"],
        "ohlc4": fields["ohlc4"],
        # --- EMAs / indicators ---
        "fast_ema": fields["fast_ema"],
        "slow_ema": fields["slow_ema"],
        "ema_trend": fields["ema_trend"],
        "rsi": fields["rsi"],
        "atr": fields["atr"],
        "atr_pct": fields["atr_pct"],
        "volume_sma": fields["volume_sma"],
        "volume_ratio": fields["volume_ratio"],
        # --- candle anatomy ---
        "body_size": fields["body_size"],
        "range_size": fields["range_size"],
        "upper_wick": fields["upper_wick"],
        "lower_wick": fields["lower_wick"],
        "body_pct_of_range": fields["body_pct_of_range"],
        "upper_wick_pct": fields["upper_wick_pct"],
        "lower_wick_pct": fields["lower_wick_pct"],
        # --- trend / regime ---
        "ema_bull_stack": fields["ema_bull_stack"],
        "ema_bear_stack": fields["ema_bear_stack"],
        "trend_direction": fields["trend_direction"],
        "price_vs_trend": fields["price_vs_trend"],
        "momentum_regime": fields["momentum_regime"],
        "volatility_regime": fields["volatility_regime"],
        "volume_regime": fields["volume_regime"],
        "candle_bias": fields["candle_bias"],
        "wick_bias": fields["wick_bias"],
        # --- volumatic S/R ---
        "volumatic_upper_level": fields["volumatic_upper_level"],
        "volumatic_lower_level": fields["volumatic_lower_level"],
        "volumatic_n_vol": fields["volumatic_n_vol"],
        "volumatic_upper_band_high": fields["volumatic_upper_band_high"],
        "volumatic_upper_band_low": fields["volumatic_upper_band_low"],
        "volumatic_lower_band_high": fields["volumatic_lower_band_high"],
        "volumatic_lower_band_low": fields["volumatic_lower_band_low"],
        # --- swing structure ---
        "internal_swing_high": fields["internal_swing_high"],
        "internal_swing_low": fields["internal_swing_low"],
        "major_swing_high": fields["major_swing_high"],
        "major_swing_low": fields["major_swing_low"],
        # --- prediction map ---
        "prediction_swing_level": fields["prediction_swing_level"],
        "inducement_level": fields["inducement_level"],
        "continuation_level": fields["continuation_level"],
        "invalidation_level": fields["invalidation_level"],
        "displacement_origin": fields["displacement_origin"],
        "displacement_far_edge": fields["displacement_far_edge"],
        "bull_displacement": fields["bull_displacement"],
        "bear_displacement": fields["bear_displacement"],
        "probability_score": fields["probability_score"],
        # --- Tier 2: EMA-derived ---
        "ema_spread": derived["ema_spread"],
        "ema_spread_pct": derived["ema_spread_pct"],
        "distance_from_fast": derived["distance_from_fast"],
        "distance_from_fast_pct": derived["distance_from_fast_pct"],
        "distance_from_slow": derived["distance_from_slow"],
        "distance_from_slow_pct": derived["distance_from_slow_pct"],
        "candle_range": derived["candle_range"],
        "body": derived["body"],
        "wick_ratio": derived["wick_ratio"],
        # --- Tier 2: trend EMA distance ---
        "distance_to_ema_trend": derived["distance_to_ema_trend"],
        "distance_to_ema_trend_pct": derived["distance_to_ema_trend_pct"],
        "distance_to_ema_trend_atr": derived["distance_to_ema_trend_atr"],
        # --- Tier 2: candle position ---
        "close_position_in_range": derived["close_position_in_range"],
        # --- Tier 2: volumatic distances ---
        "distance_to_volumatic_upper": derived["distance_to_volumatic_upper"],
        "distance_to_volumatic_upper_atr": derived["distance_to_volumatic_upper_atr"],
        "distance_to_volumatic_lower": derived["distance_to_volumatic_lower"],
        "distance_to_volumatic_lower_atr": derived["distance_to_volumatic_lower_atr"],
        # --- Tier 2: prediction structure distances ---
        "distance_to_prediction_swing": derived["distance_to_prediction_swing"],
        "distance_to_prediction_swing_atr": derived["distance_to_prediction_swing_atr"],
        "distance_to_inducement": derived["distance_to_inducement"],
        "distance_to_inducement_atr": derived["distance_to_inducement_atr"],
        "distance_to_continuation": derived["distance_to_continuation"],
        "distance_to_continuation_atr": derived["distance_to_continuation_atr"],
        "distance_to_invalidation": derived["distance_to_invalidation"],
        "distance_to_invalidation_atr": derived["distance_to_invalidation_atr"],
        # --- Tier 2: R-multiples & confluence ---
        "rr_to_target": derived["rr_to_target"],
        "rr_to_continuation": derived["rr_to_continuation"],
        "confluence_count": derived["confluence_count"],
        "confluence_score": derived["confluence_score"],
        "signal_alignment_score": derived["signal_alignment_score"],
        # --- Tier 2: swing distances ---
        "distance_to_internal_high": derived["distance_to_internal_high"],
        "distance_to_internal_high_atr": derived["distance_to_internal_high_atr"],
        "distance_to_internal_low": derived["distance_to_internal_low"],
        "distance_to_internal_low_atr": derived["distance_to_internal_low_atr"],
        "distance_to_major_high": derived["distance_to_major_high"],
        "distance_to_major_high_atr": derived["distance_to_major_high_atr"],
        "distance_to_major_low": derived["distance_to_major_low"],
        "distance_to_major_low_atr": derived["distance_to_major_low_atr"],
        # --- raw contexts ---
        "bar_index": bar_index,
        "bar_time": str(bar_time_raw) if bar_time_raw is not None else None,
        "micro_context": feature_context,
        "macro_context": {},
        "release_context": release_context,
        "research_context": research_context,
        "research_unknown_context": research_unknown_context,
    }


def _resolve_event_time_ms(normalized_event: dict[str, Any]) -> int:
    event_time_ms = _safe_int(normalized_event.get("event_time"))
    if event_time_ms is not None and event_time_ms > 0:
        return event_time_ms

    batch_close_time = _safe_int(normalized_event.get("batch_close_time"))
    if batch_close_time is not None and batch_close_time > 0:
        return batch_close_time

    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _extract_signal_fields(
    normalized_event: dict[str, Any],
    contexts: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    price = _safe_float(normalized_event.get("price"))
    close_value = _extract_float_from_context(contexts, ("close", "c", "bar_close", "last"))
    if close_value is None:
        close_value = price

    return {
        # --- identity ---
        "side": _normalize_side(normalized_event.get("side")),
        "signal_name": _safe_text(normalized_event.get("signal_name"))
        or _safe_text(normalized_event.get("signal_type"))
        or "unknown_signal",
        "signal_family": _safe_text(normalized_event.get("signal_family")) or "unknown_family",
        # --- OHLCV ---
        "price": price,
        "open": _extract_float_from_context(contexts, ("open", "o", "bar_open")),
        "high": _extract_float_from_context(contexts, ("high", "h", "bar_high")),
        "low": _extract_float_from_context(contexts, ("low", "l", "bar_low")),
        "close": close_value,
        "volume": _extract_float_from_context(contexts, ("volume", "vol", "v")),
        "hl2": _extract_float_from_context(contexts, ("hl2",)),
        "hlc3": _extract_float_from_context(contexts, ("hlc3",)),
        "ohlc4": _extract_float_from_context(contexts, ("ohlc4",)),
        # --- EMAs / indicators ---
        "fast_ema": _extract_float_from_context(contexts, ("fast_ema", "ema_fast", "ema9")),
        "slow_ema": _extract_float_from_context(contexts, ("slow_ema", "ema_slow", "ema21")),
        "ema_trend": _extract_float_from_context(contexts, ("ema_trend", "trend_ema")),
        "rsi": _extract_float_from_context(contexts, ("rsi", "rsi_value", "rsi_val")),
        "atr": _extract_float_from_context(contexts, ("atr", "atr_value")),
        "atr_pct": _extract_float_from_context(contexts, ("atr_pct",)),
        "volume_sma": _extract_float_from_context(contexts, ("volume_sma", "vol_sma")),
        "volume_ratio": _extract_float_from_context(contexts, ("volume_ratio",)),
        # --- candle anatomy (Pine-logged) ---
        "body_size": _extract_float_from_context(contexts, ("body_size",)),
        "range_size": _extract_float_from_context(contexts, ("range_size",)),
        "upper_wick": _extract_float_from_context(contexts, ("upper_wick",)),
        "lower_wick": _extract_float_from_context(contexts, ("lower_wick",)),
        "body_pct_of_range": _extract_float_from_context(contexts, ("body_pct_of_range",)),
        "upper_wick_pct": _extract_float_from_context(contexts, ("upper_wick_pct",)),
        "lower_wick_pct": _extract_float_from_context(contexts, ("lower_wick_pct",)),
        # --- trend / regime (categorical) ---
        "ema_bull_stack": _extract_any_from_context(contexts, ("ema_bull_stack",)),
        "ema_bear_stack": _extract_any_from_context(contexts, ("ema_bear_stack",)),
        "trend_direction": _safe_text(_extract_any_from_context(contexts, ("trend_direction",))),
        "price_vs_trend": _safe_text(_extract_any_from_context(contexts, ("price_vs_trend",))),
        "momentum_regime": _safe_text(_extract_any_from_context(contexts, ("momentum_regime",))),
        "volatility_regime": _safe_text(_extract_any_from_context(contexts, ("volatility_regime",))),
        "volume_regime": _safe_text(_extract_any_from_context(contexts, ("volume_regime",))),
        "candle_bias": _safe_text(_extract_any_from_context(contexts, ("candle_bias",))),
        "wick_bias": _safe_text(_extract_any_from_context(contexts, ("wick_bias",))),
        # --- volumatic S/R ---
        "volumatic_upper_level": _extract_float_from_context(contexts, ("volumatic_upper_level",)),
        "volumatic_lower_level": _extract_float_from_context(contexts, ("volumatic_lower_level",)),
        "volumatic_n_vol": _extract_float_from_context(contexts, ("volumatic_n_vol",)),
        "volumatic_upper_band_high": _extract_float_from_context(contexts, ("volumatic_upper_band_high",)),
        "volumatic_upper_band_low": _extract_float_from_context(contexts, ("volumatic_upper_band_low",)),
        "volumatic_lower_band_high": _extract_float_from_context(contexts, ("volumatic_lower_band_high",)),
        "volumatic_lower_band_low": _extract_float_from_context(contexts, ("volumatic_lower_band_low",)),
        # --- swing structure ---
        "internal_swing_high": _extract_float_from_context(contexts, ("internal_swing_high",)),
        "internal_swing_low": _extract_float_from_context(contexts, ("internal_swing_low",)),
        "major_swing_high": _extract_float_from_context(contexts, ("major_swing_high",)),
        "major_swing_low": _extract_float_from_context(contexts, ("major_swing_low",)),
        # --- prediction map ---
        "prediction_swing_level": _extract_float_from_context(contexts, ("prediction_swing_level",)),
        "inducement_level": _extract_float_from_context(contexts, ("inducement_level",)),
        "continuation_level": _extract_float_from_context(contexts, ("continuation_level",)),
        "invalidation_level": _extract_float_from_context(contexts, ("invalidation_level",)),
        "displacement_origin": _extract_float_from_context(contexts, ("displacement_origin",)),
        "displacement_far_edge": _extract_float_from_context(contexts, ("displacement_far_edge",)),
        "bull_displacement": _extract_any_from_context(contexts, ("bull_displacement",)),
        "bear_displacement": _extract_any_from_context(contexts, ("bear_displacement",)),
        "probability_score": _extract_float_from_context(contexts, ("probability_score",)),
    }


def _difference(left: Optional[float], right: Optional[float]) -> Optional[float]:
    if left is None or right is None:
        return None
    return left - right


def _percent_of_price(value: Optional[float], price: Optional[float]) -> Optional[float]:
    if value is None or price is None or abs(price) <= _EPSILON:
        return None
    return (value / price) * 100.0


def _body_size(open_price: Optional[float], close_price: Optional[float]) -> Optional[float]:
    if open_price is None or close_price is None:
        return None
    return abs(close_price - open_price)


def _wick_ratio(candle_range: Optional[float], body: Optional[float]) -> Optional[float]:
    if candle_range is None or candle_range <= 0 or body is None:
        return None
    return max(0.0, min(1.0, (candle_range - body) / candle_range))


def _atr_normalise(distance: Optional[float], atr: Optional[float]) -> Optional[float]:
    if distance is None or atr is None or abs(atr) <= _EPSILON:
        return None
    return distance / atr


def _compute_r_multiples(
    dist_prediction_swing: Optional[float],
    dist_continuation: Optional[float],
    dist_invalidation: Optional[float],
) -> tuple[Optional[float], Optional[float]]:
    rr_to_target: Optional[float] = None
    if (dist_prediction_swing is not None
            and dist_invalidation is not None
            and abs(dist_invalidation) > _EPSILON):
        rr_to_target = abs(dist_prediction_swing) / abs(dist_invalidation)

    rr_to_continuation: Optional[float] = None
    if (dist_continuation is not None
            and dist_invalidation is not None
            and abs(dist_invalidation) > _EPSILON):
        rr_to_continuation = abs(dist_continuation) / abs(dist_invalidation)

    return rr_to_target, rr_to_continuation


def _compute_confluence_metrics(
    price: Optional[float],
    atr: Optional[float],
    reference_levels: list[Optional[float]],
) -> tuple[int, float]:
    confluence_count = 0
    confluence_score: float = 0.0
    if price is None or atr is None or abs(atr) <= _EPSILON:
        return confluence_count, confluence_score

    for level in reference_levels:
        if level is None:
            continue
        dist = abs(level - price)
        if dist <= atr:
            confluence_count += 1
        if dist <= 0.5 * atr:
            confluence_score += 1.0
        elif dist <= atr:
            confluence_score += 0.5
        elif dist <= 2.0 * atr:
            confluence_score += 0.25

    return confluence_count, confluence_score


def _check_alignment(side: str, label: str, bullish_kw: str, bearish_kw: str) -> Optional[bool]:
    if not label:
        return None
    if (side == "long" and bullish_kw in label) or (side == "short" and bearish_kw in label):
        return True
    return False


def _safe_lower(value: Any) -> str:
    return str(value).lower() if value else ""


def _volume_alignment(fields: dict[str, Any]) -> Optional[bool]:
    vol_regime = _safe_lower(fields.get("volume_regime"))
    if not vol_regime:
        return None
    if "high" in vol_regime or "above" in vol_regime:
        return True
    return False


def _compute_signal_alignment_score(side: str, fields: dict[str, Any]) -> Optional[float]:
    checks = [
        _check_alignment(side, _safe_lower(fields.get("trend_direction")), "bullish", "bearish"),
        _check_alignment(side, _safe_lower(fields.get("momentum_regime")), "bull", "bear"),
        _volume_alignment(fields),
        _check_alignment(side, _safe_lower(fields.get("candle_bias")), "bull", "bear"),
    ]
    hits = sum(1 for c in checks if c is True)
    total = sum(1 for c in checks if c is not None)
    if total > 0:
        return hits / total
    return None


def _derive_signal_metrics(fields: dict[str, Any]) -> dict[str, Any]:
    price = _safe_float(fields.get("price"))
    open_price = _safe_float(fields.get("open"))
    high_price = _safe_float(fields.get("high"))
    low_price = _safe_float(fields.get("low"))
    close_price = _safe_float(fields.get("close"))
    fast_ema = _safe_float(fields.get("fast_ema"))
    slow_ema = _safe_float(fields.get("slow_ema"))
    ema_trend = _safe_float(fields.get("ema_trend"))
    atr = _safe_float(fields.get("atr"))

    # --- existing EMA-derived ---
    ema_spread = _difference(fast_ema, slow_ema)
    distance_from_fast = _difference(price, fast_ema)
    distance_from_slow = _difference(price, slow_ema)
    candle_range = _difference(high_price, low_price)
    body = _body_size(open_price, close_price)
    wick_ratio = _wick_ratio(candle_range, body)

    ema_spread_pct = _percent_of_price(ema_spread, price)
    distance_from_fast_pct = _percent_of_price(distance_from_fast, price)
    distance_from_slow_pct = _percent_of_price(distance_from_slow, price)

    # --- distance to trend EMA ---
    distance_to_ema_trend = _difference(price, ema_trend)
    distance_to_ema_trend_pct = _percent_of_price(distance_to_ema_trend, price)
    distance_to_ema_trend_atr = _atr_normalise(distance_to_ema_trend, atr)

    # --- close position in range (0 = low, 1 = high) ---
    close_position_in_range = None
    if close_price is not None and low_price is not None and candle_range is not None and candle_range > 0:
        close_position_in_range = (close_price - low_price) / candle_range

    # --- volumatic distances ---
    vol_upper = _safe_float(fields.get("volumatic_upper_level"))
    vol_lower = _safe_float(fields.get("volumatic_lower_level"))

    distance_to_volumatic_upper = _difference(vol_upper, price)
    distance_to_volumatic_upper_atr = _atr_normalise(distance_to_volumatic_upper, atr)
    distance_to_volumatic_lower = _difference(price, vol_lower)
    distance_to_volumatic_lower_atr = _atr_normalise(distance_to_volumatic_lower, atr)

    # --- prediction structure distances ---
    pred_swing = _safe_float(fields.get("prediction_swing_level"))
    induce = _safe_float(fields.get("inducement_level"))
    cont = _safe_float(fields.get("continuation_level"))
    inval = _safe_float(fields.get("invalidation_level"))

    distance_to_prediction_swing = _difference(pred_swing, price) if pred_swing is not None else None
    distance_to_prediction_swing_atr = _atr_normalise(distance_to_prediction_swing, atr)

    distance_to_inducement = _difference(price, induce) if induce is not None else None
    distance_to_inducement_atr = _atr_normalise(distance_to_inducement, atr)

    distance_to_continuation = _difference(cont, price) if cont is not None else None
    distance_to_continuation_atr = _atr_normalise(distance_to_continuation, atr)

    distance_to_invalidation = _difference(price, inval) if inval is not None else None
    distance_to_invalidation_atr = _atr_normalise(distance_to_invalidation, atr)

    # --- swing distances (NEW Tier B) ---
    int_high = _safe_float(fields.get("internal_swing_high"))
    int_low = _safe_float(fields.get("internal_swing_low"))
    maj_high = _safe_float(fields.get("major_swing_high"))
    maj_low = _safe_float(fields.get("major_swing_low"))

    distance_to_internal_high = _difference(int_high, price)
    distance_to_internal_high_atr = _atr_normalise(distance_to_internal_high, atr)
    distance_to_internal_low = _difference(price, int_low)
    distance_to_internal_low_atr = _atr_normalise(distance_to_internal_low, atr)
    distance_to_major_high = _difference(maj_high, price)
    distance_to_major_high_atr = _atr_normalise(distance_to_major_high, atr)
    distance_to_major_low = _difference(price, maj_low)
    distance_to_major_low_atr = _atr_normalise(distance_to_major_low, atr)

    # --- R-multiples ---
    rr_to_target, rr_to_continuation = _compute_r_multiples(
        distance_to_prediction_swing, distance_to_continuation, distance_to_invalidation,
    )

    # --- confluence (count + weighted score) ---
    reference_levels = [
        vol_upper, vol_lower, pred_swing, induce, cont,
        maj_high, maj_low, int_high, int_low,
    ]
    confluence_count, confluence_score = _compute_confluence_metrics(price, atr, reference_levels)

    # --- signal alignment score ---
    side = str(fields.get("side") or "long")
    signal_alignment_score = _compute_signal_alignment_score(side, fields)

    return {
        "ema_spread": ema_spread,
        "distance_from_fast": distance_from_fast,
        "distance_from_slow": distance_from_slow,
        "candle_range": candle_range,
        "body": body,
        "wick_ratio": wick_ratio,
        "ema_spread_pct": ema_spread_pct,
        "distance_from_fast_pct": distance_from_fast_pct,
        "distance_from_slow_pct": distance_from_slow_pct,
        # --- Tier 2: trend EMA distance ---
        "distance_to_ema_trend": distance_to_ema_trend,
        "distance_to_ema_trend_pct": distance_to_ema_trend_pct,
        "distance_to_ema_trend_atr": distance_to_ema_trend_atr,
        # --- Tier 2: candle position ---
        "close_position_in_range": close_position_in_range,
        # --- Tier 2: volumatic distances ---
        "distance_to_volumatic_upper": distance_to_volumatic_upper,
        "distance_to_volumatic_upper_atr": distance_to_volumatic_upper_atr,
        "distance_to_volumatic_lower": distance_to_volumatic_lower,
        "distance_to_volumatic_lower_atr": distance_to_volumatic_lower_atr,
        # --- Tier 2: swing distances ---
        "distance_to_internal_high": distance_to_internal_high,
        "distance_to_internal_high_atr": distance_to_internal_high_atr,
        "distance_to_internal_low": distance_to_internal_low,
        "distance_to_internal_low_atr": distance_to_internal_low_atr,
        "distance_to_major_high": distance_to_major_high,
        "distance_to_major_high_atr": distance_to_major_high_atr,
        "distance_to_major_low": distance_to_major_low,
        "distance_to_major_low_atr": distance_to_major_low_atr,
        # --- Tier 2: prediction structure distances ---
        "distance_to_prediction_swing": distance_to_prediction_swing,
        "distance_to_prediction_swing_atr": distance_to_prediction_swing_atr,
        "distance_to_inducement": distance_to_inducement,
        "distance_to_inducement_atr": distance_to_inducement_atr,
        "distance_to_continuation": distance_to_continuation,
        "distance_to_continuation_atr": distance_to_continuation_atr,
        "distance_to_invalidation": distance_to_invalidation,
        "distance_to_invalidation_atr": distance_to_invalidation_atr,
        # --- Tier 2: R-multiples ---
        "rr_to_target": rr_to_target,
        "rr_to_continuation": rr_to_continuation,
        # --- Tier 2: confluence & alignment ---
        "confluence_count": confluence_count,
        "confluence_score": confluence_score,
        "signal_alignment_score": signal_alignment_score,
    }


def record_signal_snapshots(*, normalized_events: list[dict[str, Any]]) -> dict[str, Any]:
    journal_file = _journal_path()
    existing_rows = _load_jsonl(journal_file)
    existing_ids = {str(row.get("signal_id") or "") for row in existing_rows}

    written_count = 0
    duplicate_count = 0
    signal_ids: list[str] = []

    for event in normalized_events:
        snapshot = _build_signal_snapshot(event)
        if snapshot is None:
            continue

        signal_id = snapshot["signal_id"]
        if signal_id in existing_ids:
            duplicate_count += 1
            continue

        _append_jsonl(journal_file, snapshot)
        existing_ids.add(signal_id)
        signal_ids.append(signal_id)
        written_count += 1

    return {
        "written_count": written_count,
        "duplicate_count": duplicate_count,
        "signal_ids": signal_ids,
    }


def _load_normalized_event_file(path: Path) -> Optional[dict[str, Any]]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _load_stored_normalized_events() -> list[dict[str, Any]]:
    ensure_ingest_directories()
    ingest_paths = get_ingest_paths()
    rows: list[dict[str, Any]] = []

    for event_file in ingest_paths.normalized_signal_events.glob("*.json"):
        event_row = _load_normalized_event_file(event_file)
        if event_row is None:
            continue

        micro_row = _load_normalized_event_file(ingest_paths.normalized_micro_context / event_file.name) or {}
        macro_row = _load_normalized_event_file(ingest_paths.normalized_macro_context / event_file.name) or {}

        hydrated_event = dict(event_row)
        hydrated_event["micro_context"] = _as_dict(micro_row.get("micro"))
        hydrated_event["macro_context"] = _as_dict(macro_row.get("macro"))
        rows.append(hydrated_event)

    rows.sort(key=lambda row: int(row.get("event_time") or 0))
    return rows


def backfill_signal_snapshots_from_storage(*, event_ids: Optional[list[str]] = None) -> dict[str, Any]:
    journal_file = _journal_path()
    existing_rows = _load_jsonl(journal_file)
    existing_ids = {str(row.get("signal_id") or "") for row in existing_rows}
    filtered_event_ids = {str(value).strip() for value in (event_ids or []) if str(value).strip()}
    apply_filter = bool(filtered_event_ids)

    written_count = 0
    duplicate_count = 0
    scanned_count = 0
    signal_ids: list[str] = []

    for event in _load_stored_normalized_events():
        event_id = str(event.get("event_id") or "").strip()
        if apply_filter and event_id not in filtered_event_ids:
            continue

        scanned_count += 1
        snapshot = _build_signal_snapshot(event)
        if snapshot is None:
            continue

        signal_id = snapshot["signal_id"]
        if signal_id in existing_ids:
            duplicate_count += 1
            continue

        _append_jsonl(journal_file, snapshot)
        existing_ids.add(signal_id)
        signal_ids.append(signal_id)
        written_count += 1

    return {
        "scanned_count": scanned_count,
        "written_count": written_count,
        "duplicate_count": duplicate_count,
        "signal_ids": signal_ids,
    }


def _horizon_close(future_rows: list[dict[str, Any]], bars: int) -> Optional[float]:
    if len(future_rows) < bars:
        return None
    return _safe_float(future_rows[bars - 1].get("close"))


def _max_high(rows: list[dict[str, Any]]) -> Optional[float]:
    values = [_safe_float(row.get("high")) for row in rows]
    parsed = [value for value in values if value is not None]
    return max(parsed) if parsed else None


def _min_low(rows: list[dict[str, Any]]) -> Optional[float]:
    values = [_safe_float(row.get("low")) for row in rows]
    parsed = [value for value in values if value is not None]
    return min(parsed) if parsed else None


def _compute_mfe_mae(
    *,
    side: str,
    entry_price: Optional[float],
    max_high: Optional[float],
    min_low: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if entry_price is None or not _is_non_zero(entry_price) or max_high is None or min_low is None:
        return None, None, None, None

    if side == "short":
        mfe = entry_price - min_low
        mae = max_high - entry_price
    else:
        mfe = max_high - entry_price
        mae = entry_price - min_low

    return mfe, mae, (mfe / entry_price) * 100.0, (mae / entry_price) * 100.0


def _compute_reversion_hit(
    *,
    reversion_window: list[dict[str, Any]],
    fast_ema: Optional[float],
    entry_price: Optional[float],
) -> Optional[bool]:
    if len(reversion_window) < 5 or fast_ema is None:
        return None

    found_bar_price = False
    for row in reversion_window:
        low_value = _safe_float(row.get("low"))
        high_value = _safe_float(row.get("high"))
        if low_value is None or high_value is None:
            continue
        found_bar_price = True
        if low_value <= fast_ema <= high_value:
            return True

    if found_bar_price:
        return False

    close_values = [_safe_float(row.get("close")) for row in reversion_window]
    parsed_closes = [value for value in close_values if value is not None]
    if not parsed_closes:
        return None

    tolerance = max(abs(entry_price or 0.0) * 0.0005, 1e-9)
    return any(abs(close_value - fast_ema) <= tolerance for close_value in parsed_closes)


def _compute_continuation_hit(
    *,
    reversion_window: list[dict[str, Any]],
    side: str,
    entry_price: Optional[float],
    continuation_threshold_pct: float,
) -> Optional[bool]:
    if len(reversion_window) < 5 or entry_price is None or not _is_non_zero(entry_price):
        return None

    threshold = abs(entry_price) * continuation_threshold_pct
    min_low_5 = _min_low(reversion_window)
    max_high_5 = _max_high(reversion_window)

    if side == "short" and min_low_5 is not None:
        return min_low_5 <= (entry_price - threshold)
    if side == "long" and max_high_5 is not None:
        return max_high_5 >= (entry_price + threshold)
    return None


def _compute_strengths(
    *,
    side: str,
    close_5: Optional[float],
    entry_price: Optional[float],
    fast_ema: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    if close_5 is None or entry_price is None or not _is_non_zero(entry_price):
        return None, None, None

    direction = -1.0 if side == "short" else 1.0
    signed_move_pct = direction * ((close_5 - entry_price) / entry_price) * 100.0
    continuation_strength = max(0.0, signed_move_pct)
    reversion_strength = max(0.0, -signed_move_pct)

    if fast_ema is not None:
        start_dist = abs(entry_price - fast_ema)
        end_dist = abs(close_5 - fast_ema)
        if start_dist > 0:
            ema_pullback_strength = max(0.0, ((start_dist - end_dist) / start_dist) * 100.0)
            reversion_strength = max(reversion_strength, ema_pullback_strength)

    return signed_move_pct, continuation_strength, reversion_strength


def _did_price_reach_level(
    rows: list[dict[str, Any]],
    level: Optional[float],
    direction: str,
) -> Optional[bool]:
    """Check if price reached a level. direction='above' or 'below'."""
    if level is None or not rows:
        return None
    for row in rows:
        h = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        if h is None or low is None:
            continue
        if direction == "above" and h >= level:
            return True
        if direction == "below" and low <= level:
            return True
    return False


def _extreme_excursion(
    high: float, low: float, side: str, entry_price: float, which: str,
) -> float:
    if which == "mfe":
        return (high - entry_price) if side == "long" else (entry_price - low)
    return (entry_price - low) if side == "long" else (high - entry_price)


def _bars_to_extreme(
    rows: list[dict[str, Any]],
    side: str,
    entry_price: Optional[float],
    which: str,
) -> Optional[int]:
    """Return bar index (1-based) at which MFE or MAE was reached."""
    if entry_price is None or not rows:
        return None
    best_val: Optional[float] = None
    best_idx: Optional[int] = None
    for i, row in enumerate(rows):
        h = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        if h is None or low is None:
            continue
        val = _extreme_excursion(h, low, side, entry_price, which)
        if best_val is None or val > best_val:
            best_val = val
            best_idx = i + 1
    return best_idx


def _signed_return_pct(
    side: str,
    entry_price: Optional[float],
    close_n: Optional[float],
) -> Optional[float]:
    if entry_price is None or close_n is None or abs(entry_price) <= _EPSILON:
        return None
    direction = -1.0 if side == "short" else 1.0
    return direction * ((close_n - entry_price) / entry_price) * 100.0


def _classify_signal_quality(
    mfe: Optional[float],
    mae: Optional[float],
) -> Optional[str]:
    """good / neutral / bad based on MFE/MAE ratio."""
    if mfe is None or mae is None:
        return None
    if mae <= _EPSILON:
        return "good"
    ratio = mfe / mae
    if ratio >= 2.0:
        return "good"
    if ratio >= 0.8:
        return "neutral"
    return "bad"


def _classify_entry_efficiency(bars_to_mfe: Optional[int], max_horizon: int) -> Optional[str]:
    """early / timely / late based on bars_to_mfe relative to horizon."""
    if bars_to_mfe is None or max_horizon <= 0:
        return None
    pct = bars_to_mfe / max_horizon
    if pct <= 0.25:
        return "early"
    if pct <= 0.60:
        return "timely"
    return "late"


def _classify_structure_truth(target_hit: Optional[bool], invalidation_hit: Optional[bool]) -> Optional[str]:
    """confirmed / failed / ambiguous."""
    if target_hit is None and invalidation_hit is None:
        return None
    if target_hit and not invalidation_hit:
        return "confirmed"
    if invalidation_hit and not target_hit:
        return "failed"
    return "ambiguous"


def _classify_regime_success(
    signal_alignment_score: Optional[float],
    signed_move_pct: Optional[float],
) -> Optional[str]:
    """strong / weak / contra based on alignment + outcome."""
    if signal_alignment_score is None or signed_move_pct is None:
        return None
    aligned = signal_alignment_score >= 0.5
    profitable = signed_move_pct > 0
    if aligned and profitable:
        return "strong"
    if not aligned and not profitable:
        return "contra"
    return "weak"


def _compute_outcome_row(
    *,
    signal_row: dict[str, Any],
    future_rows: list[dict[str, Any]],
    horizon_bars: tuple[int, ...],
    continuation_threshold_pct: float,
) -> dict[str, Any]:
    side = str(signal_row.get("side") or "long")
    entry_price = _safe_float(signal_row.get("price"))
    fast_ema = _safe_float(signal_row.get("fast_ema"))

    horizon_closes: dict[int, Optional[float]] = {
        bars: _horizon_close(future_rows, bars) for bars in horizon_bars
    }

    max_horizon = max(horizon_bars)
    evaluation_window = future_rows[:max_horizon]
    reversion_window = future_rows[:5]
    max_high = _max_high(evaluation_window)
    min_low = _min_low(evaluation_window)

    mfe, mae, mfe_pct, mae_pct = _compute_mfe_mae(
        side=side,
        entry_price=entry_price,
        max_high=max_high,
        min_low=min_low,
    )

    reversion_hit_5bar = _compute_reversion_hit(
        reversion_window=reversion_window,
        fast_ema=fast_ema,
        entry_price=entry_price,
    )
    continuation_hit_5bar = _compute_continuation_hit(
        reversion_window=reversion_window,
        side=side,
        entry_price=entry_price,
        continuation_threshold_pct=continuation_threshold_pct,
    )

    close_5 = horizon_closes.get(5)
    signed_move_pct, continuation_strength, reversion_strength = _compute_strengths(
        side=side,
        close_5=close_5,
        entry_price=entry_price,
        fast_ema=fast_ema,
    )

    # --- Tier C: ATR-normalised excursion ---
    atr = _safe_float(signal_row.get("atr"))
    mfe_atr = _atr_normalise(mfe, atr)
    mae_atr = _atr_normalise(mae, atr)

    # --- Tier C: bars to MFE / MAE ---
    bars_to_mfe = _bars_to_extreme(evaluation_window, side, entry_price, "mfe")
    bars_to_mae = _bars_to_extreme(evaluation_window, side, entry_price, "mae")

    # --- Tier C: prediction structure hits ---
    pred_swing = _safe_float(signal_row.get("prediction_swing_level"))
    inval_level = _safe_float(signal_row.get("invalidation_level"))
    cont_level = _safe_float(signal_row.get("continuation_level"))
    induce_level = _safe_float(signal_row.get("inducement_level"))
    displ_origin = _safe_float(signal_row.get("displacement_origin"))

    if side == "long":
        target_hit = _did_price_reach_level(evaluation_window, pred_swing, "above")
        invalidation_hit = _did_price_reach_level(evaluation_window, inval_level, "below")
        continuation_hit_pred = _did_price_reach_level(evaluation_window, cont_level, "above")
        inducement_swept = _did_price_reach_level(evaluation_window, induce_level, "below")
        displacement_origin_hit = _did_price_reach_level(evaluation_window, displ_origin, "below")
    else:
        target_hit = _did_price_reach_level(evaluation_window, pred_swing, "below")
        invalidation_hit = _did_price_reach_level(evaluation_window, inval_level, "above")
        continuation_hit_pred = _did_price_reach_level(evaluation_window, cont_level, "below")
        inducement_swept = _did_price_reach_level(evaluation_window, induce_level, "above")
        displacement_origin_hit = _did_price_reach_level(evaluation_window, displ_origin, "above")

    # --- Tier C: signed return windows ---
    return_1bar_pct = _signed_return_pct(side, entry_price, horizon_closes.get(1))
    return_3bar_pct = _signed_return_pct(side, entry_price, horizon_closes.get(3))
    return_5bar_pct = _signed_return_pct(side, entry_price, horizon_closes.get(5))
    return_10bar_pct = _signed_return_pct(side, entry_price, horizon_closes.get(10))
    # 20-bar return only if we have enough bars
    close_20 = _horizon_close(future_rows, 20)
    return_20bar_pct = _signed_return_pct(side, entry_price, close_20)

    # --- Tier C: quality classification labels ---
    signal_quality_label = _classify_signal_quality(mfe, mae)
    entry_efficiency_label = _classify_entry_efficiency(bars_to_mfe, max_horizon)
    structure_truth_label = _classify_structure_truth(target_hit, invalidation_hit)
    signal_alignment = _safe_float(signal_row.get("signal_alignment_score"))
    regime_success_label = _classify_regime_success(signal_alignment, signed_move_pct)

    return {
        "signal_id": str(signal_row.get("signal_id") or ""),
        "evaluated_at": _utc_now_iso(),
        "symbol": str(signal_row.get("symbol") or ""),
        "timeframe": str(signal_row.get("timeframe") or ""),
        "side": side,
        "signal_name": str(signal_row.get("signal_name") or ""),
        "entry_price": entry_price,
        "entry_time_ms": _safe_int(signal_row.get("event_time_ms")),
        "bars_available": len(future_rows),
        # horizon closes
        "close_1bar": horizon_closes.get(1),
        "close_3bar": horizon_closes.get(3),
        "close_5bar": horizon_closes.get(5),
        "close_10bar": horizon_closes.get(10),
        # excursion
        "mfe": mfe,
        "mae": mae,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "mfe_atr": mfe_atr,
        "mae_atr": mae_atr,
        "bars_to_mfe": bars_to_mfe,
        "bars_to_mae": bars_to_mae,
        # existing outcome hits
        "reversion_hit_5bar": reversion_hit_5bar,
        "continuation_hit_5bar": continuation_hit_5bar,
        "reversion_strength": reversion_strength,
        "continuation_strength": continuation_strength,
        "signed_move_pct_5bar": signed_move_pct,
        # prediction structure hits
        "target_hit": target_hit,
        "invalidation_hit": invalidation_hit,
        "continuation_hit": continuation_hit_pred,
        "inducement_swept": inducement_swept,
        "displacement_origin_hit": displacement_origin_hit,
        # forward returns
        "return_1bar_pct": return_1bar_pct,
        "return_3bar_pct": return_3bar_pct,
        "return_5bar_pct": return_5bar_pct,
        "return_10bar_pct": return_10bar_pct,
        "return_20bar_pct": return_20bar_pct,
        # quality labels
        "signal_quality_label": signal_quality_label,
        "entry_efficiency_label": entry_efficiency_label,
        "structure_truth_label": structure_truth_label,
        "regime_success_label": regime_success_label,
        "status": "evaluated",
    }


def _group_journal_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row.get("symbol") or ""), str(row.get("timeframe") or ""))
        grouped.setdefault(key, []).append(row)

    for group_rows in grouped.values():
        group_rows.sort(key=lambda row: int(row.get("event_time_ms") or 0))
    return grouped


def _evaluate_group_rows(
    *,
    rows: list[dict[str, Any]],
    existing_ids: set[str],
    min_future_bars: int,
    continuation_threshold_pct: float,
    horizon_bars: tuple[int, ...],
    outcome_file: Path,
) -> tuple[int, int]:
    evaluated_new_count = 0
    pending_count = 0

    for index, row in enumerate(rows):
        signal_id = str(row.get("signal_id") or "")
        if not signal_id or signal_id in existing_ids:
            continue

        future_rows = rows[index + 1 :]
        if len(future_rows) < min_future_bars:
            pending_count += 1
            continue

        outcome_row = _compute_outcome_row(
            signal_row=row,
            future_rows=future_rows,
            horizon_bars=horizon_bars,
            continuation_threshold_pct=continuation_threshold_pct,
        )
        _append_jsonl(outcome_file, outcome_row)
        existing_ids.add(signal_id)
        evaluated_new_count += 1

    return evaluated_new_count, pending_count


def run_signal_outcome_evaluation_once(
    *,
    min_future_bars: int = DEFAULT_MIN_FUTURE_BARS,
    continuation_threshold_pct: float = DEFAULT_CONTINUATION_THRESHOLD_PCT,
    horizon_bars: tuple[int, ...] = DEFAULT_HORIZON_BARS,
) -> dict[str, Any]:
    if min_future_bars < 1:
        raise ValueError("min_future_bars must be >= 1")

    journal_rows = load_signal_journal_rows()
    existing_outcomes = load_signal_outcome_rows()
    existing_ids = {str(row.get("signal_id") or "") for row in existing_outcomes}

    grouped = _group_journal_rows(journal_rows)

    outcome_file = _outcome_path()
    evaluated_new_count = 0
    pending_count = 0

    for rows in grouped.values():
        group_evaluated, group_pending = _evaluate_group_rows(
            rows=rows,
            existing_ids=existing_ids,
            min_future_bars=min_future_bars,
            continuation_threshold_pct=continuation_threshold_pct,
            horizon_bars=horizon_bars,
            outcome_file=outcome_file,
        )
        evaluated_new_count += group_evaluated
        pending_count += group_pending

    return {
        "evaluated_new_count": evaluated_new_count,
        "pending_count": pending_count,
        "total_journal_rows": len(journal_rows),
        "total_outcome_rows": len(existing_ids),
    }


def _apply_common_filters(
    rows: list[dict[str, Any]],
    *,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    signal_name: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> list[dict[str, Any]]:
    filtered = rows

    if symbol is not None:
        filtered = [row for row in filtered if str(row.get("symbol") or "") == symbol]

    if side is not None:
        normalized_side = _normalize_side(side)
        filtered = [row for row in filtered if str(row.get("side") or "") == normalized_side]

    if signal_name is not None:
        filtered = [row for row in filtered if str(row.get("signal_name") or "") == signal_name]

    if timeframe is not None:
        filtered = [row for row in filtered if str(row.get("timeframe") or "") == timeframe]

    return filtered


def get_recent_signal_journal_rows(
    limit: int,
    *,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    signal_name: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    rows = load_signal_journal_rows()
    filtered = _apply_common_filters(
        rows,
        symbol=symbol,
        side=side,
        signal_name=signal_name,
        timeframe=timeframe,
    )
    filtered.sort(key=lambda row: int(row.get("event_time_ms") or 0), reverse=True)
    return filtered[:safe_limit]


def get_recent_signal_outcome_rows(
    limit: int,
    *,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    signal_name: Optional[str] = None,
    timeframe: Optional[str] = None,
    status: Optional[str] = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    rows = load_signal_outcome_rows()
    filtered = _apply_common_filters(
        rows,
        symbol=symbol,
        side=side,
        signal_name=signal_name,
        timeframe=timeframe,
    )

    if status is not None:
        filtered = [row for row in filtered if str(row.get("status") or "") == status]

    filtered.sort(key=lambda row: str(row.get("evaluated_at") or ""), reverse=True)
    return filtered[:safe_limit]


def get_outcome_engine_defaults() -> dict[str, Any]:
    min_future_bars = _safe_int(os.getenv("SIGNAL_OUTCOME_MIN_FUTURE_BARS")) or DEFAULT_MIN_FUTURE_BARS
    continuation_threshold_pct = _safe_float(os.getenv("SIGNAL_OUTCOME_CONTINUATION_THRESHOLD_PCT"))
    if continuation_threshold_pct is None:
        continuation_threshold_pct = DEFAULT_CONTINUATION_THRESHOLD_PCT

    return {
        "min_future_bars": max(1, min_future_bars),
        "continuation_threshold_pct": max(0.0, continuation_threshold_pct),
        "horizon_bars": DEFAULT_HORIZON_BARS,
    }
