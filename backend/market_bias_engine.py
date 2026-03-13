from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Optional

from backend.signal_outcome_engine import (
    build_signal_snapshot_from_normalized_signal,
    load_signal_journal_rows,
    load_signal_outcome_rows,
)
from backend.tradingview_ingest_storage import ensure_ingest_directories, get_ingest_paths

_MARKET_BIAS_FILE = "market_bias_scores.jsonl"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_side(value: Any) -> str:
    cleaned = str(value or "").strip().lower()
    if cleaned in {"buy", "long"}:
        return "long"
    if cleaned in {"sell", "short"}:
        return "short"
    return "long"


def _bias_path() -> Path:
    ensure_ingest_directories()
    return get_ingest_paths().state_policy_scores / _MARKET_BIAS_FILE


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


def _sigmoid(value: float) -> float:
    bounded = max(-20.0, min(20.0, value))
    return 1.0 / (1.0 + math.exp(-bounded))


def _volume_medians(journal_rows: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    bucket: dict[tuple[str, str], list[float]] = {}

    for row in journal_rows:
        volume = _safe_float(row.get("volume"))
        if volume is None or volume <= 0:
            continue

        key = (str(row.get("symbol") or ""), str(row.get("timeframe") or ""))
        bucket.setdefault(key, []).append(volume)

    return {
        key: float(median(values))
        for key, values in bucket.items()
        if values
    }


def _rsi_bucket(rsi: Optional[float]) -> str:
    if rsi is None:
        return "unknown"
    if rsi < 40:
        return "lt40"
    if rsi < 45:
        return "40_45"
    if rsi <= 55:
        return "45_55"
    if rsi <= 60:
        return "55_60"
    return "gt60"


def _ema_spread_bucket(ema_spread_pct: Optional[float]) -> str:
    if ema_spread_pct is None:
        return "unknown"

    absolute_pct = abs(ema_spread_pct)
    if absolute_pct < 0.05:
        return "tiny"
    if absolute_pct < 0.15:
        return "small"
    if absolute_pct < 0.30:
        return "medium"
    return "large"


def _volume_bucket(volume: Optional[float], median_volume: Optional[float]) -> str:
    if volume is None or median_volume in {None, 0.0}:
        return "unknown"

    ratio = volume / median_volume
    if ratio < 0.75:
        return "low"
    if ratio > 1.25:
        return "high"
    return "normal"


def _hour_bucket(event_time_ms: Any) -> str:
    try:
        parsed = int(event_time_ms)
    except (TypeError, ValueError):
        return "unknown"

    if parsed <= 0:
        return "unknown"

    hour = datetime.fromtimestamp(parsed / 1000.0, tz=timezone.utc).hour
    return f"h{hour:02d}"


def _bucket_key(snapshot: dict[str, Any], medians: dict[tuple[str, str], float]) -> str:
    symbol = str(snapshot.get("symbol") or "")
    timeframe = str(snapshot.get("timeframe") or "")
    signal_name = str(snapshot.get("signal_name") or "unknown_signal")
    side = _normalize_side(snapshot.get("side"))

    median_volume = medians.get((symbol, timeframe))
    volume = _safe_float(snapshot.get("volume"))

    return "|".join(
        (
            signal_name,
            side,
            f"tf:{timeframe}",
            f"rsi:{_rsi_bucket(_safe_float(snapshot.get('rsi')))}",
            f"ema:{_ema_spread_bucket(_safe_float(snapshot.get('ema_spread_pct')))}",
            f"vol:{_volume_bucket(volume, median_volume)}",
            f"hour:{_hour_bucket(snapshot.get('event_time_ms'))}",
        )
    )


def _score_rsi_reversion(side: str, rsi: Optional[float]) -> tuple[float, list[str]]:
    if rsi is None:
        return 0.0, []

    if side == "short" and rsi <= 45:
        return 0.20, ["RSI below neutral for short setup"]
    if side == "long" and rsi >= 55:
        return 0.20, ["RSI above neutral for long setup"]
    if side == "short" and rsi >= 55:
        return -0.10, []
    if side == "long" and rsi <= 45:
        return -0.10, []
    return 0.0, []


def _ensure_bucket_row(raw_bucket: dict[str, dict[str, Any]], bucket_key: str) -> dict[str, Any]:
    return raw_bucket.setdefault(
        bucket_key,
        {
            "count": 0,
            "reversion_hits": 0,
            "continuation_hits": 0,
            "sum_mfe_pct": 0.0,
            "sum_mae_pct": 0.0,
            "sum_move_3bar_pct": 0.0,
            "move_3bar_count": 0,
        },
    )


def _accumulate_mfe_mae(bucket_row: dict[str, Any], outcome: dict[str, Any]) -> None:
    mfe_pct = _safe_float(outcome.get("mfe_pct"))
    mae_pct = _safe_float(outcome.get("mae_pct"))
    if mfe_pct is not None:
        bucket_row["sum_mfe_pct"] += mfe_pct
    if mae_pct is not None:
        bucket_row["sum_mae_pct"] += mae_pct


def _accumulate_move_3bar(bucket_row: dict[str, Any], snapshot: dict[str, Any], outcome: dict[str, Any]) -> None:
    close_3bar = _safe_float(outcome.get("close_3bar"))
    entry_price = _safe_float(outcome.get("entry_price"))
    if close_3bar is None or entry_price in {None, 0.0}:
        return

    side = _normalize_side(snapshot.get("side"))
    direction = -1.0 if side == "short" else 1.0
    move_3bar_pct = direction * ((close_3bar - entry_price) / entry_price) * 100.0
    bucket_row["sum_move_3bar_pct"] += move_3bar_pct
    bucket_row["move_3bar_count"] += 1


def _iter_eligible_outcome_pairs(
    *,
    journal_map: dict[str, dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for outcome in outcome_rows:
        signal_id = str(outcome.get("signal_id") or "")
        if not signal_id:
            continue

        snapshot = journal_map.get(signal_id)
        if snapshot is None:
            continue

        reversion_hit = outcome.get("reversion_hit_5bar")
        continuation_hit = outcome.get("continuation_hit_5bar")
        if reversion_hit is None or continuation_hit is None:
            continue

        pairs.append((snapshot, outcome))
    return pairs


def _build_bucket_stats(
    journal_rows: list[dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
    medians: dict[tuple[str, str], float],
) -> dict[str, dict[str, Any]]:
    journal_map = {str(row.get("signal_id") or ""): row for row in journal_rows}
    raw_bucket: dict[str, dict[str, Any]] = {}
    eligible_pairs = _iter_eligible_outcome_pairs(
        journal_map=journal_map,
        outcome_rows=outcome_rows,
    )

    for snapshot, outcome in eligible_pairs:
        reversion_hit = bool(outcome.get("reversion_hit_5bar"))
        continuation_hit = bool(outcome.get("continuation_hit_5bar"))

        bucket_key = _bucket_key(snapshot, medians)
        row = _ensure_bucket_row(raw_bucket, bucket_key)

        row["count"] += 1
        row["reversion_hits"] += 1 if reversion_hit else 0
        row["continuation_hits"] += 1 if continuation_hit else 0
        _accumulate_mfe_mae(row, outcome)
        _accumulate_move_3bar(row, snapshot, outcome)

    finalized: dict[str, dict[str, Any]] = {}
    for key, value in raw_bucket.items():
        count = int(value["count"])
        if count <= 0:
            continue

        move_count = int(value["move_3bar_count"])
        finalized[key] = {
            "count": count,
            "reversion_rate": value["reversion_hits"] / count,
            "continuation_rate": value["continuation_hits"] / count,
            "avg_mfe_pct": value["sum_mfe_pct"] / count,
            "avg_mae_pct": value["sum_mae_pct"] / count,
            "avg_move_3bar_pct": (value["sum_move_3bar_pct"] / move_count) if move_count > 0 else None,
        }

    return finalized


def _feature_score(snapshot: dict[str, Any]) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []

    side = _normalize_side(snapshot.get("side"))
    rsi = _safe_float(snapshot.get("rsi"))
    rsi_score, rsi_reasons = _score_rsi_reversion(side, rsi)
    score += rsi_score
    reasons.extend(rsi_reasons)

    ema_spread_pct = abs(_safe_float(snapshot.get("ema_spread_pct")) or 0.0)
    if ema_spread_pct >= 0.15:
        score += 0.15
        reasons.append("EMA spread is stretched")

    distance_from_slow_pct = abs(_safe_float(snapshot.get("distance_from_slow_pct")) or 0.0)
    if distance_from_slow_pct >= 0.10:
        score += 0.10
        reasons.append("Price is extended away from slow EMA")

    wick_ratio = _safe_float(snapshot.get("wick_ratio"))
    if wick_ratio is not None and wick_ratio >= 0.55:
        score += 0.10
        reasons.append("High wick ratio suggests rejection")

    return score, reasons


def _confidence_label(sample_count: int, reversion_bias: float) -> str:
    edge = abs(reversion_bias - 0.5)

    if sample_count >= 200 and edge >= 0.10:
        return "high"
    if sample_count >= 50 and edge >= 0.06:
        return "medium"
    if sample_count >= 20:
        return "low"
    return "very_low"


def compute_market_bias_for_signal(
    *,
    snapshot: dict[str, Any],
    bucket_stats: dict[str, dict[str, Any]],
    medians: dict[tuple[str, str], float],
) -> dict[str, Any]:
    bucket_key = _bucket_key(snapshot, medians)
    stats = bucket_stats.get(bucket_key)

    sample_count = int(stats.get("count") if stats else 0)
    reversion_rate = float(stats.get("reversion_rate") if stats else 0.5)
    continuation_rate = float(stats.get("continuation_rate") if stats else 0.5)

    historical_score = (reversion_rate - continuation_rate) * 2.0
    feature_score, feature_reasons = _feature_score(snapshot)
    score = historical_score + feature_score

    reversion_bias = _sigmoid(score)
    continuation_bias = 1.0 - reversion_bias

    reasons: list[str] = []
    if stats:
        reasons.append(
            f"Historical bucket reversion rate {reversion_rate:.1%} across {sample_count} signals"
        )
    reasons.extend(feature_reasons)

    confidence = _confidence_label(sample_count, reversion_bias)
    status_value = "insufficient_samples" if sample_count < 10 else "ready"

    return {
        "signal_id": str(snapshot.get("signal_id") or ""),
        "computed_at": _utc_now_iso(),
        "symbol": str(snapshot.get("symbol") or ""),
        "timeframe": str(snapshot.get("timeframe") or ""),
        "side": _normalize_side(snapshot.get("side")),
        "signal_name": str(snapshot.get("signal_name") or ""),
        "bucket_key": bucket_key,
        "sample_count": sample_count,
        "reversion_rate": reversion_rate,
        "continuation_rate": continuation_rate,
        "reversion_bias": reversion_bias,
        "continuation_bias": continuation_bias,
        "confidence": confidence,
        "status": status_value,
        "reasons": reasons,
        "avg_mfe_pct": stats.get("avg_mfe_pct") if stats else None,
        "avg_mae_pct": stats.get("avg_mae_pct") if stats else None,
        "avg_move_3bar_pct": stats.get("avg_move_3bar_pct") if stats else None,
    }


def compute_market_bias_preview_for_normalized_signal(
    *,
    normalized_signal: dict[str, Any],
) -> dict[str, Any]:
    snapshot = build_signal_snapshot_from_normalized_signal(normalized_signal)
    if snapshot is None:
        return {
            "signal_id": "",
            "computed_at": _utc_now_iso(),
            "symbol": str(normalized_signal.get("symbol") or ""),
            "timeframe": str(normalized_signal.get("timeframe") or ""),
            "side": _normalize_side(normalized_signal.get("side")),
            "signal_name": str(normalized_signal.get("signal_name") or ""),
            "bucket_key": "",
            "sample_count": 0,
            "reversion_rate": 0.5,
            "continuation_rate": 0.5,
            "reversion_bias": 0.5,
            "continuation_bias": 0.5,
            "confidence": "very_low",
            "status": "unavailable",
            "reasons": ["Signal snapshot could not be built from normalized payload"],
            "avg_mfe_pct": None,
            "avg_mae_pct": None,
            "avg_move_3bar_pct": None,
        }

    journal_rows = load_signal_journal_rows()
    outcome_rows = load_signal_outcome_rows()
    medians = _volume_medians(journal_rows)
    bucket_stats = _build_bucket_stats(journal_rows, outcome_rows, medians)
    return compute_market_bias_for_signal(
        snapshot=snapshot,
        bucket_stats=bucket_stats,
        medians=medians,
    )


def compute_and_store_signal_bias(*, signal_ids: Optional[list[str]] = None) -> dict[str, Any]:
    journal_rows = load_signal_journal_rows()
    outcome_rows = load_signal_outcome_rows()
    if not journal_rows:
        return {
            "computed_count": 0,
            "existing_count": 0,
            "missing_signal_count": 0,
            "eligible_signal_count": 0,
        }

    journal_map = {str(row.get("signal_id") or ""): row for row in journal_rows}

    if signal_ids is None:
        target_ids = list(journal_map.keys())
    else:
        target_ids = [str(signal_id) for signal_id in signal_ids]

    bias_file = _bias_path()
    existing_bias = _load_jsonl(bias_file)
    existing_signal_ids = {str(row.get("signal_id") or "") for row in existing_bias}

    medians = _volume_medians(journal_rows)
    stats = _build_bucket_stats(journal_rows, outcome_rows, medians)

    computed_count = 0
    existing_count = 0
    missing_signal_count = 0

    for signal_id in target_ids:
        if signal_id in existing_signal_ids:
            existing_count += 1
            continue

        snapshot = journal_map.get(signal_id)
        if snapshot is None:
            missing_signal_count += 1
            continue

        bias_row = compute_market_bias_for_signal(
            snapshot=snapshot,
            bucket_stats=stats,
            medians=medians,
        )
        _append_jsonl(bias_file, bias_row)
        existing_signal_ids.add(signal_id)
        computed_count += 1

    return {
        "computed_count": computed_count,
        "existing_count": existing_count,
        "missing_signal_count": missing_signal_count,
        "eligible_signal_count": len(target_ids),
    }


def get_recent_market_bias_rows(
    limit: int,
    *,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    signal_name: Optional[str] = None,
    confidence: Optional[str] = None,
    status: Optional[str] = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    rows = _load_jsonl(_bias_path())

    filtered = rows
    if symbol is not None:
        filtered = [row for row in filtered if str(row.get("symbol") or "") == symbol]

    if side is not None:
        normalized_side = _normalize_side(side)
        filtered = [row for row in filtered if str(row.get("side") or "") == normalized_side]

    if signal_name is not None:
        filtered = [row for row in filtered if str(row.get("signal_name") or "") == signal_name]

    if confidence is not None:
        filtered = [row for row in filtered if str(row.get("confidence") or "") == confidence]

    if status is not None:
        filtered = [row for row in filtered if str(row.get("status") or "") == status]

    filtered.sort(key=lambda row: str(row.get("computed_at") or ""), reverse=True)
    return filtered[:safe_limit]
