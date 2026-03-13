"""Release cohort scoring engine.

Computes performance metrics grouped by release_id, release_version,
release_channel, and research field presence.  Maintains rolling windows
(short / medium / long horizon) and assigns promotion_score, decay_score,
and confidence_score per cohort.

Also manages the feature lifecycle state machine:
  observe -> candidate -> promote -> decay -> retire
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.event_writer import get_connection
from backend.signal_outcome_engine import (
    load_signal_journal_rows,
    load_signal_outcome_rows,
)
from backend.tradingview_bridge_manifest import load_bridge_manifest

# ── Constants ────────────────────────────────────────────────────────

HORIZON_WINDOWS: Dict[str, int] = {
    "short": 60,     # ~1 hour of 1m bars
    "medium": 360,   # ~6 hours
    "long": 1440,    # ~24 hours
}

# Outcome classification thresholds (signed_move_pct_5bar)
WIN_THRESHOLD_PCT = 0.10
LOSS_THRESHOLD_PCT = -0.10

# Lifecycle transition thresholds
PROMOTION_MIN_SAMPLES = 40
DEMOTION_MIN_SAMPLES = 20
PROMOTE_QUALITY_THRESHOLD = 0.15
DECAY_QUALITY_THRESHOLD = -0.05
RETIRE_QUALITY_THRESHOLD = -0.20
CANDIDATE_CORRELATION_THRESHOLD = 0.10

VALID_LIFECYCLE_STATES = ("observe", "candidate", "promote", "decay", "retire")

_EPSILON = 1e-12

_HISTORY_FILE = "cohort_score_history.jsonl"


def _history_path() -> Path:
    from backend.tradingview_ingest_storage import ensure_ingest_directories, get_ingest_paths
    ensure_ingest_directories()
    return get_ingest_paths().state_outcomes / _HISTORY_FILE


def _append_history_row(row: Dict[str, Any]) -> None:
    path = _history_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False))
        f.write("\n")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


# ── Cohort Key Construction ──────────────────────────────────────────

def _build_cohort_key(
    *,
    release_id: Optional[str],
    release_version: Optional[str],
    release_channel: Optional[str],
    strategy_id: Optional[str],
    symbol: Optional[str],
    side: Optional[str],
) -> str:
    parts = [
        release_id or "_",
        release_version or "_",
        release_channel or "_",
        strategy_id or "_",
        symbol or "_",
        side or "_",
    ]
    return "|".join(parts)


# ── Outcome Classification ───────────────────────────────────────────

def _classify_outcome(outcome: Dict[str, Any]) -> str:
    signed_move = _safe_float(outcome.get("signed_move_pct_5bar"))
    if signed_move is None:
        return "scratch"
    if signed_move >= WIN_THRESHOLD_PCT:
        return "win"
    if signed_move <= LOSS_THRESHOLD_PCT:
        return "loss"
    return "scratch"


# ── Signal-Outcome Joining ───────────────────────────────────────────

def _join_journal_outcomes(
    journal_rows: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Join signal journal rows with their evaluated outcomes."""
    outcome_by_id: Dict[str, Dict[str, Any]] = {}
    for outcome in outcome_rows:
        sid = str(outcome.get("signal_id") or "")
        if sid:
            outcome_by_id[sid] = outcome

    joined: List[Dict[str, Any]] = []
    for journal_row in journal_rows:
        sid = str(journal_row.get("signal_id") or "")
        outcome = outcome_by_id.get(sid)
        if outcome is None:
            continue
        merged = dict(journal_row)
        merged["outcome"] = outcome
        merged["outcome_label"] = _classify_outcome(outcome)
        joined.append(merged)

    return joined


# ── Windowed Filtering ───────────────────────────────────────────────

def _filter_by_window(
    rows: List[Dict[str, Any]],
    window_bars: int,
) -> List[Dict[str, Any]]:
    """Keep only the most recent `window_bars` rows by event_time_ms."""
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda r: int(r.get("event_time_ms") or 0), reverse=True)
    return sorted_rows[:window_bars]


# ── Cohort Grouping ──────────────────────────────────────────────────

def _group_by_cohort(
    rows: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group joined rows by cohort key (release_id|version|channel|strategy|symbol|side)."""
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = _build_cohort_key(
            release_id=row.get("release_id"),
            release_version=row.get("release_version"),
            release_channel=row.get("release_channel"),
            strategy_id=row.get("strategy_id"),
            symbol=row.get("symbol"),
            side=row.get("side"),
        )
        groups[key].append(row)
    return dict(groups)


# ── Cohort Metric Computation ────────────────────────────────────────

def _safe_mean(values: List[Optional[float]]) -> Optional[float]:
    parsed = [v for v in values if v is not None]
    if not parsed:
        return None
    return sum(parsed) / len(parsed)


def _safe_rate(count: int, total: int) -> Optional[float]:
    if total <= 0:
        return None
    return count / total


def _compute_cohort_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute aggregate outcome metrics for a cohort group."""
    sample_count = len(rows)
    win_count = sum(1 for r in rows if r.get("outcome_label") == "win")
    loss_count = sum(1 for r in rows if r.get("outcome_label") == "loss")
    scratch_count = sample_count - win_count - loss_count

    outcomes = [r.get("outcome", {}) for r in rows]
    pnl_values = [_safe_float(o.get("signed_move_pct_5bar")) for o in outcomes]
    mfe_values = [_safe_float(o.get("mfe_pct")) for o in outcomes]
    mae_values = [_safe_float(o.get("mae_pct")) for o in outcomes]
    cont_values = [_safe_float(o.get("continuation_strength")) for o in outcomes]
    rev_values = [_safe_float(o.get("reversion_strength")) for o in outcomes]

    cont_hits = [o.get("continuation_hit_5bar") for o in outcomes]
    rev_hits = [o.get("reversion_hit_5bar") for o in outcomes]
    cont_hit_count = sum(1 for v in cont_hits if v is True)
    rev_hit_count = sum(1 for v in rev_hits if v is True)
    cont_total = sum(1 for v in cont_hits if v is not None)
    rev_total = sum(1 for v in rev_hits if v is not None)

    win_rate = _safe_rate(win_count, sample_count)
    avg_pnl = _safe_mean(pnl_values)

    return {
        "sample_count": sample_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "scratch_count": scratch_count,
        "win_rate": win_rate,
        "avg_pnl_pct": avg_pnl,
        "avg_mfe_pct": _safe_mean(mfe_values),
        "avg_mae_pct": _safe_mean(mae_values),
        "avg_continuation_strength": _safe_mean(cont_values),
        "avg_reversion_strength": _safe_mean(rev_values),
        "continuation_hit_rate": _safe_rate(cont_hit_count, cont_total),
        "reversion_hit_rate": _safe_rate(rev_hit_count, rev_total),
    }


# ── Score Assignment ─────────────────────────────────────────────────

def _compute_quality_score(metrics: Dict[str, Any]) -> float:
    """quality_score = avg_pnl_pct * win_rate, bounded."""
    avg_pnl = metrics.get("avg_pnl_pct")
    win_rate = metrics.get("win_rate")
    if avg_pnl is None or win_rate is None:
        return 0.0
    return avg_pnl * win_rate


def _compute_promotion_score(metrics: Dict[str, Any]) -> float:
    """Higher when cohort wins often and continuation is strong."""
    win_rate = metrics.get("win_rate") or 0.0
    cont_strength = metrics.get("avg_continuation_strength") or 0.0
    cont_hit = metrics.get("continuation_hit_rate") or 0.0
    sample_count = metrics.get("sample_count") or 0

    # Confidence discount for small samples
    confidence_factor = min(1.0, sample_count / PROMOTION_MIN_SAMPLES)
    raw = (win_rate * 0.4) + (cont_hit * 0.3) + (min(cont_strength, 1.0) * 0.3)
    return raw * confidence_factor


def _compute_decay_score(metrics: Dict[str, Any]) -> float:
    """Higher when cohort loses often or reverts strongly."""
    win_rate = metrics.get("win_rate") or 0.0
    rev_strength = metrics.get("avg_reversion_strength") or 0.0
    mae = abs(metrics.get("avg_mae_pct") or 0.0)
    sample_count = metrics.get("sample_count") or 0

    confidence_factor = min(1.0, sample_count / DEMOTION_MIN_SAMPLES)
    loss_pressure = max(0.0, 1.0 - win_rate)
    raw = (loss_pressure * 0.4) + (min(rev_strength, 1.0) * 0.3) + (min(mae / 1.0, 1.0) * 0.3)
    return raw * confidence_factor


def _compute_confidence_score(metrics: Dict[str, Any]) -> float:
    """Sample-weighted confidence in the score's reliability."""
    sample_count = metrics.get("sample_count") or 0
    return min(1.0, sample_count / PROMOTION_MIN_SAMPLES)


# ── Persistence ──────────────────────────────────────────────────────

@dataclass
class CohortScoreParams:
    """Bundles all cohort identity and computed score fields for persistence."""

    cohort_key: str
    release_id: Optional[str]
    release_version: Optional[str]
    release_channel: Optional[str]
    strategy_id: Optional[str]
    symbol: Optional[str]
    side: Optional[str]
    horizon: str
    window_bars: int
    promotion_score: float
    decay_score: float
    confidence_score: float
    quality_score: float
    scored_at: str


def _upsert_cohort_score(
    conn: sqlite3.Connection,
    params: CohortScoreParams,
    metrics: Dict[str, Any],
) -> None:
    existing = conn.execute(
        "SELECT id FROM release_cohort_scores WHERE cohort_key = ? AND horizon = ?",
        (params.cohort_key, params.horizon),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE release_cohort_scores SET
                release_id = ?, release_version = ?, release_channel = ?,
                strategy_id = ?, symbol = ?, side = ?,
                window_bars = ?,
                sample_count = ?, win_count = ?, loss_count = ?, scratch_count = ?,
                win_rate = ?, avg_pnl_pct = ?, avg_mfe_pct = ?, avg_mae_pct = ?,
                avg_continuation_strength = ?, avg_reversion_strength = ?,
                continuation_hit_rate = ?, reversion_hit_rate = ?,
                promotion_score = ?, decay_score = ?, confidence_score = ?, quality_score = ?,
                scored_at = ?
            WHERE cohort_key = ? AND horizon = ?
            """,
            (
                params.release_id, params.release_version, params.release_channel,
                params.strategy_id, params.symbol, params.side,
                params.window_bars,
                metrics["sample_count"], metrics["win_count"], metrics["loss_count"], metrics["scratch_count"],
                metrics["win_rate"], metrics["avg_pnl_pct"], metrics["avg_mfe_pct"], metrics["avg_mae_pct"],
                metrics["avg_continuation_strength"], metrics["avg_reversion_strength"],
                metrics["continuation_hit_rate"], metrics["reversion_hit_rate"],
                params.promotion_score, params.decay_score, params.confidence_score, params.quality_score,
                params.scored_at,
                params.cohort_key, params.horizon,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO release_cohort_scores (
                cohort_key, release_id, release_version, release_channel,
                strategy_id, symbol, side,
                horizon, window_bars,
                sample_count, win_count, loss_count, scratch_count,
                win_rate, avg_pnl_pct, avg_mfe_pct, avg_mae_pct,
                avg_continuation_strength, avg_reversion_strength,
                continuation_hit_rate, reversion_hit_rate,
                promotion_score, decay_score, confidence_score, quality_score,
                scored_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                params.cohort_key, params.release_id, params.release_version, params.release_channel,
                params.strategy_id, params.symbol, params.side,
                params.horizon, params.window_bars,
                metrics["sample_count"], metrics["win_count"], metrics["loss_count"], metrics["scratch_count"],
                metrics["win_rate"], metrics["avg_pnl_pct"], metrics["avg_mfe_pct"], metrics["avg_mae_pct"],
                metrics["avg_continuation_strength"], metrics["avg_reversion_strength"],
                metrics["continuation_hit_rate"], metrics["reversion_hit_rate"],
                params.promotion_score, params.decay_score, params.confidence_score, params.quality_score,
                params.scored_at,
            ),
        )


# ── Feature Lifecycle ────────────────────────────────────────────────

def _normalized_spread(avg_a: Optional[float], avg_b: Optional[float]) -> Optional[float]:
    if avg_a is None or avg_b is None:
        return None
    spread = avg_a - avg_b
    normalizer = max(abs(avg_a), abs(avg_b), _EPSILON)
    return spread / normalizer


def _continuation_correlation(
    cont_values: List[Tuple[float, float]],
) -> Optional[float]:
    if len(cont_values) < 5:
        return None
    high_cont = [v for v, c in cont_values if c > 0.1]
    low_cont = [v for v, c in cont_values if c <= 0.1]
    if not high_cont or not low_cont:
        return None
    avg_high = sum(high_cont) / len(high_cont)
    avg_low = sum(low_cont) / len(low_cont)
    return _normalized_spread(avg_high, avg_low)


def _compute_feature_correlation(
    feature_key: str,
    joined_rows: List[Dict[str, Any]],
) -> Tuple[int, Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Compute correlation between a research field's value and outcomes.

    Returns (sample_count, corr_with_win, corr_with_continuation, avg_value_winners, avg_value_losers).
    """
    winner_values: List[float] = []
    loser_values: List[float] = []
    all_values: List[float] = []
    cont_values: List[Tuple[float, float]] = []

    for row in joined_rows:
        research = row.get("research_context") or {}
        raw_val = research.get(feature_key)
        val = _safe_float(raw_val)
        if val is None:
            continue

        all_values.append(val)
        label = row.get("outcome_label")
        if label == "win":
            winner_values.append(val)
        elif label == "loss":
            loser_values.append(val)

        cont_strength = _safe_float((row.get("outcome") or {}).get("continuation_strength"))
        if cont_strength is not None:
            cont_values.append((val, cont_strength))

    sample_count = len(all_values)
    if sample_count == 0:
        return 0, None, None, None, None

    avg_winners = (sum(winner_values) / len(winner_values)) if winner_values else None
    avg_losers = (sum(loser_values) / len(loser_values)) if loser_values else None

    # Simple correlation proxy: normalized difference between winner and loser averages
    # Simple correlation proxy: normalized difference between winner and loser averages
    corr_win = _normalized_spread(avg_winners, avg_losers)

    # Continuation correlation: average feature value when continuation_strength > 0.1
    corr_cont = _continuation_correlation(cont_values)

    return sample_count, corr_win, corr_cont, avg_winners, avg_losers


def _transition_from_observe(
    sample_count: int, promotion_score: float, decay_score: float, corr_win: Optional[float],
) -> str:
    if sample_count >= PROMOTION_MIN_SAMPLES and corr_win is not None and corr_win > CANDIDATE_CORRELATION_THRESHOLD:
        return "candidate"
    return "observe"


def _transition_from_candidate(
    sample_count: int, promotion_score: float, decay_score: float, corr_win: Optional[float],
) -> str:
    if sample_count >= PROMOTION_MIN_SAMPLES and promotion_score >= PROMOTE_QUALITY_THRESHOLD:
        return "promote"
    if sample_count >= DEMOTION_MIN_SAMPLES and decay_score > 0.6:
        return "decay"
    return "candidate"


def _transition_from_promote(
    sample_count: int, promotion_score: float, decay_score: float, corr_win: Optional[float],
) -> str:
    if sample_count >= DEMOTION_MIN_SAMPLES and decay_score > 0.7:
        return "decay"
    return "promote"


def _transition_from_decay(
    sample_count: int, promotion_score: float, decay_score: float, corr_win: Optional[float],
) -> str:
    if sample_count >= PROMOTION_MIN_SAMPLES and promotion_score >= PROMOTE_QUALITY_THRESHOLD:
        return "candidate"
    if sample_count >= DEMOTION_MIN_SAMPLES and decay_score > 0.8:
        return "retire"
    return "decay"


_LIFECYCLE_HANDLERS: Dict[str, Any] = {
    "observe": _transition_from_observe,
    "candidate": _transition_from_candidate,
    "promote": _transition_from_promote,
    "decay": _transition_from_decay,
}


def _resolve_lifecycle_transition(
    current_status: str,
    sample_count: int,
    promotion_score: float,
    decay_score: float,
    corr_win: Optional[float],
) -> str:
    """Determine the next lifecycle state for a feature."""
    if current_status == "retire":
        return "retire"
    handler = _LIFECYCLE_HANDLERS.get(current_status)
    if handler is not None:
        return handler(sample_count, promotion_score, decay_score, corr_win)
    return current_status


def _upsert_feature_lifecycle(
    conn: sqlite3.Connection,
    *,
    feature_key: str,
    layer: str,
    sample_count: int,
    promotion_score: float,
    decay_score: float,
    confidence_score: float,
    corr_win: Optional[float],
    corr_cont: Optional[float],
    avg_winners: Optional[float],
    avg_losers: Optional[float],
    now_iso: str,
) -> Dict[str, Any]:
    """Upsert a feature lifecycle row and resolve state transitions."""
    existing = conn.execute(
        "SELECT id, current_status FROM feature_lifecycle WHERE feature_key = ?",
        (feature_key,),
    ).fetchone()

    if existing:
        current_status = existing["current_status"]
    else:
        current_status = "observe"

    new_status = _resolve_lifecycle_transition(
        current_status, sample_count, promotion_score, decay_score, corr_win,
    )
    status_changed = new_status != current_status

    if existing:
        conn.execute(
            """
            UPDATE feature_lifecycle SET
                sample_count = ?, promotion_score = ?, decay_score = ?,
                confidence_score = ?,
                correlation_with_win = ?, correlation_with_continuation = ?,
                avg_value_winners = ?, avg_value_losers = ?,
                current_status = ?, previous_status = ?,
                last_evaluated_at = ?,
                status_changed_at = CASE WHEN ? THEN ? ELSE status_changed_at END
            WHERE feature_key = ?
            """,
            (
                sample_count, promotion_score, decay_score,
                confidence_score,
                corr_win, corr_cont,
                avg_winners, avg_losers,
                new_status, current_status if status_changed else None,
                now_iso,
                status_changed, now_iso,
                feature_key,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO feature_lifecycle (
                feature_key, layer, current_status, previous_status,
                sample_count, promotion_score, decay_score, confidence_score,
                correlation_with_win, correlation_with_continuation,
                avg_value_winners, avg_value_losers,
                last_evaluated_at, status_changed_at, created_at
            ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                feature_key, layer, new_status,
                sample_count, promotion_score, decay_score, confidence_score,
                corr_win, corr_cont,
                avg_winners, avg_losers,
                now_iso, now_iso, now_iso,
            ),
        )

    return {
        "feature_key": feature_key,
        "previous_status": current_status,
        "current_status": new_status,
        "transitioned": status_changed,
        "sample_count": sample_count,
    }


# ── Public Interface ─────────────────────────────────────────────────

def run_cohort_scoring(
    *,
    horizon_windows: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Score all release cohorts across horizons and update feature lifecycle.

    Returns a summary dict with cohort counts and feature transitions.
    """
    windows = horizon_windows or HORIZON_WINDOWS
    now_iso = _utc_now_iso()

    journal_rows = load_signal_journal_rows()
    outcome_rows = load_signal_outcome_rows()
    joined = _join_journal_outcomes(journal_rows, outcome_rows)

    if not joined:
        return {
            "scored_at": now_iso,
            "total_joined_rows": 0,
            "horizons": {},
            "feature_transitions": [],
        }

    horizons_summary: Dict[str, Any] = {}

    with get_connection() as conn:
        # ── Score release cohorts per horizon ──
        for horizon_name, window_bars in windows.items():
            windowed = _filter_by_window(joined, window_bars)
            groups = _group_by_cohort(windowed)

            cohort_results: List[Dict[str, Any]] = []
            for cohort_key, group_rows in groups.items():
                metrics = _compute_cohort_metrics(group_rows)
                promotion = _compute_promotion_score(metrics)
                decay = _compute_decay_score(metrics)
                confidence = _compute_confidence_score(metrics)
                quality = _compute_quality_score(metrics)

                sample_row = group_rows[0]
                score_params = CohortScoreParams(
                    cohort_key=cohort_key,
                    release_id=sample_row.get("release_id"),
                    release_version=sample_row.get("release_version"),
                    release_channel=sample_row.get("release_channel"),
                    strategy_id=sample_row.get("strategy_id"),
                    symbol=sample_row.get("symbol"),
                    side=sample_row.get("side"),
                    horizon=horizon_name,
                    window_bars=window_bars,
                    promotion_score=promotion,
                    decay_score=decay,
                    confidence_score=confidence,
                    quality_score=quality,
                    scored_at=now_iso,
                )
                _upsert_cohort_score(conn, score_params, metrics)

                cohort_results.append({
                    "cohort_key": cohort_key,
                    "sample_count": metrics["sample_count"],
                    "win_rate": metrics["win_rate"],
                    "quality_score": quality,
                    "promotion_score": promotion,
                    "decay_score": decay,
                })

            horizons_summary[horizon_name] = {
                "window_bars": window_bars,
                "cohort_count": len(cohort_results),
                "total_samples": sum(c["sample_count"] for c in cohort_results),
                "cohorts": cohort_results,
            }

        # ── Evaluate feature lifecycle ──
        manifest = load_bridge_manifest()
        experimental_fields = manifest.telemetry_contract.experimental_fields
        feature_transitions: List[Dict[str, Any]] = []

        for feature_key, field_def in experimental_fields.items():
            if field_def.layer != "research":
                continue

            sample_count, corr_win, corr_cont, avg_winners, avg_losers = _compute_feature_correlation(
                feature_key, joined,
            )

            if sample_count == 0:
                continue

            safe_corr = corr_win or 0.0
            conf = min(1.0, sample_count / PROMOTION_MIN_SAMPLES)
            promo = max(0.0, safe_corr) * conf
            dec = max(0.0, -safe_corr) * conf

            transition = _upsert_feature_lifecycle(
                conn,
                feature_key=feature_key,
                layer=field_def.layer,
                sample_count=sample_count,
                promotion_score=promo,
                decay_score=dec,
                confidence_score=conf,
                corr_win=corr_win,
                corr_cont=corr_cont,
                avg_winners=avg_winners,
                avg_losers=avg_losers,
                now_iso=now_iso,
            )
            feature_transitions.append(transition)

        conn.commit()

    # ── Append history snapshot ──
    for horizon_name, h_summary in horizons_summary.items():
        for cohort in h_summary.get("cohorts", []):
            _append_history_row({
                "scored_at": now_iso,
                "horizon": horizon_name,
                "window_bars": h_summary["window_bars"],
                "cohort_key": cohort["cohort_key"],
                "sample_count": cohort["sample_count"],
                "win_rate": cohort["win_rate"],
                "quality_score": cohort["quality_score"],
                "promotion_score": cohort["promotion_score"],
                "decay_score": cohort["decay_score"],
            })
    for ft in feature_transitions:
        _append_history_row({
            "scored_at": now_iso,
            "record_type": "feature_lifecycle",
            "feature_key": ft["feature_key"],
            "previous_status": ft["previous_status"],
            "current_status": ft["current_status"],
            "transitioned": ft["transitioned"],
            "sample_count": ft["sample_count"],
        })

    return {
        "scored_at": now_iso,
        "total_joined_rows": len(joined),
        "horizons": horizons_summary,
        "feature_transitions": feature_transitions,
    }


# ── Query Functions ──────────────────────────────────────────────────

def get_cohort_scores(
    *,
    horizon: Optional[str] = None,
    release_version: Optional[str] = None,
    release_channel: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Retrieve stored cohort scores with optional filters."""
    clauses: List[str] = []
    params: List[Any] = []

    if horizon is not None:
        clauses.append("horizon = ?")
        params.append(horizon)
    if release_version is not None:
        clauses.append("release_version = ?")
        params.append(release_version)
    if release_channel is not None:
        clauses.append("release_channel = ?")
        params.append(release_channel)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    safe_limit = max(1, min(limit, 500))
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM release_cohort_scores{where} ORDER BY scored_at DESC LIMIT ?",
            params,
        ).fetchall()

    return [dict(row) for row in rows]


def get_cohort_leaderboard(
    *,
    horizon: str = "medium",
    sort_by: str = "quality_score",
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Rank cohorts by a scoring metric within a specific horizon."""
    valid_sort_keys = {"quality_score", "promotion_score", "decay_score", "win_rate", "confidence_score"}
    if sort_by not in valid_sort_keys:
        sort_by = "quality_score"

    safe_limit = max(1, min(limit, 100))

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM release_cohort_scores
            WHERE horizon = ? AND sample_count > 0
            ORDER BY {sort_by} DESC
            LIMIT ?
            """,
            (horizon, safe_limit),
        ).fetchall()

    return [dict(row) for row in rows]


def get_feature_lifecycle_status(
    *,
    status_filter: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Retrieve feature lifecycle rows."""
    params: List[Any] = []
    where = ""
    if status_filter is not None and status_filter in VALID_LIFECYCLE_STATES:
        where = " WHERE current_status = ?"
        params.append(status_filter)

    safe_limit = max(1, min(limit, 500))
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM feature_lifecycle{where} ORDER BY last_evaluated_at DESC LIMIT ?",
            params,
        ).fetchall()

    return [dict(row) for row in rows]


def compare_release_cohorts(
    *,
    left_version: str,
    right_version: str,
    horizon: str = "medium",
) -> Dict[str, Any]:
    """Side-by-side comparison of two release versions."""
    with get_connection() as conn:
        left_rows = conn.execute(
            "SELECT * FROM release_cohort_scores WHERE release_version = ? AND horizon = ?",
            (left_version, horizon),
        ).fetchall()
        right_rows = conn.execute(
            "SELECT * FROM release_cohort_scores WHERE release_version = ? AND horizon = ?",
            (right_version, horizon),
        ).fetchall()

    def _aggregate(rows: List[sqlite3.Row]) -> Dict[str, Any]:
        if not rows:
            return {"cohort_count": 0}
        dicts = [dict(r) for r in rows]
        total_samples = sum(d.get("sample_count") or 0 for d in dicts)
        avg_quality = _safe_mean([d.get("quality_score") for d in dicts])
        avg_promo = _safe_mean([d.get("promotion_score") for d in dicts])
        avg_decay = _safe_mean([d.get("decay_score") for d in dicts])
        avg_win = _safe_mean([d.get("win_rate") for d in dicts])
        return {
            "cohort_count": len(dicts),
            "total_samples": total_samples,
            "avg_quality_score": avg_quality,
            "avg_promotion_score": avg_promo,
            "avg_decay_score": avg_decay,
            "avg_win_rate": avg_win,
            "cohorts": dicts,
        }

    return {
        "horizon": horizon,
        "left_version": left_version,
        "right_version": right_version,
        "left": _aggregate(left_rows),
        "right": _aggregate(right_rows),
    }


def get_cohort_score_for_signal(
    *,
    release_id: Optional[str],
    release_version: Optional[str],
    release_channel: Optional[str],
    strategy_id: Optional[str],
    symbol: Optional[str],
    side: Optional[str],
    horizon: str = "medium",
) -> Optional[Dict[str, Any]]:
    """Look up the cohort score for a specific signal's release context."""
    cohort_key = _build_cohort_key(
        release_id=release_id,
        release_version=release_version,
        release_channel=release_channel,
        strategy_id=strategy_id,
        symbol=symbol,
        side=side,
    )

    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM release_cohort_scores WHERE cohort_key = ? AND horizon = ?",
            (cohort_key, horizon),
        ).fetchone()

    return dict(row) if row else None


def _parse_history_lines(text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and parsed.get("record_type") != "feature_lifecycle":
            rows.append(parsed)
    return rows


def get_cohort_score_history(
    *,
    cohort_key: Optional[str] = None,
    horizon: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Read scoring history from the append-only JSONL log.

    Returns the most recent `limit` cohort score snapshots, optionally
    filtered by cohort_key and/or horizon.
    """
    path = _history_path()
    if not path.exists():
        return []

    rows = _parse_history_lines(path.read_text(encoding="utf-8"))

    if cohort_key is not None:
        rows = [r for r in rows if r.get("cohort_key") == cohort_key]
    if horizon is not None:
        rows = [r for r in rows if r.get("horizon") == horizon]

    rows.sort(key=lambda r: str(r.get("scored_at") or ""), reverse=True)
    safe_limit = max(1, min(limit, 1000))
    return rows[:safe_limit]
