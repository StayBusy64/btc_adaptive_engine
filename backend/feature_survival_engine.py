"""Feature Survival Engine — scores features and recommends lifecycle actions.

Consumes the master feature registry and outcome data to compute dynamic
survival scores.  The engine periodically re-evaluates every registered
feature and recommends: keep_hot, demote_warm, demote_experimental, or archive.

Survival formula (8 components):
  score = 0.30*predictive_gain
        + 0.20*regime_specific_gain
        + 0.15*confluence_synergy
        + 0.15*recency_relevance
        + 0.10*frequency_of_usefulness
        - 0.05*storage_cost
        - 0.03*compute_cost
        - 0.02*redundancy_penalty
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from backend.feature_registry import (
    FEATURE_INDEX,
    FEATURES,
    FeatureEntry,
    LifecycleStatus,
    SurvivalWeights,
    compute_survival_score,
    lifecycle_from_score,
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HOT_THRESHOLD = 0.70
WARM_THRESHOLD = 0.50
EXPERIMENTAL_THRESHOLD = 0.30

# Minimum outcome samples before a feature can be demoted
MIN_SAMPLES_FOR_SCORING = 50

# Grace period (hours) — new features cannot be demoted within this window
GRACE_PERIOD_HOURS = 72


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FeatureMetrics:
    """Runtime metrics collected for a single feature."""
    key: str
    sample_count: int = 0
    win_rate: float = 0.0              # fraction of outcomes where feature present + positive
    regime_hit_rate: float = 0.0       # fraction of outcomes with correct regime alignment
    confluence_boost: float = 0.0      # avg improvement when this feature has confluence
    last_useful_at: Optional[str] = None  # ISO timestamp of last positive contribution
    times_useful: int = 0              # total count of positive contributions
    bytes_stored: int = 0              # approximate storage footprint
    compute_ms: float = 0.0           # average compute time in milliseconds
    correlation_with: tuple[str, ...] = ()  # keys of features highly correlated (>0.8)


@dataclass
class SurvivalResult:
    """Result of survival scoring for a single feature."""
    key: str
    tier: str
    current_lifecycle: str
    survival_score: float
    recommended_lifecycle: str
    action: str                       # "keep" | "promote" | "demote" | "archive"
    components: dict[str, float]
    reason: str = ""


# ---------------------------------------------------------------------------
# Core scoring
# ---------------------------------------------------------------------------

def compute_dynamic_survival_weights(
    entry: FeatureEntry,
    metrics: Optional[FeatureMetrics] = None,
) -> SurvivalWeights:
    """Build survival weights from runtime metrics, falling back to defaults."""
    if metrics is None or metrics.sample_count < MIN_SAMPLES_FOR_SCORING:
        return entry.survival_weights

    # Map runtime metrics → 0-1 component scores
    predictive_gain = min(1.0, metrics.win_rate * 1.5)  # 0.67 win rate → 1.0
    regime_specific_gain = min(1.0, metrics.regime_hit_rate * 1.25)
    confluence_synergy = min(1.0, metrics.confluence_boost * 2.0)

    # Recency: exponential decay from last_useful_at
    recency = 0.5
    if metrics.last_useful_at:
        try:
            last_dt = datetime.fromisoformat(metrics.last_useful_at)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            hours_ago = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600.0
            recency = math.exp(-0.01 * hours_ago)  # half-life ~69 hours
        except (ValueError, TypeError):
            pass

    frequency = min(1.0, metrics.times_useful / max(1, metrics.sample_count))

    # Cost penalties
    storage_cost = min(1.0, metrics.bytes_stored / (10 * 1024 * 1024))  # 10 MB → 1.0
    compute_cost = min(1.0, metrics.compute_ms / 100.0)  # 100ms → 1.0
    redundancy = min(1.0, len(metrics.correlation_with) * 0.25)

    return SurvivalWeights(
        predictive_gain=predictive_gain,
        regime_specific_gain=regime_specific_gain,
        confluence_synergy=confluence_synergy,
        recency_relevance=recency,
        frequency_of_usefulness=frequency,
        storage_cost=storage_cost,
        compute_cost=compute_cost,
        redundancy_penalty=redundancy,
    )


def score_feature(
    entry: FeatureEntry,
    metrics: Optional[FeatureMetrics] = None,
) -> float:
    """Compute the survival score for a feature given runtime metrics."""
    weights = compute_dynamic_survival_weights(entry, metrics)
    return compute_survival_score(weights)


def _determine_action(
    current: LifecycleStatus,
    recommended: LifecycleStatus,
) -> str:
    order = [LifecycleStatus.ARCHIVED, LifecycleStatus.EXPERIMENTAL,
             LifecycleStatus.WARM, LifecycleStatus.HOT]
    cur_idx = order.index(current)
    rec_idx = order.index(recommended)
    if rec_idx > cur_idx:
        return "promote"
    if rec_idx < cur_idx:
        return "demote" if recommended != LifecycleStatus.ARCHIVED else "archive"
    return "keep"


# ---------------------------------------------------------------------------
# Triage — evaluate all features at once
# ---------------------------------------------------------------------------

def evaluate_feature_triage(
    metrics_by_key: Optional[dict[str, FeatureMetrics]] = None,
) -> list[SurvivalResult]:
    """Score every registered feature and return lifecycle recommendations."""
    if metrics_by_key is None:
        metrics_by_key = {}

    results: list[SurvivalResult] = []
    for entry in FEATURES:
        m = metrics_by_key.get(entry.key)
        weights = compute_dynamic_survival_weights(entry, m)
        score = compute_survival_score(weights)
        recommended = lifecycle_from_score(score)
        current = entry.lifecycle
        action = _determine_action(current, recommended)

        # Grace period: don't demote if insufficient samples
        if action in ("demote", "archive") and (m is None or m.sample_count < MIN_SAMPLES_FOR_SCORING):
            action = "keep"
            reason = f"grace: only {m.sample_count if m else 0} samples (need {MIN_SAMPLES_FOR_SCORING})"
        else:
            reason = f"score={score:.3f} → {recommended.value}"

        results.append(SurvivalResult(
            key=entry.key,
            tier=entry.tier.value,
            current_lifecycle=current.value,
            survival_score=score,
            recommended_lifecycle=recommended.value,
            action=action,
            components={
                "predictive_gain": weights.predictive_gain,
                "regime_specific_gain": weights.regime_specific_gain,
                "confluence_synergy": weights.confluence_synergy,
                "recency_relevance": weights.recency_relevance,
                "frequency_of_usefulness": weights.frequency_of_usefulness,
                "storage_cost": weights.storage_cost,
                "compute_cost": weights.compute_cost,
                "redundancy_penalty": weights.redundancy_penalty,
            },
            reason=reason,
        ))

    return results


def summarize_triage(results: list[SurvivalResult]) -> dict[str, Any]:
    """Aggregate triage results into a summary dict."""
    actions: dict[str, int] = {}
    for r in results:
        actions[r.action] = actions.get(r.action, 0) + 1

    by_lifecycle: dict[str, int] = {}
    for r in results:
        by_lifecycle[r.recommended_lifecycle] = by_lifecycle.get(r.recommended_lifecycle, 0) + 1

    lowest = min(results, key=lambda r: r.survival_score) if results else None
    highest = max(results, key=lambda r: r.survival_score) if results else None

    return {
        "total_features": len(results),
        "actions": actions,
        "recommended_lifecycle_counts": by_lifecycle,
        "lowest_score_feature": lowest.key if lowest else None,
        "lowest_score": lowest.survival_score if lowest else None,
        "highest_score_feature": highest.key if highest else None,
        "highest_score": highest.survival_score if highest else None,
    }
