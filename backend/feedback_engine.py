from typing import Any, Dict, List, Optional


def summarize_state(
    state: dict,
    *,
    cohort_scores: Optional[List[Dict[str, Any]]] = None,
    feature_lifecycle: Optional[List[Dict[str, Any]]] = None,
) -> dict:
    """Feedback summary incorporating release cohort intelligence.

    When cohort scores are available, the summary includes the best and
    worst performing releases, promoted features, and decaying features.
    """
    base = {
        "current_regime": state.get("regime_id", "unknown"),
        "self_trust_score": 0.0,
        "favored_setup_family": None,
        "weak_setup_family": None,
    }

    if not cohort_scores:
        return base

    # Find strongest and weakest cohorts
    scored = [c for c in cohort_scores if (c.get("sample_count") or 0) >= 10]
    if not scored:
        return base

    best = max(scored, key=lambda c: c.get("quality_score") or 0.0)
    worst = min(scored, key=lambda c: c.get("quality_score") or 0.0)

    best_quality = best.get("quality_score") or 0.0
    worst_quality = worst.get("quality_score") or 0.0

    # Self-trust: weighted average of quality scores across active cohorts
    total_samples = sum(c.get("sample_count") or 0 for c in scored)
    if total_samples > 0:
        weighted_quality = sum(
            (c.get("quality_score") or 0.0) * (c.get("sample_count") or 0)
            for c in scored
        ) / total_samples
        base["self_trust_score"] = max(0.0, min(1.0, weighted_quality + 0.5))

    base["best_release"] = {
        "release_version": best.get("release_version"),
        "release_channel": best.get("release_channel"),
        "quality_score": best_quality,
        "win_rate": best.get("win_rate"),
        "sample_count": best.get("sample_count"),
    }
    base["worst_release"] = {
        "release_version": worst.get("release_version"),
        "release_channel": worst.get("release_channel"),
        "quality_score": worst_quality,
        "win_rate": worst.get("win_rate"),
        "sample_count": worst.get("sample_count"),
    }

    # Feature status summary
    if feature_lifecycle:
        base["promoted_features"] = [
            f.get("feature_key") for f in feature_lifecycle
            if f.get("current_status") == "promote"
        ]
        base["decaying_features"] = [
            f.get("feature_key") for f in feature_lifecycle
            if f.get("current_status") in ("decay", "retire")
        ]
        base["candidate_features"] = [
            f.get("feature_key") for f in feature_lifecycle
            if f.get("current_status") == "candidate"
        ]

    return base
