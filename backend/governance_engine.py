from typing import Any, Dict, Optional


def govern(
    model_output: dict,
    *,
    cohort_score: Optional[Dict[str, Any]] = None,
    feature_lifecycle: Optional[Dict[str, Dict[str, Any]]] = None,
) -> dict:
    """Governance gate that uses release cohort intelligence.

    When a cohort score is available, the gate adjusts its confidence cap
    and may allow trades only if the cohort has earned enough trust.
    """
    if cohort_score is None:
        return {
            "allow_trade": False,
            "observe_only": True,
            "confidence_cap": 0.0,
            "governance_reason": "no_cohort_score",
        }

    promotion_score = cohort_score.get("promotion_score") or 0.0
    decay_score = cohort_score.get("decay_score") or 0.0
    confidence_score = cohort_score.get("confidence_score") or 0.0
    quality_score = cohort_score.get("quality_score") or 0.0
    sample_count = cohort_score.get("sample_count") or 0

    # Cohort must have minimum samples before earning trade rights
    if sample_count < 20:
        return {
            "allow_trade": False,
            "observe_only": True,
            "confidence_cap": 0.0,
            "governance_reason": "insufficient_cohort_samples",
            "sample_count": sample_count,
        }

    # High decay score blocks trading
    if decay_score > 0.7 and confidence_score > 0.5:
        return {
            "allow_trade": False,
            "observe_only": True,
            "confidence_cap": 0.0,
            "governance_reason": "cohort_decay_block",
            "decay_score": decay_score,
        }

    # Marginal cohort: observe only, but raise confidence cap
    if quality_score < 0.05:
        return {
            "allow_trade": False,
            "observe_only": True,
            "confidence_cap": min(promotion_score, 0.3),
            "governance_reason": "marginal_quality",
            "quality_score": quality_score,
        }

    # Cohort has earned confidence
    confidence_cap = min(1.0, promotion_score * confidence_score)

    # Demote features that are in decay/retire state
    active_feature_penalty = 0.0
    if feature_lifecycle:
        decayed_count = sum(
            1 for f in feature_lifecycle.values()
            if f.get("current_status") in ("decay", "retire")
        )
        total_features = len(feature_lifecycle)
        if total_features > 0:
            active_feature_penalty = decayed_count / total_features

    adjusted_cap = max(0.0, confidence_cap * (1.0 - active_feature_penalty * 0.5))

    return {
        "allow_trade": adjusted_cap > 0.15,
        "observe_only": adjusted_cap <= 0.15,
        "confidence_cap": adjusted_cap,
        "governance_reason": "cohort_approved" if adjusted_cap > 0.15 else "below_threshold",
        "quality_score": quality_score,
        "promotion_score": promotion_score,
        "decay_score": decay_score,
        "active_feature_penalty": active_feature_penalty,
    }
