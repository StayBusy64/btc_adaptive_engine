import math
from typing import Any, Dict, Optional


def apply_time_decay(base_weight: float, decay_rate: float, time_delta: float) -> float:
    return base_weight * math.exp(-decay_rate * time_delta)


def apply_performance_decay(base_weight: float, penalty: float) -> float:
    return max(0.0, base_weight * (1.0 - penalty))


def apply_cohort_decay(
    base_weight: float,
    cohort_decay_score: float,
    cohort_confidence: float,
) -> float:
    """Apply release-cohort-informed decay to a signal weight.

    When the cohort has high decay_score (poor performance) and high
    confidence (enough samples), the weight shrinks faster.
    """
    effective_penalty = cohort_decay_score * cohort_confidence
    return max(0.0, base_weight * (1.0 - effective_penalty))


def apply_contradiction_decay(
    base_weight: float,
    contradiction_multiplier: float,
    half_life_bars: int,
    bars_since_signal: int,
    has_contradiction: bool,
) -> float:
    """Decay a signal weight using the manifest's contradiction framework.

    If an active contradiction signal is detected, the effective half-life
    is shortened by the contradiction_multiplier, accelerating decay.
    """
    if half_life_bars <= 0:
        return 0.0

    effective_half_life = half_life_bars
    if has_contradiction:
        effective_half_life = max(1, int(half_life_bars / contradiction_multiplier))

    decay_rate = math.log(2) / effective_half_life
    return base_weight * math.exp(-decay_rate * bars_since_signal)


def compute_signal_weight(
    *,
    base_weight: float = 1.0,
    time_decay_rate: float = 0.002,
    bars_elapsed: float = 0.0,
    performance_penalty: float = 0.0,
    cohort_score: Optional[Dict[str, Any]] = None,
    contradiction_active: bool = False,
    contradiction_multiplier: float = 2.0,
    half_life_bars: int = 10,
) -> Dict[str, Any]:
    """Compute a composite signal weight incorporating all decay dimensions.

    Returns a dict with the final weight and intermediate components for
    observability.
    """
    w = base_weight

    # Time decay
    w_time = apply_time_decay(w, time_decay_rate, bars_elapsed)

    # Performance decay
    w_perf = apply_performance_decay(w_time, performance_penalty)

    # Cohort decay
    w_cohort = w_perf
    cohort_penalty = 0.0
    if cohort_score is not None:
        cohort_decay = cohort_score.get("decay_score") or 0.0
        cohort_conf = cohort_score.get("confidence_score") or 0.0
        w_cohort = apply_cohort_decay(w_perf, cohort_decay, cohort_conf)
        cohort_penalty = cohort_decay * cohort_conf

    # Contradiction decay
    w_final = apply_contradiction_decay(
        w_cohort,
        contradiction_multiplier,
        half_life_bars,
        int(bars_elapsed),
        contradiction_active,
    )

    return {
        "final_weight": w_final,
        "base_weight": base_weight,
        "after_time_decay": w_time,
        "after_performance_decay": w_perf,
        "after_cohort_decay": w_cohort,
        "cohort_penalty": cohort_penalty,
        "contradiction_active": contradiction_active,
        "bars_elapsed": bars_elapsed,
    }
