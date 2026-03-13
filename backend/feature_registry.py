"""Master Feature Registry — single source of truth for all tracked features.

Every feature in the system is registered here with its tier, source, survival
score components, and lifecycle status.  The registry is consumed by the
feature survival engine to decide hot / warm / experimental / archive actions.

Tier definitions:
  A — Pine-logged (arrives in webhook payload, zero backend cost)
  B — Backend-derived (computed from Tier A in signal_outcome_engine)
  C — Outcome truth labels (computed after N future bars observed)
  D — Experimental (unproven, must earn promotion)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Tier(str, Enum):
    A = "A"   # Pine-logged raw fields
    B = "B"   # Backend-derived metrics
    C = "C"   # Outcome truth labels
    D = "D"   # Experimental / unproven


class LifecycleStatus(str, Enum):
    HOT = "hot"                   # score >= 0.70 — active in model & snapshots
    WARM = "warm"                 # 0.50–0.69 — logged but not in model
    EXPERIMENTAL = "experimental" # 0.30–0.49 — shadow-logged, no influence
    ARCHIVED = "archived"         # < 0.30 — stopped collecting


# ---------------------------------------------------------------------------
# Feature entry dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SurvivalWeights:
    """Per-feature survival score components (0.0–1.0 each)."""
    predictive_gain: float = 0.50
    regime_specific_gain: float = 0.50
    confluence_synergy: float = 0.50
    recency_relevance: float = 0.80
    frequency_of_usefulness: float = 0.50
    storage_cost: float = 0.05
    compute_cost: float = 0.05
    redundancy_penalty: float = 0.00


SURVIVAL_COEFFICIENTS = {
    "predictive_gain":        0.30,
    "regime_specific_gain":   0.20,
    "confluence_synergy":     0.15,
    "recency_relevance":      0.15,
    "frequency_of_usefulness": 0.10,
    "storage_cost":          -0.05,
    "compute_cost":          -0.03,
    "redundancy_penalty":    -0.02,
}


def compute_survival_score(w: SurvivalWeights) -> float:
    """Weighted sum → single 0–1 survival score."""
    raw = (
        0.30 * w.predictive_gain
        + 0.20 * w.regime_specific_gain
        + 0.15 * w.confluence_synergy
        + 0.15 * w.recency_relevance
        + 0.10 * w.frequency_of_usefulness
        - 0.05 * w.storage_cost
        - 0.03 * w.compute_cost
        - 0.02 * w.redundancy_penalty
    )
    return max(0.0, min(1.0, raw))


def lifecycle_from_score(score: float) -> LifecycleStatus:
    if score >= 0.70:
        return LifecycleStatus.HOT
    if score >= 0.50:
        return LifecycleStatus.WARM
    if score >= 0.30:
        return LifecycleStatus.EXPERIMENTAL
    return LifecycleStatus.ARCHIVED


@dataclass
class FeatureEntry:
    """Single feature in the registry."""
    key: str
    tier: Tier
    source: str                             # "pine" | "backend" | "outcome" | "experimental"
    value_type: str                         # "float" | "int" | "str" | "bool"
    group: str                              # logical group for ROI ranking
    description: str = ""
    unit: Optional[str] = None
    survival_weights: SurvivalWeights = field(default_factory=SurvivalWeights)
    dependencies: tuple[str, ...] = ()      # keys this feature is derived from

    @property
    def survival_score(self) -> float:
        return compute_survival_score(self.survival_weights)

    @property
    def lifecycle(self) -> LifecycleStatus:
        return lifecycle_from_score(self.survival_score)


# ---------------------------------------------------------------------------
# Default survival weight presets per tier
# ---------------------------------------------------------------------------

_TIER_A_DEFAULTS = SurvivalWeights(
    predictive_gain=0.60, regime_specific_gain=0.50, confluence_synergy=0.40,
    recency_relevance=0.90, frequency_of_usefulness=0.80,
    storage_cost=0.02, compute_cost=0.00, redundancy_penalty=0.00,
)

_TIER_B_DEFAULTS = SurvivalWeights(
    predictive_gain=0.55, regime_specific_gain=0.50, confluence_synergy=0.50,
    recency_relevance=0.85, frequency_of_usefulness=0.70,
    storage_cost=0.05, compute_cost=0.10, redundancy_penalty=0.05,
)

_TIER_C_DEFAULTS = SurvivalWeights(
    predictive_gain=0.70, regime_specific_gain=0.60, confluence_synergy=0.30,
    recency_relevance=0.90, frequency_of_usefulness=0.90,
    storage_cost=0.08, compute_cost=0.15, redundancy_penalty=0.00,
)

_TIER_D_DEFAULTS = SurvivalWeights(
    predictive_gain=0.30, regime_specific_gain=0.30, confluence_synergy=0.20,
    recency_relevance=0.70, frequency_of_usefulness=0.30,
    storage_cost=0.10, compute_cost=0.15, redundancy_penalty=0.10,
)


def _a(key: str, vtype: str, group: str, desc: str = "", **kw) -> FeatureEntry:  # noqa: ANN003
    return FeatureEntry(key=key, tier=Tier.A, source="pine", value_type=vtype,
                        group=group, description=desc,
                        survival_weights=kw.get("sw", _TIER_A_DEFAULTS))


def _b(key: str, vtype: str, group: str, deps: tuple[str, ...] = (), desc: str = "", **kw) -> FeatureEntry:  # noqa: ANN003
    return FeatureEntry(key=key, tier=Tier.B, source="backend", value_type=vtype,
                        group=group, description=desc, dependencies=deps,
                        survival_weights=kw.get("sw", _TIER_B_DEFAULTS))


def _c(key: str, vtype: str, group: str, desc: str = "", **kw) -> FeatureEntry:  # noqa: ANN003
    return FeatureEntry(key=key, tier=Tier.C, source="outcome", value_type=vtype,
                        group=group, description=desc,
                        survival_weights=kw.get("sw", _TIER_C_DEFAULTS))


def _d(key: str, vtype: str, group: str, desc: str = "", **kw) -> FeatureEntry:  # noqa: ANN003
    return FeatureEntry(key=key, tier=Tier.D, source="experimental", value_type=vtype,
                        group=group, description=desc,
                        survival_weights=kw.get("sw", _TIER_D_DEFAULTS))


# ===================================================================
# MASTER REGISTRY
# ===================================================================

FEATURES: tuple[FeatureEntry, ...] = (
    # ---------------------------------------------------------------
    # Tier A — Pine-logged OHLCV
    # ---------------------------------------------------------------
    _a("price",   "float", "ohlcv", "Signal-time price"),
    _a("open",    "float", "ohlcv", "Bar open"),
    _a("high",    "float", "ohlcv", "Bar high"),
    _a("low",     "float", "ohlcv", "Bar low"),
    _a("close",   "float", "ohlcv", "Bar close"),
    _a("volume",  "float", "ohlcv", "Bar volume"),
    _a("hl2",     "float", "ohlcv", "(H+L)/2"),
    _a("hlc3",    "float", "ohlcv", "(H+L+C)/3"),
    _a("ohlc4",   "float", "ohlcv", "(O+H+L+C)/4"),

    # Tier A — EMAs / indicators
    _a("fast_ema",     "float", "ema_indicators", "Fast EMA value"),
    _a("slow_ema",     "float", "ema_indicators", "Slow EMA value"),
    _a("ema_trend",    "float", "ema_indicators", "Trend EMA value"),
    _a("rsi",          "float", "ema_indicators", "RSI value"),
    _a("atr",          "float", "ema_indicators", "ATR value"),
    _a("atr_pct",      "float", "ema_indicators", "ATR as % of price"),
    _a("volume_sma",   "float", "ema_indicators", "Volume SMA"),
    _a("volume_ratio", "float", "ema_indicators", "Volume / SMA ratio"),

    # Tier A — candle anatomy
    _a("body_size",          "float", "candle_anatomy", "Abs body size"),
    _a("range_size",         "float", "candle_anatomy", "High - Low"),
    _a("upper_wick",         "float", "candle_anatomy", "Upper wick size"),
    _a("lower_wick",         "float", "candle_anatomy", "Lower wick size"),
    _a("body_pct_of_range",  "float", "candle_anatomy", "Body / range %"),
    _a("upper_wick_pct",     "float", "candle_anatomy", "Upper wick / range %"),
    _a("lower_wick_pct",     "float", "candle_anatomy", "Lower wick / range %"),

    # Tier A — trend / regime categorical
    _a("ema_bull_stack",     "bool", "regime", "Fast > Slow > Trend"),
    _a("ema_bear_stack",     "bool", "regime", "Fast < Slow < Trend"),
    _a("trend_direction",    "str",  "regime", "bullish/bearish/neutral"),
    _a("price_vs_trend",     "str",  "regime", "above/below trend EMA"),
    _a("momentum_regime",    "str",  "regime", "RSI regime label"),
    _a("volatility_regime",  "str",  "regime", "ATR regime label"),
    _a("volume_regime",      "str",  "regime", "Volume regime label"),
    _a("candle_bias",        "str",  "regime", "Candle colour bias"),
    _a("wick_bias",          "str",  "regime", "Wick direction bias"),

    # Tier A — volumatic S/R
    _a("volumatic_upper_level",     "float", "volumatic", "Upper S/R level"),
    _a("volumatic_lower_level",     "float", "volumatic", "Lower S/R level"),
    _a("volumatic_n_vol",           "float", "volumatic", "Normalized volume"),
    _a("volumatic_upper_band_high", "float", "volumatic", "Upper band high"),
    _a("volumatic_upper_band_low",  "float", "volumatic", "Upper band low"),
    _a("volumatic_lower_band_high", "float", "volumatic", "Lower band high"),
    _a("volumatic_lower_band_low",  "float", "volumatic", "Lower band low"),

    # Tier A — swing structure
    _a("internal_swing_high", "float", "swing_structure", "Most recent internal swing high"),
    _a("internal_swing_low",  "float", "swing_structure", "Most recent internal swing low"),
    _a("major_swing_high",    "float", "swing_structure", "Most recent major swing high"),
    _a("major_swing_low",     "float", "swing_structure", "Most recent major swing low"),

    # Tier A — prediction map
    _a("prediction_swing_level", "float", "prediction_map", "Predicted target level"),
    _a("inducement_level",       "float", "prediction_map", "Inducement trap level"),
    _a("continuation_level",     "float", "prediction_map", "Continuation level"),
    _a("invalidation_level",     "float", "prediction_map", "Invalidation / stop level"),
    _a("displacement_origin",    "float", "prediction_map", "Displacement origin price"),
    _a("displacement_far_edge",  "float", "prediction_map", "Displacement far edge"),
    _a("bull_displacement",      "bool",  "prediction_map", "Bullish displacement active"),
    _a("bear_displacement",      "bool",  "prediction_map", "Bearish displacement active"),
    _a("probability_score",      "float", "prediction_map", "Pine probability score"),

    # ---------------------------------------------------------------
    # Tier B — Backend-derived metrics
    # ---------------------------------------------------------------
    # EMA distances
    _b("ema_spread",               "float", "ema_distances", ("fast_ema", "slow_ema")),
    _b("ema_spread_pct",           "float", "ema_distances", ("ema_spread", "price")),
    _b("distance_from_fast",       "float", "ema_distances", ("price", "fast_ema")),
    _b("distance_from_fast_pct",   "float", "ema_distances", ("distance_from_fast", "price")),
    _b("distance_from_slow",       "float", "ema_distances", ("price", "slow_ema")),
    _b("distance_from_slow_pct",   "float", "ema_distances", ("distance_from_slow", "price")),
    _b("distance_to_ema_trend",     "float", "ema_distances", ("price", "ema_trend")),
    _b("distance_to_ema_trend_pct", "float", "ema_distances", ("distance_to_ema_trend", "price")),
    _b("distance_to_ema_trend_atr", "float", "ema_distances", ("distance_to_ema_trend", "atr")),

    # Candle metrics
    _b("candle_range",             "float", "candle_metrics", ("high", "low")),
    _b("body",                     "float", "candle_metrics", ("open", "close")),
    _b("wick_ratio",               "float", "candle_metrics", ("candle_range", "body")),
    _b("close_position_in_range",  "float", "candle_metrics", ("close", "low", "high")),

    # Volumatic distances
    _b("distance_to_volumatic_upper",     "float", "volumatic_distances", ("volumatic_upper_level", "price")),
    _b("distance_to_volumatic_upper_atr", "float", "volumatic_distances", ("distance_to_volumatic_upper", "atr")),
    _b("distance_to_volumatic_lower",     "float", "volumatic_distances", ("price", "volumatic_lower_level")),
    _b("distance_to_volumatic_lower_atr", "float", "volumatic_distances", ("distance_to_volumatic_lower", "atr")),

    # Swing distances (NEW — Tier B expansion)
    _b("distance_to_internal_high",     "float", "swing_distances", ("internal_swing_high", "price")),
    _b("distance_to_internal_high_atr", "float", "swing_distances", ("distance_to_internal_high", "atr")),
    _b("distance_to_internal_low",      "float", "swing_distances", ("price", "internal_swing_low")),
    _b("distance_to_internal_low_atr",  "float", "swing_distances", ("distance_to_internal_low", "atr")),
    _b("distance_to_major_high",        "float", "swing_distances", ("major_swing_high", "price")),
    _b("distance_to_major_high_atr",    "float", "swing_distances", ("distance_to_major_high", "atr")),
    _b("distance_to_major_low",         "float", "swing_distances", ("price", "major_swing_low")),
    _b("distance_to_major_low_atr",     "float", "swing_distances", ("distance_to_major_low", "atr")),

    # Prediction structure distances
    _b("distance_to_prediction_swing",     "float", "prediction_distances", ("prediction_swing_level", "price")),
    _b("distance_to_prediction_swing_atr", "float", "prediction_distances", ("distance_to_prediction_swing", "atr")),
    _b("distance_to_inducement",           "float", "prediction_distances", ("price", "inducement_level")),
    _b("distance_to_inducement_atr",       "float", "prediction_distances", ("distance_to_inducement", "atr")),
    _b("distance_to_continuation",         "float", "prediction_distances", ("continuation_level", "price")),
    _b("distance_to_continuation_atr",     "float", "prediction_distances", ("distance_to_continuation", "atr")),
    _b("distance_to_invalidation",         "float", "prediction_distances", ("price", "invalidation_level")),
    _b("distance_to_invalidation_atr",     "float", "prediction_distances", ("distance_to_invalidation", "atr")),

    # R-multiples & confluence
    _b("rr_to_target",       "float", "risk_reward", ("distance_to_prediction_swing", "distance_to_invalidation")),
    _b("rr_to_continuation", "float", "risk_reward", ("distance_to_continuation", "distance_to_invalidation")),
    _b("confluence_count",   "int",   "confluence",  (), "Count of levels within 1 ATR of price"),

    # Alignment / combo scores (NEW — Tier B expansion)
    _b("signal_alignment_score", "float", "alignment",
       ("trend_direction", "momentum_regime", "volume_regime", "candle_bias"),
       "0-1 score: how many regime signals agree with trade side"),
    _b("confluence_score",       "float", "alignment",
       ("confluence_count",),
       "Weighted confluence (levels within 0.5/1/2 ATR bands)"),

    # ---------------------------------------------------------------
    # Tier C — Outcome truth labels
    # ---------------------------------------------------------------
    # Horizon close prices
    _c("close_1bar",  "float", "horizon_returns", "Close price 1 bar forward"),
    _c("close_3bar",  "float", "horizon_returns", "Close price 3 bars forward"),
    _c("close_5bar",  "float", "horizon_returns", "Close price 5 bars forward"),
    _c("close_10bar", "float", "horizon_returns", "Close price 10 bars forward"),

    # Excursion
    _c("mfe",     "float", "excursion", "Max Favorable Excursion (raw)"),
    _c("mae",     "float", "excursion", "Max Adverse Excursion (raw)"),
    _c("mfe_pct", "float", "excursion", "MFE as % of entry"),
    _c("mae_pct", "float", "excursion", "MAE as % of entry"),
    _c("mfe_atr", "float", "excursion", "MFE / ATR at signal time"),
    _c("mae_atr", "float", "excursion", "MAE / ATR at signal time"),
    _c("bars_to_mfe", "int", "excursion", "Bars elapsed until MFE reached"),
    _c("bars_to_mae", "int", "excursion", "Bars elapsed until MAE reached"),

    # Existing outcome signals
    _c("reversion_hit_5bar",     "bool",  "outcome_hits", "Price touched fast EMA within 5 bars"),
    _c("continuation_hit_5bar",  "bool",  "outcome_hits", "Price extended beyond threshold within 5 bars"),
    _c("reversion_strength",     "float", "outcome_hits", "Reversion strength metric"),
    _c("continuation_strength",  "float", "outcome_hits", "Continuation strength metric"),
    _c("signed_move_pct_5bar",   "float", "outcome_hits", "Signed move % at 5-bar close"),

    # NEW truth labels — prediction structure hits
    _c("target_hit",             "bool",  "prediction_hits", "Price reached prediction_swing_level"),
    _c("invalidation_hit",       "bool",  "prediction_hits", "Price breached invalidation_level"),
    _c("continuation_hit",       "bool",  "prediction_hits", "Price reached continuation_level"),
    _c("inducement_swept",       "bool",  "prediction_hits", "Price swept inducement_level"),
    _c("displacement_origin_hit", "bool", "prediction_hits", "Price returned to displacement_origin"),

    # Return windows
    _c("return_1bar_pct",  "float", "forward_returns", "Signed return % at 1 bar"),
    _c("return_3bar_pct",  "float", "forward_returns", "Signed return % at 3 bars"),
    _c("return_5bar_pct",  "float", "forward_returns", "Signed return % at 5 bars"),
    _c("return_10bar_pct", "float", "forward_returns", "Signed return % at 10 bars"),
    _c("return_20bar_pct", "float", "forward_returns", "Signed return % at 20 bars"),

    # Quality classification labels
    _c("signal_quality_label",    "str", "quality_labels", "good/neutral/bad based on MFE/MAE ratio"),
    _c("entry_efficiency_label",  "str", "quality_labels", "early/timely/late based on bars_to_mfe"),
    _c("structure_truth_label",   "str", "quality_labels", "confirmed/failed/ambiguous"),
    _c("regime_success_label",    "str", "quality_labels", "strong/weak/contra based on regime alignment outcome"),

    # ---------------------------------------------------------------
    # Tier D — Experimental (unproven, must earn promotion)
    # ---------------------------------------------------------------
    _d("trend_volume_combo",     "float", "experimental_combos", "trend_direction aligned with volume_regime"),
    _d("trend_volatility_combo", "float", "experimental_combos", "trend_direction aligned with volatility_regime"),
    _d("momentum_structure_combo", "float", "experimental_combos", "momentum_regime aligned with swing structure"),
)


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

FEATURE_INDEX: dict[str, FeatureEntry] = {f.key: f for f in FEATURES}


def get_feature(key: str) -> Optional[FeatureEntry]:
    return FEATURE_INDEX.get(key)


def get_features_by_tier(tier: Tier) -> tuple[FeatureEntry, ...]:
    return tuple(f for f in FEATURES if f.tier == tier)


def get_features_by_group(group: str) -> tuple[FeatureEntry, ...]:
    return tuple(f for f in FEATURES if f.group == group)


def get_features_by_lifecycle(status: LifecycleStatus) -> tuple[FeatureEntry, ...]:
    return tuple(f for f in FEATURES if f.lifecycle == status)


def get_all_groups() -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for f in FEATURES:
        seen.setdefault(f.group, None)
    return tuple(seen.keys())


def summarize_registry() -> dict[str, int]:
    """Return counts by tier and lifecycle status."""
    result: dict[str, int] = {}
    for tier in Tier:
        result[f"tier_{tier.value}_count"] = sum(1 for f in FEATURES if f.tier == tier)
    for status in LifecycleStatus:
        result[f"lifecycle_{status.value}_count"] = sum(1 for f in FEATURES if f.lifecycle == status)
    result["total_features"] = len(FEATURES)
    return result
