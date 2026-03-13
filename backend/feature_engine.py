from __future__ import annotations

import logging
from typing import Any

from backend.bar_utils import normalize_bar_rows
from backend.candle_feature_engine import CandleFeatureEngine
from backend.displacement_engine import DisplacementEngine
from backend.divergence_engine import DivergenceEngine
from backend.feature_contract import FeatureContext, FeatureEngine, FeatureMap, FeatureSpec
from backend.indicators_engine import IndicatorsEngine
from backend.liquidity_engine import LiquidityEngine
from backend.model_engine import score_state
from backend.orderflow_engine import OrderFlowEngine
from backend.range_expansion_engine import RangeExpansionEngine
from backend.regime_engine import classify_regime
from backend.session_context_engine import SessionContextEngine
from backend.structure_engine import StructureEngine
from backend.trend_engine import TrendEngine
from backend.volatility_engine import VolatilityEngine
from backend.volume_profile_engine import compute_and_store_volume_profile_snapshot
from backend.event_writer import (
    attach_feature_snapshot_to_bar_state,
    get_recent_bar_states_for_symbol_timeframe,
    insert_feature_snapshot,
    insert_model_prediction,
    insert_regime_state,
    register_feature_registry_entries,
)

logger = logging.getLogger(__name__)
DEFAULT_FEATURE_LOOKBACK = 300
FEATURE_VERSION = "feature-engine-v1"
VP_AUCTION_REGIME_PRIORITY: tuple[tuple[str, str], ...] = (
    ("vp_reversion_to_value_from_above_context", "reversion_from_above"),
    ("vp_reversion_to_value_from_below_context", "reversion_from_below"),
    ("vp_continuation_auction_up", "continuation_up"),
    ("vp_continuation_auction_down", "continuation_down"),
    ("vp_acceptance_outside_value_above", "acceptance_above"),
    ("vp_acceptance_outside_value_below", "acceptance_below"),
    ("vp_failed_auction_above", "failed_auction_above"),
    ("vp_failed_auction_below", "failed_auction_below"),
)
VP_TRADE_BIAS_BY_AUCTION_REGIME: dict[str, str] = {
    "reversion_from_below": "long_reversion",
    "reversion_from_above": "short_reversion",
    "continuation_up": "long_continuation",
    "continuation_down": "short_continuation",
}
VP_ACTIONABLE_TRADE_BIASES = {
    "long_reversion",
    "short_reversion",
    "long_continuation",
    "short_continuation",
}
VP_HIGH_CONFIDENCE_AUCTION_REGIMES = {"continuation_up", "continuation_down"}
VP_MEDIUM_CONFIDENCE_AUCTION_REGIMES = {"reversion_from_above", "reversion_from_below"}
VP_LOW_CONFIDENCE_AUCTION_REGIMES = {
    "acceptance_above",
    "acceptance_below",
    "failed_auction_above",
    "failed_auction_below",
}
VP_TRADE_BIAS_SCORE_BY_CONFIDENCE: dict[str, int] = {
    "high": 3,
    "medium": 2,
    "low": 1,
    "none": 0,
}


class FeaturePipeline:
    def __init__(self, engines: list[FeatureEngine]):
        self.engines = engines

    def specs(self) -> tuple[FeatureSpec, ...]:
        all_specs: list[FeatureSpec] = []
        for engine in self.engines:
            all_specs.extend(engine.specs())
        return tuple(all_specs)

    def compute(self, context: FeatureContext) -> FeatureMap:
        combined: FeatureMap = {}
        for engine in self.engines:
            engine_values = engine.compute(context)
            for key, value in engine_values.items():
                combined[key] = value
        return combined


_DEFAULT_PIPELINE = FeaturePipeline(
    [
        CandleFeatureEngine(),
        RangeExpansionEngine(),
        VolatilityEngine(),
        TrendEngine(),
        IndicatorsEngine(),
        StructureEngine(),
        LiquidityEngine(),
        DisplacementEngine(),
        SessionContextEngine(),
        OrderFlowEngine(),
        DivergenceEngine(),
    ]
)


def get_default_feature_pipeline() -> FeaturePipeline:
    return _DEFAULT_PIPELINE


def resolve_vp_auction_regime(feature_values: dict[str, Any]) -> str:
    for feature_key, regime_label in VP_AUCTION_REGIME_PRIORITY:
        if feature_values.get(feature_key, 0) == 1:
            return regime_label
    return "neutral"


def resolve_vp_trade_bias(vp_auction_regime: Any) -> str:
    return VP_TRADE_BIAS_BY_AUCTION_REGIME.get(str(vp_auction_regime), "neutral")


def resolve_vp_trade_bias_actionable(vp_trade_bias: Any) -> int:
    return 1 if str(vp_trade_bias) in VP_ACTIONABLE_TRADE_BIASES else 0


def resolve_vp_trade_bias_confidence(vp_auction_regime: Any, vp_trade_bias_actionable: Any) -> str:
    regime = str(vp_auction_regime)
    actionable = 1 if vp_trade_bias_actionable == 1 else 0

    if actionable == 1 and regime in VP_HIGH_CONFIDENCE_AUCTION_REGIMES:
        return "high"
    if actionable == 1 and regime in VP_MEDIUM_CONFIDENCE_AUCTION_REGIMES:
        return "medium"
    if actionable == 0 and regime in VP_LOW_CONFIDENCE_AUCTION_REGIMES:
        return "low"
    return "none"


def resolve_vp_trade_bias_score(vp_trade_bias_confidence: Any) -> int:
    return VP_TRADE_BIAS_SCORE_BY_CONFIDENCE.get(str(vp_trade_bias_confidence), 0)


def resolve_vp_policy_candidate(vp_trade_bias_actionable: Any, vp_trade_bias_score: Any) -> int:
    try:
        score = float(vp_trade_bias_score)
    except (TypeError, ValueError):
        score = 0.0
    return 1 if vp_trade_bias_actionable == 1 and score >= 2.0 else 0


def resolve_vp_policy_side(vp_trade_bias: Any, vp_policy_candidate: Any) -> str:
    if vp_policy_candidate == 1 and str(vp_trade_bias) in {"long_reversion", "long_continuation"}:
        return "long"
    if vp_policy_candidate == 1 and str(vp_trade_bias) in {"short_reversion", "short_continuation"}:
        return "short"
    return "none"


def resolve_vp_trade_bias_summary(
    vp_auction_regime: Any,
    vp_trade_bias: Any,
    vp_trade_bias_confidence: Any,
    vp_trade_bias_score: Any,
) -> str:
    return (
        f"{vp_auction_regime}|{vp_trade_bias}|{vp_trade_bias_confidence}|"
        f"score={vp_trade_bias_score}"
    )


def resolve_vp_policy_reason(
    vp_policy_side: Any,
    vp_trade_bias_summary: Any,
    vp_policy_candidate: Any,
) -> str:
    return f"{vp_policy_side}|{vp_trade_bias_summary}|candidate={vp_policy_candidate}"


def run_feature_pipeline_for_latest_bar(
    *,
    symbol: str,
    timeframe: str,
    source_bar_id: int,
    lookback: int = DEFAULT_FEATURE_LOOKBACK,
) -> dict[str, Any] | None:
    bar_rows = get_recent_bar_states_for_symbol_timeframe(
        symbol=symbol,
        timeframe=timeframe,
        limit=lookback,
        ascending=True,
    )
    normalized_bars = normalize_bar_rows(bar_rows)

    if not normalized_bars:
        logger.info("feature pipeline skipped: no normalized bars symbol=%s timeframe=%s", symbol, timeframe)
        return None

    latest_bar = normalized_bars[-1]
    timestamp = str(latest_bar.get("timestamp") or "")

    # Side-effect: Rolling volume profile computation
    vp_snapshot = None
    try:
        vp_snapshot = compute_and_store_volume_profile_snapshot(
            bars=normalized_bars,
            symbol=symbol,
            timeframe=timeframe,
        )
    except Exception:
        logger.exception("Failed to compute and store rolling volume profile snapshot")

    context = FeatureContext(
        symbol=symbol,
        timeframe=timeframe,
        bars=normalized_bars,
        latest_bar_id=source_bar_id,
        latest_timestamp=timestamp,
    )

    pipeline = get_default_feature_pipeline()
    register_feature_registry_entries(pipeline.specs())

    feature_values = pipeline.compute(context)
    
    if vp_snapshot:
        feature_values["vp_poc_relative"] = vp_snapshot.poc_relative
        feature_values["vp_value_area_width_pct"] = vp_snapshot.value_area_width_pct
        feature_values["vp_poc_migration_delta"] = vp_snapshot.poc_migration_delta
        feature_values["vp_poc_migrating_up"] = 1 if vp_snapshot.poc_migrating_up else 0
        feature_values["vp_poc_migrating_down"] = 1 if vp_snapshot.poc_migrating_down else 0
        feature_values["vp_poc_migration_strength"] = vp_snapshot.poc_migration_strength
        
        if vp_snapshot.distance_to_poc_pct is not None:
            feature_values["vp_close_pos_in_profile"] = vp_snapshot.close_position_in_profile
            feature_values["vp_distance_to_poc_pct"] = vp_snapshot.distance_to_poc_pct
            feature_values["vp_distance_to_vah_pct"] = vp_snapshot.distance_to_vah_pct
            feature_values["vp_distance_to_val_pct"] = vp_snapshot.distance_to_val_pct
            feature_values["vp_inside_value_area"] = 1 if vp_snapshot.inside_value_area else 0
            feature_values["vp_above_vah"] = 1 if vp_snapshot.above_vah else 0
            feature_values["vp_below_val"] = 1 if vp_snapshot.below_val else 0

            # Profile-aware feature flags
            feature_values["vp_reversion_candidate"] = 1 if (
                not vp_snapshot.inside_value_area
                and vp_snapshot.distance_to_poc_pct < 0.25
            ) else 0

            feature_values["vp_acceptance_above_value"] = 1 if (
                vp_snapshot.above_vah
                and (vp_snapshot.close_position_in_profile or 0.0) > 1.0
            ) else 0

            feature_values["vp_acceptance_below_value"] = 1 if (
                vp_snapshot.below_val
                and (vp_snapshot.close_position_in_profile or 0.0) < 0.0
            ) else 0

            feature_values["vp_balanced_rotation_context"] = 1 if (
                vp_snapshot.inside_value_area
                and vp_snapshot.balance_state == "balanced"
            ) else 0

            feature_values["vp_compressed_value_area"] = 1 if (
                vp_snapshot.value_area_width_pct < 0.35
            ) else 0

            feature_values["vp_poc_magnet_context"] = 1 if (
                vp_snapshot.distance_to_poc_pct < 0.08
            ) else 0

            # Multi-bar validation logic
            try:
                if len(normalized_bars) >= 3:
                    recent_bars = normalized_bars[-3:]
                    above_vah_count = sum(1 for b in recent_bars if b.get("close") is not None and b["close"] > vp_snapshot.vah)
                    below_val_count = sum(1 for b in recent_bars if b.get("close") is not None and b["close"] < vp_snapshot.val)

                    # Rejections based on current bar and previous bar
                    prev_bar = normalized_bars[-2]
                    curr_bar = normalized_bars[-1]

                    prev_close = prev_bar.get("close", 0)
                    curr_close = curr_bar.get("close", 0)

                    feature_values["vp_acceptance_above_value_confirmed"] = 1 if above_vah_count >= 2 else 0
                    feature_values["vp_acceptance_below_value_confirmed"] = 1 if below_val_count >= 2 else 0

                    feature_values["vp_rejection_back_into_value_from_above"] = 1 if (
                        prev_close > vp_snapshot.vah and curr_close <= vp_snapshot.vah
                    ) else 0

                    feature_values["vp_rejection_back_into_value_from_below"] = 1 if (
                        prev_close < vp_snapshot.val and curr_close >= vp_snapshot.val
                    ) else 0
                else:
                    feature_values["vp_acceptance_above_value_confirmed"] = 0
                    feature_values["vp_acceptance_below_value_confirmed"] = 0
                    feature_values["vp_rejection_back_into_value_from_above"] = 0
                    feature_values["vp_rejection_back_into_value_from_below"] = 0
            except Exception:
                logger.exception(
                    "VP multi-bar logic failure",
                    extra={
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "bar_count": len(normalized_bars),
                        "vah": vp_snapshot.vah,
                        "val": vp_snapshot.val,
                    },
                )
                feature_values["vp_acceptance_below_value_confirmed"] = 0
                feature_values["vp_rejection_back_into_value_from_above"] = 0
                feature_values["vp_rejection_back_into_value_from_below"] = 0

        migrating_up_flag = 1 if feature_values.get("vp_poc_migrating_up", 0) == 1 else 0
        migrating_down_flag = 1 if feature_values.get("vp_poc_migrating_down", 0) == 1 else 0
        acceptance_above_value = 1 if feature_values.get("vp_acceptance_above_value", 0) == 1 else 0
        acceptance_below_value = 1 if feature_values.get("vp_acceptance_below_value", 0) == 1 else 0
        acceptance_above_confirmed = 1 if feature_values.get("vp_acceptance_above_value_confirmed", 0) == 1 else 0
        acceptance_below_confirmed = 1 if feature_values.get("vp_acceptance_below_value_confirmed", 0) == 1 else 0

        feature_values["vp_acceptance_outside_value_above"] = (
            1 if acceptance_above_value == 1 and acceptance_above_confirmed == 1 else 0
        )
        feature_values["vp_acceptance_outside_value_below"] = (
            1 if acceptance_below_value == 1 and acceptance_below_confirmed == 1 else 0
        )
        feature_values["vp_continuation_auction_up"] = (
            1 if feature_values["vp_acceptance_outside_value_above"] == 1 and migrating_up_flag == 1 else 0
        )
        feature_values["vp_continuation_auction_down"] = (
            1 if feature_values["vp_acceptance_outside_value_below"] == 1 and migrating_down_flag == 1 else 0
        )

        feature_values["vp_equilibrium_rising_context"] = 1 if migrating_up_flag == 1 else 0
        feature_values["vp_equilibrium_falling_context"] = 1 if migrating_down_flag == 1 else 0
        feature_values["vp_equilibrium_stable_context"] = (
            1 if migrating_up_flag == 0 and migrating_down_flag == 0 else 0
        )
        feature_values["vp_value_shift_with_acceptance_up"] = (
            1 if migrating_up_flag == 1 and acceptance_above_confirmed == 1 else 0
        )
        feature_values["vp_value_shift_with_acceptance_down"] = (
            1 if migrating_down_flag == 1 and acceptance_below_confirmed == 1 else 0
        )
        rejection_from_above = 1 if feature_values.get("vp_rejection_back_into_value_from_above", 0) == 1 else 0
        rejection_from_below = 1 if feature_values.get("vp_rejection_back_into_value_from_below", 0) == 1 else 0
        reversion_candidate = 1 if feature_values.get("vp_reversion_candidate", 0) == 1 else 0

        feature_values["vp_failed_auction_above"] = (
            1 if rejection_from_above == 1 and migrating_up_flag == 0 else 0
        )
        feature_values["vp_failed_auction_below"] = (
            1 if rejection_from_below == 1 and migrating_down_flag == 0 else 0
        )
        feature_values["vp_reversion_to_value_from_above_context"] = (
            1
            if feature_values["vp_failed_auction_above"] == 1 and reversion_candidate == 1
            else 0
        )
        feature_values["vp_reversion_to_value_from_below_context"] = (
            1
            if feature_values["vp_failed_auction_below"] == 1 and reversion_candidate == 1
            else 0
        )

    feature_values["vp_auction_regime"] = resolve_vp_auction_regime(feature_values)
    feature_values["vp_trade_bias"] = resolve_vp_trade_bias(feature_values["vp_auction_regime"])
    feature_values["vp_trade_bias_actionable"] = resolve_vp_trade_bias_actionable(
        feature_values["vp_trade_bias"]
    )
    feature_values["vp_trade_bias_confidence"] = resolve_vp_trade_bias_confidence(
        feature_values["vp_auction_regime"],
        feature_values["vp_trade_bias_actionable"],
    )
    feature_values["vp_trade_bias_score"] = resolve_vp_trade_bias_score(
        feature_values["vp_trade_bias_confidence"]
    )
    feature_values["vp_policy_candidate"] = resolve_vp_policy_candidate(
        feature_values["vp_trade_bias_actionable"],
        feature_values["vp_trade_bias_score"],
    )
    feature_values["vp_policy_side"] = resolve_vp_policy_side(
        feature_values["vp_trade_bias"],
        feature_values["vp_policy_candidate"],
    )
    feature_values["vp_trade_bias_summary"] = resolve_vp_trade_bias_summary(
        feature_values["vp_auction_regime"],
        feature_values["vp_trade_bias"],
        feature_values["vp_trade_bias_confidence"],
        feature_values["vp_trade_bias_score"],
    )
    feature_values["vp_policy_reason"] = resolve_vp_policy_reason(
        feature_values["vp_policy_side"],
        feature_values["vp_trade_bias_summary"],
        feature_values["vp_policy_candidate"],
    )

    regime_output = classify_regime(feature_values)
    model_output = score_state(feature_values)
    regime_id_raw = regime_output.get("regime_id")
    regime_id = str(regime_id_raw) if regime_id_raw is not None else None

    snapshot_id = insert_feature_snapshot(
        timestamp=timestamp,
        symbol=symbol,
        timeframe=timeframe,
        source_bar_id=source_bar_id,
        feature_version=FEATURE_VERSION,
        feature_values=feature_values,
        regime_output=regime_output,
        model_output=model_output,
    )

    insert_regime_state(
        timestamp=timestamp,
        symbol=symbol,
        regime_id=regime_id,
        regime_confidence=regime_output.get("regime_confidence"),
        transition_risk=regime_output.get("transition_risk"),
    )

    insert_model_prediction(
        timestamp=timestamp,
        symbol=symbol,
        long_probability=model_output.get("long_probability"),
        short_probability=model_output.get("short_probability"),
        no_trade_probability=model_output.get("no_trade_probability"),
        expected_excursion=model_output.get("expected_excursion"),
        setup_trust_score=model_output.get("setup_trust_score"),
    )

    attach_feature_snapshot_to_bar_state(
        bar_state_id=source_bar_id,
        snapshot_id=snapshot_id,
        feature_values=feature_values,
        regime_output=regime_output,
        model_output=model_output,
    )

    return {
        "snapshot_id": snapshot_id,
        "feature_values": feature_values,
        "regime": regime_output,
        "model": model_output,
    }
