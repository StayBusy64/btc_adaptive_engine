"""Tests for cohort_scoring_engine, decay_engine, governance_engine, and feedback_engine.

Covers:
  - Cohort key construction
  - Outcome classification (win / loss / scratch)
  - Journal-outcome joining
  - Windowed filtering
  - Cohort grouping
  - Metric computation (win_rate, avg_pnl, continuation, reversion)
  - Score assignment (quality, promotion, decay, confidence)
  - Feature correlation proxy
  - Lifecycle state machine transitions (observe → candidate → promote → decay → retire)
  - Decay engine (time, performance, cohort, contradiction, composite)
  - Governance engine (sample check, decay block, marginal quality, approved gate)
  - Feedback engine (best/worst release, self_trust, feature lists)
"""
from __future__ import annotations

import math
from typing import Any, Dict, List

import pytest

from backend.cohort_scoring_engine import (
    CANDIDATE_CORRELATION_THRESHOLD,
    DECAY_QUALITY_THRESHOLD,
    DEMOTION_MIN_SAMPLES,
    HORIZON_WINDOWS,
    LOSS_THRESHOLD_PCT,
    PROMOTE_QUALITY_THRESHOLD,
    PROMOTION_MIN_SAMPLES,
    RETIRE_QUALITY_THRESHOLD,
    WIN_THRESHOLD_PCT,
    _build_cohort_key,
    _classify_outcome,
    _compute_cohort_metrics,
    _compute_confidence_score,
    _compute_decay_score,
    _compute_feature_correlation,
    _compute_promotion_score,
    _compute_quality_score,
    _filter_by_window,
    _group_by_cohort,
    _join_journal_outcomes,
    _resolve_lifecycle_transition,
    _safe_float,
    _safe_int,
)
from backend.decay_engine import (
    apply_cohort_decay,
    apply_contradiction_decay,
    apply_performance_decay,
    apply_time_decay,
    compute_signal_weight,
)
from backend.feedback_engine import summarize_state
from backend.governance_engine import govern


# ── Helpers ──────────────────────────────────────────────────────────

def _make_journal_row(
    signal_id: str = "sig-1",
    *,
    release_id: str = "bridge_v2",
    release_version: str = "2.1.0",
    release_channel: str = "production",
    strategy_id: str = "bridge_signal_sender_v2",
    symbol: str = "BTCUSDT",
    side: str = "buy",
    event_time_ms: int = 1_700_000_000_000,
    research_context: dict | None = None,
) -> Dict[str, Any]:
    return {
        "signal_id": signal_id,
        "release_id": release_id,
        "release_version": release_version,
        "release_channel": release_channel,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "side": side,
        "event_time_ms": event_time_ms,
        "research_context": research_context or {},
    }


def _make_outcome_row(
    signal_id: str = "sig-1",
    *,
    signed_move_pct_5bar: float = 0.15,
    mfe_pct: float = 0.20,
    mae_pct: float = -0.05,
    continuation_strength: float = 0.3,
    reversion_strength: float = 0.1,
    continuation_hit_5bar: bool = True,
    reversion_hit_5bar: bool = False,
) -> Dict[str, Any]:
    return {
        "signal_id": signal_id,
        "signed_move_pct_5bar": signed_move_pct_5bar,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "continuation_strength": continuation_strength,
        "reversion_strength": reversion_strength,
        "continuation_hit_5bar": continuation_hit_5bar,
        "reversion_hit_5bar": reversion_hit_5bar,
    }


def _make_joined_rows(count: int, *, wins: int, losses: int, **kw) -> List[Dict[str, Any]]:
    """Build `count` joined rows: `wins` wins, `losses` losses, rest scratch."""
    rows: List[Dict[str, Any]] = []
    scratches = count - wins - losses
    idx = 0
    for i in range(wins):
        j = _make_journal_row(f"sig-w{i}", event_time_ms=1_700_000_000_000 + idx, **kw)
        o = _make_outcome_row(f"sig-w{i}", signed_move_pct_5bar=0.20)
        j["outcome"] = o
        j["outcome_label"] = "win"
        rows.append(j)
        idx += 1
    for i in range(losses):
        j = _make_journal_row(f"sig-l{i}", event_time_ms=1_700_000_000_000 + idx, **kw)
        o = _make_outcome_row(f"sig-l{i}", signed_move_pct_5bar=-0.20)
        j["outcome"] = o
        j["outcome_label"] = "loss"
        rows.append(j)
        idx += 1
    for i in range(scratches):
        j = _make_journal_row(f"sig-s{i}", event_time_ms=1_700_000_000_000 + idx, **kw)
        o = _make_outcome_row(f"sig-s{i}", signed_move_pct_5bar=0.02)
        j["outcome"] = o
        j["outcome_label"] = "scratch"
        rows.append(j)
        idx += 1
    return rows


# ═══════════════════════════════════════════════════════════════════════
# 1. COHORT SCORING ENGINE - UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════


class TestSafeConversions:
    def test_safe_float_valid(self):
        assert _safe_float(3.14) == 3.14
        assert _safe_float("2.5") == 2.5
        assert _safe_float(0) == 0.0

    def test_safe_float_none(self):
        assert _safe_float(None) is None

    def test_safe_float_invalid(self):
        assert _safe_float("abc") is None

    def test_safe_int_valid(self):
        assert _safe_int(42) == 42
        assert _safe_int("7") == 7

    def test_safe_int_none(self):
        assert _safe_int(None) is None


class TestBuildCohortKey:
    def test_full_key(self):
        key = _build_cohort_key(
            release_id="bridge_v2",
            release_version="2.1.0",
            release_channel="production",
            strategy_id="strat",
            symbol="BTCUSDT",
            side="buy",
        )
        assert key == "bridge_v2|2.1.0|production|strat|BTCUSDT|buy"

    def test_missing_fields_use_underscore(self):
        key = _build_cohort_key(
            release_id=None,
            release_version=None,
            release_channel=None,
            strategy_id=None,
            symbol=None,
            side=None,
        )
        assert key == "_|_|_|_|_|_"

    def test_partial_fields(self):
        key = _build_cohort_key(
            release_id="v2",
            release_version=None,
            release_channel="canary",
            strategy_id=None,
            symbol="BTCUSDT",
            side=None,
        )
        assert key == "v2|_|canary|_|BTCUSDT|_"


class TestClassifyOutcome:
    def test_win(self):
        assert _classify_outcome({"signed_move_pct_5bar": 0.15}) == "win"

    def test_loss(self):
        assert _classify_outcome({"signed_move_pct_5bar": -0.15}) == "loss"

    def test_scratch_middle(self):
        assert _classify_outcome({"signed_move_pct_5bar": 0.05}) == "scratch"

    def test_scratch_boundary_positive(self):
        assert _classify_outcome({"signed_move_pct_5bar": 0.09}) == "scratch"

    def test_win_boundary(self):
        assert _classify_outcome({"signed_move_pct_5bar": WIN_THRESHOLD_PCT}) == "win"

    def test_loss_boundary(self):
        assert _classify_outcome({"signed_move_pct_5bar": LOSS_THRESHOLD_PCT}) == "loss"

    def test_none_value(self):
        assert _classify_outcome({"signed_move_pct_5bar": None}) == "scratch"

    def test_missing_key(self):
        assert _classify_outcome({}) == "scratch"


class TestJoinJournalOutcomes:
    def test_matching_join(self):
        journals = [_make_journal_row("sig-1"), _make_journal_row("sig-2")]
        outcomes = [_make_outcome_row("sig-1"), _make_outcome_row("sig-2")]
        joined = _join_journal_outcomes(journals, outcomes)
        assert len(joined) == 2
        assert all("outcome" in j for j in joined)
        assert all("outcome_label" in j for j in joined)

    def test_unmatched_journal_dropped(self):
        journals = [_make_journal_row("sig-1"), _make_journal_row("sig-no-match")]
        outcomes = [_make_outcome_row("sig-1")]
        joined = _join_journal_outcomes(journals, outcomes)
        assert len(joined) == 1
        assert joined[0]["signal_id"] == "sig-1"

    def test_empty_inputs(self):
        assert _join_journal_outcomes([], []) == []
        assert _join_journal_outcomes([_make_journal_row()], []) == []

    def test_outcome_label_assigned(self):
        journals = [_make_journal_row("s1")]
        outcomes = [_make_outcome_row("s1", signed_move_pct_5bar=-0.50)]
        joined = _join_journal_outcomes(journals, outcomes)
        assert joined[0]["outcome_label"] == "loss"


class TestFilterByWindow:
    def test_truncates_to_window(self):
        rows = [
            {"event_time_ms": 1000 + i, "data": i}
            for i in range(100)
        ]
        filtered = _filter_by_window(rows, 10)
        assert len(filtered) == 10
        # Should keep the 10 most recent (highest event_time_ms)
        assert filtered[0]["event_time_ms"] == 1099
        assert filtered[-1]["event_time_ms"] == 1090

    def test_fewer_rows_than_window(self):
        rows = [{"event_time_ms": i} for i in range(5)]
        assert len(_filter_by_window(rows, 60)) == 5

    def test_empty(self):
        assert _filter_by_window([], 60) == []


class TestGroupByCohort:
    def test_single_group(self):
        rows = _make_joined_rows(5, wins=3, losses=1)
        groups = _group_by_cohort(rows)
        assert len(groups) == 1
        key = list(groups.keys())[0]
        assert "bridge_v2" in key
        assert len(groups[key]) == 5

    def test_multiple_groups(self):
        rows_a = _make_joined_rows(3, wins=2, losses=0, release_version="2.0.0")
        rows_b = _make_joined_rows(2, wins=0, losses=2, release_version="2.1.0")
        groups = _group_by_cohort(rows_a + rows_b)
        assert len(groups) == 2


class TestComputeCohortMetrics:
    def test_all_wins(self):
        rows = _make_joined_rows(10, wins=10, losses=0)
        m = _compute_cohort_metrics(rows)
        assert m["sample_count"] == 10
        assert m["win_count"] == 10
        assert m["loss_count"] == 0
        assert m["scratch_count"] == 0
        assert m["win_rate"] == 1.0

    def test_all_losses(self):
        rows = _make_joined_rows(5, wins=0, losses=5)
        m = _compute_cohort_metrics(rows)
        assert m["win_rate"] == 0.0
        assert m["loss_count"] == 5

    def test_mixed(self):
        rows = _make_joined_rows(10, wins=6, losses=2)
        m = _compute_cohort_metrics(rows)
        assert m["win_count"] == 6
        assert m["loss_count"] == 2
        assert m["scratch_count"] == 2
        assert m["win_rate"] == pytest.approx(0.6)

    def test_pnl_average(self):
        rows = _make_joined_rows(2, wins=1, losses=1)
        m = _compute_cohort_metrics(rows)
        # win=0.20, loss=-0.20 → avg = 0.0
        assert m["avg_pnl_pct"] == pytest.approx(0.0)


class TestScoreAssignment:
    def test_quality_score(self):
        metrics = {"avg_pnl_pct": 0.10, "win_rate": 0.60}
        assert _compute_quality_score(metrics) == pytest.approx(0.06)

    def test_quality_score_none(self):
        assert _compute_quality_score({"avg_pnl_pct": None, "win_rate": None}) == 0.0

    def test_promotion_score_high_win_rate(self):
        metrics = {
            "win_rate": 0.80,
            "avg_continuation_strength": 0.50,
            "continuation_hit_rate": 0.60,
            "sample_count": PROMOTION_MIN_SAMPLES,
        }
        score = _compute_promotion_score(metrics)
        # (0.80*0.4 + 0.60*0.3 + 0.50*0.3) * 1.0 = 0.32 + 0.18 + 0.15 = 0.65
        assert score == pytest.approx(0.65)

    def test_promotion_score_low_samples(self):
        metrics = {
            "win_rate": 0.80,
            "avg_continuation_strength": 0.50,
            "continuation_hit_rate": 0.60,
            "sample_count": PROMOTION_MIN_SAMPLES // 2,
        }
        full_score = _compute_promotion_score({**metrics, "sample_count": PROMOTION_MIN_SAMPLES})
        half_score = _compute_promotion_score(metrics)
        assert half_score == pytest.approx(full_score * 0.5)

    def test_decay_score_all_losses(self):
        metrics = {
            "win_rate": 0.0,
            "avg_reversion_strength": 0.80,
            "avg_mae_pct": -0.50,
            "sample_count": DEMOTION_MIN_SAMPLES,
        }
        score = _compute_decay_score(metrics)
        # (1.0*0.4 + 0.80*0.3 + 0.50*0.3) * 1.0 = 0.40 + 0.24 + 0.15 = 0.79
        assert score == pytest.approx(0.79)

    def test_confidence_score_zero_samples(self):
        assert _compute_confidence_score({"sample_count": 0}) == 0.0

    def test_confidence_score_max(self):
        assert _compute_confidence_score({"sample_count": PROMOTION_MIN_SAMPLES * 2}) == 1.0

    def test_confidence_score_fractional(self):
        half = PROMOTION_MIN_SAMPLES // 2
        assert _compute_confidence_score({"sample_count": half}) == pytest.approx(
            half / PROMOTION_MIN_SAMPLES
        )


class TestFeatureCorrelation:
    def _rows_with_research(self, n: int, winner_val: float, loser_val: float) -> list:
        """Build n joined rows: half wins, half losses, with a research field."""
        rows = []
        half = n // 2
        for i in range(half):
            r = _make_journal_row(f"fw{i}", research_context={"test_field": winner_val})
            r["outcome"] = _make_outcome_row(f"fw{i}", signed_move_pct_5bar=0.20, continuation_strength=0.5)
            r["outcome_label"] = "win"
            rows.append(r)
        for i in range(half):
            r = _make_journal_row(f"fl{i}", research_context={"test_field": loser_val})
            r["outcome"] = _make_outcome_row(f"fl{i}", signed_move_pct_5bar=-0.20, continuation_strength=0.01)
            r["outcome_label"] = "loss"
            rows.append(r)
        return rows

    def test_positive_correlation(self):
        rows = self._rows_with_research(20, winner_val=0.8, loser_val=0.2)
        count, corr_win, _, avg_w, avg_l = _compute_feature_correlation("test_field", rows)
        assert count == 20
        assert corr_win is not None and corr_win > 0
        assert avg_w == pytest.approx(0.8)
        assert avg_l == pytest.approx(0.2)

    def test_no_values_returns_zeros(self):
        rows = [_make_journal_row("x", research_context={})]
        rows[0]["outcome"] = _make_outcome_row("x")
        rows[0]["outcome_label"] = "win"
        count, corr_win, _, _, _ = _compute_feature_correlation("missing_field", rows)
        assert count == 0
        assert corr_win is None


class TestLifecycleTransitions:
    """Test the lifecycle state machine: observe → candidate → promote → decay → retire."""

    def test_observe_stays_without_enough_data(self):
        assert _resolve_lifecycle_transition("observe", 5, 0.1, 0.1, 0.05) == "observe"

    def test_observe_to_candidate(self):
        result = _resolve_lifecycle_transition(
            "observe",
            PROMOTION_MIN_SAMPLES,
            0.2,
            0.1,
            CANDIDATE_CORRELATION_THRESHOLD + 0.01,
        )
        assert result == "candidate"

    def test_candidate_to_promote(self):
        result = _resolve_lifecycle_transition(
            "candidate",
            PROMOTION_MIN_SAMPLES,
            PROMOTE_QUALITY_THRESHOLD,
            0.1,
            0.5,
        )
        assert result == "promote"

    def test_candidate_to_decay(self):
        result = _resolve_lifecycle_transition(
            "candidate",
            DEMOTION_MIN_SAMPLES,
            0.01,
            0.65,
            0.5,
        )
        assert result == "decay"

    def test_promote_to_decay(self):
        result = _resolve_lifecycle_transition(
            "promote",
            DEMOTION_MIN_SAMPLES,
            0.01,
            0.75,
            0.5,
        )
        assert result == "decay"

    def test_promote_stays_with_low_decay(self):
        result = _resolve_lifecycle_transition("promote", 50, 0.5, 0.3, 0.5)
        assert result == "promote"

    def test_decay_to_candidate_recovery(self):
        result = _resolve_lifecycle_transition(
            "decay",
            PROMOTION_MIN_SAMPLES,
            PROMOTE_QUALITY_THRESHOLD,
            0.3,
            0.5,
        )
        assert result == "candidate"

    def test_decay_to_retire(self):
        result = _resolve_lifecycle_transition(
            "decay",
            DEMOTION_MIN_SAMPLES,
            0.01,
            0.85,
            -0.2,
        )
        assert result == "retire"

    def test_retire_is_terminal(self):
        assert _resolve_lifecycle_transition("retire", 100, 0.9, 0.0, 0.9) == "retire"


# ═══════════════════════════════════════════════════════════════════════
# 2. DECAY ENGINE - UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════


class TestTimeDecay:
    def test_zero_time_no_decay(self):
        assert apply_time_decay(1.0, 0.1, 0.0) == 1.0

    def test_positive_time_decays(self):
        assert apply_time_decay(1.0, 0.1, 10.0) < 1.0

    def test_decay_is_exponential(self):
        w1 = apply_time_decay(1.0, 0.1, 5.0)
        w2 = apply_time_decay(1.0, 0.1, 10.0)
        assert w2 < w1

    def test_high_rate_decays_faster(self):
        slow = apply_time_decay(1.0, 0.01, 10.0)
        fast = apply_time_decay(1.0, 0.10, 10.0)
        assert fast < slow


class TestPerformanceDecay:
    def test_zero_penalty(self):
        assert apply_performance_decay(1.0, 0.0) == 1.0

    def test_full_penalty(self):
        assert apply_performance_decay(1.0, 1.0) == 0.0

    def test_floors_at_zero(self):
        assert apply_performance_decay(1.0, 2.0) == 0.0

    def test_partial_penalty(self):
        assert apply_performance_decay(1.0, 0.5) == pytest.approx(0.5)


class TestCohortDecay:
    def test_zero_decay_score(self):
        assert apply_cohort_decay(1.0, 0.0, 1.0) == 1.0

    def test_full_decay_full_confidence(self):
        assert apply_cohort_decay(1.0, 1.0, 1.0) == 0.0

    def test_high_decay_low_confidence(self):
        # decay_score=0.8, confidence=0.25 → effective_penalty=0.2 → weight=0.8
        assert apply_cohort_decay(1.0, 0.8, 0.25) == pytest.approx(0.8)

    def test_floors_at_zero(self):
        assert apply_cohort_decay(0.5, 2.0, 1.0) == 0.0


class TestContradictionDecay:
    def test_no_contradiction(self):
        w = apply_contradiction_decay(1.0, 2.0, 10, 0, has_contradiction=False)
        assert w == 1.0  # bars_since=0 → exp(0) = 1.0

    def test_contradiction_shortens_halflife(self):
        w_no = apply_contradiction_decay(1.0, 2.0, 10, 5, has_contradiction=False)
        w_yes = apply_contradiction_decay(1.0, 2.0, 10, 5, has_contradiction=True)
        # With contradiction, half-life = 10/2 = 5 → faster decay
        assert w_yes < w_no

    def test_zero_halflife_returns_zero(self):
        assert apply_contradiction_decay(1.0, 2.0, 0, 5, has_contradiction=False) == 0.0

    def test_at_halflife_near_half(self):
        # At bars_since = half_life, weight ≈ 0.5 (exp(-ln2) = 0.5)
        w = apply_contradiction_decay(1.0, 2.0, 10, 10, has_contradiction=False)
        assert w == pytest.approx(0.5, abs=0.01)


class TestCompositeSignalWeight:
    def test_default_no_decay(self):
        result = compute_signal_weight(bars_elapsed=0.0)
        assert result["final_weight"] == pytest.approx(1.0, abs=0.01)

    def test_time_decay_applied(self):
        result = compute_signal_weight(bars_elapsed=100.0, time_decay_rate=0.01)
        assert result["after_time_decay"] < 1.0
        assert result["final_weight"] < result["after_time_decay"]  # contradiction also applying

    def test_cohort_score_reduces_weight(self):
        cohort = {"decay_score": 0.5, "confidence_score": 1.0}
        result = compute_signal_weight(
            bars_elapsed=0.0,
            cohort_score=cohort,
            half_life_bars=9999,
        )
        assert result["after_cohort_decay"] < result["after_performance_decay"]
        assert result["cohort_penalty"] == pytest.approx(0.5)

    def test_all_dimensions(self):
        result = compute_signal_weight(
            base_weight=1.0,
            time_decay_rate=0.001,
            bars_elapsed=10.0,
            performance_penalty=0.1,
            cohort_score={"decay_score": 0.2, "confidence_score": 0.8},
            contradiction_active=True,
            contradiction_multiplier=2.0,
            half_life_bars=20,
        )
        # Every stage should reduce the weight
        assert result["after_time_decay"] < result["base_weight"]
        assert result["after_performance_decay"] < result["after_time_decay"]
        assert result["after_cohort_decay"] < result["after_performance_decay"]
        assert result["final_weight"] < result["after_cohort_decay"]
        assert result["contradiction_active"] is True

    def test_observability_keys(self):
        result = compute_signal_weight()
        expected_keys = {
            "final_weight", "base_weight", "after_time_decay",
            "after_performance_decay", "after_cohort_decay",
            "cohort_penalty", "contradiction_active", "bars_elapsed",
        }
        assert expected_keys == set(result.keys())


# ═══════════════════════════════════════════════════════════════════════
# 3. GOVERNANCE ENGINE - UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════


class TestGovernanceEngine:
    def test_no_cohort_score_blocks(self):
        result = govern({})
        assert result["allow_trade"] is False
        assert result["observe_only"] is True
        assert result["governance_reason"] == "no_cohort_score"

    def test_insufficient_samples_blocks(self):
        result = govern({}, cohort_score={"sample_count": 5, "promotion_score": 0.5})
        assert result["allow_trade"] is False
        assert result["governance_reason"] == "insufficient_cohort_samples"

    def test_high_decay_blocks(self):
        cohort = {
            "sample_count": 50,
            "decay_score": 0.8,
            "confidence_score": 0.9,
            "quality_score": 0.10,
            "promotion_score": 0.5,
        }
        result = govern({}, cohort_score=cohort)
        assert result["allow_trade"] is False
        assert result["governance_reason"] == "cohort_decay_block"

    def test_marginal_quality_observe_only(self):
        cohort = {
            "sample_count": 50,
            "decay_score": 0.3,
            "confidence_score": 0.7,
            "quality_score": 0.02,
            "promotion_score": 0.4,
        }
        result = govern({}, cohort_score=cohort)
        assert result["allow_trade"] is False
        assert result["observe_only"] is True
        assert result["governance_reason"] == "marginal_quality"

    def test_approved_cohort(self):
        cohort = {
            "sample_count": 80,
            "decay_score": 0.2,
            "confidence_score": 0.9,
            "quality_score": 0.15,
            "promotion_score": 0.6,
        }
        result = govern({}, cohort_score=cohort)
        # confidence_cap = min(1.0, 0.6 * 0.9) = 0.54 → allow_trade
        assert result["allow_trade"] is True
        assert result["governance_reason"] == "cohort_approved"
        assert result["confidence_cap"] == pytest.approx(0.54)

    def test_feature_lifecycle_penalty(self):
        cohort = {
            "sample_count": 80,
            "decay_score": 0.2,
            "confidence_score": 0.9,
            "quality_score": 0.15,
            "promotion_score": 0.6,
        }
        # 2 out of 4 features in decay/retire → penalty = 0.5
        lifecycle = {
            "f1": {"current_status": "promote"},
            "f2": {"current_status": "decay"},
            "f3": {"current_status": "observe"},
            "f4": {"current_status": "retire"},
        }
        result = govern({}, cohort_score=cohort, feature_lifecycle=lifecycle)
        # adjusted_cap = 0.54 * (1 - 0.5 * 0.5) = 0.54 * 0.75 = 0.405
        assert result["confidence_cap"] == pytest.approx(0.405)
        assert result["active_feature_penalty"] == pytest.approx(0.5)


# ═══════════════════════════════════════════════════════════════════════
# 4. FEEDBACK ENGINE - UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════


class TestFeedbackEngine:
    def test_no_cohort_scores(self):
        result = summarize_state({"regime_id": "range"})
        assert result["current_regime"] == "range"
        assert result["self_trust_score"] == 0.0

    def test_with_cohort_scores(self):
        scores = [
            {
                "release_version": "2.0.0",
                "release_channel": "production",
                "quality_score": 0.10,
                "win_rate": 0.60,
                "sample_count": 50,
            },
            {
                "release_version": "2.1.0",
                "release_channel": "canary",
                "quality_score": -0.05,
                "win_rate": 0.30,
                "sample_count": 30,
            },
        ]
        result = summarize_state({"regime_id": "trend"}, cohort_scores=scores)
        assert result["best_release"]["release_version"] == "2.0.0"
        assert result["worst_release"]["release_version"] == "2.1.0"
        assert result["self_trust_score"] > 0

    def test_feature_lifecycle_lists(self):
        scores = [
            {"release_version": "v1", "release_channel": "prod",
             "quality_score": 0.1, "win_rate": 0.5, "sample_count": 20},
        ]
        lifecycle = [
            {"feature_key": "trend_slope_score", "current_status": "promote"},
            {"feature_key": "mean_reversion_risk", "current_status": "decay"},
            {"feature_key": "regime_bias_score", "current_status": "candidate"},
        ]
        result = summarize_state(
            {"regime_id": "trend"},
            cohort_scores=scores,
            feature_lifecycle=lifecycle,
        )
        assert "trend_slope_score" in result["promoted_features"]
        assert "mean_reversion_risk" in result["decaying_features"]
        assert "regime_bias_score" in result["candidate_features"]

    def test_self_trust_bounded(self):
        scores = [
            {"release_version": "v1", "release_channel": "prod",
             "quality_score": 5.0, "win_rate": 1.0, "sample_count": 100},
        ]
        result = summarize_state({}, cohort_scores=scores)
        assert result["self_trust_score"] <= 1.0

    def test_filters_small_samples(self):
        scores = [
            {"release_version": "v1", "release_channel": "prod",
             "quality_score": 0.50, "win_rate": 1.0, "sample_count": 5},  # < 10
        ]
        result = summarize_state({}, cohort_scores=scores)
        # All filtered → no best/worst
        assert "best_release" not in result
