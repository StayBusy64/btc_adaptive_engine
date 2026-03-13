import csv
import io
import json
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, Literal, Optional

from fastapi import Body, FastAPI, Header, HTTPException, Path, Query, Request, Response, status
from fastapi.openapi.models import Example
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.feature_engine import run_feature_pipeline_for_latest_bar
from backend.normalization_service import normalize_tradingview_alert
from backend.tradingview_ingest_cycle import (
    replay_batch_once,
    run_ingest_cycle_once,
    start_ingest_cycle_scheduler,
    stop_ingest_cycle_scheduler,
)
from backend.tradingview_ingest_models import (
    TradingViewBatchPayload,
    TradingViewCycleSummary,
    TradingViewIngestAcceptResponse,
)
from backend.tradingview_ingest_service import (
    accept_tradingview_batch,
    get_batch_by_id,
    get_event_by_id,
    get_recent_batch_rows,
    get_recent_event_rows,
)
from backend.tradingview_ingest_storage import ensure_ingest_directories
from backend.signal_outcome_engine import (
    backfill_signal_snapshots_from_storage,
    get_outcome_engine_defaults,
    get_recent_signal_journal_rows,
    get_recent_signal_outcome_rows,
    run_signal_outcome_evaluation_once,
)
from backend.market_bias_engine import (
    compute_and_store_signal_bias,
    compute_market_bias_preview_for_normalized_signal,
    get_recent_market_bias_rows,
)

from backend.cohort_scoring_engine import (
    compare_release_cohorts,
    get_cohort_leaderboard,
    get_cohort_score_history,
    get_cohort_scores,
    get_feature_lifecycle_status,
    run_cohort_scoring,
)

from backend.event_writer import (
    apply_fill_to_positions,
    claim_next_trade_candidate,
    get_broker_order_by_execution_request_id,
    heartbeat_trade_candidate_claim,
    get_fill_by_order_id,
    get_execution_journal_analytics,
    get_execution_journal_daily_rollup,
    get_bar_state_by_id,
    get_recent_broker_orders,
    get_recent_feature_snapshots,
    get_recent_fills,
    get_recent_positions,
    get_recent_volume_profile_snapshots,
    get_execution_outcomes_policy_audit,
    get_execution_outcomes_policy_audit_summary,
    get_execution_outcomes_compare,
    get_execution_outcomes_leaderboard,
    get_execution_outcomes_policy_matrix,
    get_execution_outcomes_policy_recommendation,
    get_execution_outcomes_scorecard,
    get_execution_outcomes_summary,
    get_execution_outcomes_vp_policy_cohorts,
    get_execution_outcomes_vp_policy_reason_cohorts,
    get_execution_outcomes_vp_policy_reason_laggards,
    get_execution_outcomes_vp_policy_reason_leaderboard,
    get_execution_outcomes_vp_policy_summary,
    get_execution_journal_summary,
    get_execution_journal_timeline,
    get_recent_execution_outcomes,
    get_recent_execution_requests,
    get_recent_bar_states,
    get_recent_execution_journal,
    get_recent_normalized_signals,
    get_recent_raw_webhook_events,
    get_recent_strategy_risk_decisions,
    get_recent_trade_candidates,
    get_trade_candidate_execution_summary,
    init_db,
    insert_bar_state,
    BrokerOrderParams,
    insert_broker_order,
    insert_fill_event,
    insert_execution_request,
    insert_normalized_signal,
    insert_raw_webhook_event,
    insert_risk_event,
    insert_strategy_decision,
    insert_trade_candidate,
    insert_trade_candidate_from_event,
    is_database_reachable,
    release_trade_candidate_claim,
    update_trade_candidate_status,
)

logger = logging.getLogger(__name__)
EMPTY_FIELD_ERROR = "must not be empty"
EMPTY_OPTIONAL_FIELD_ERROR = "must not be empty when provided"
EXAMPLE_SYMBOL = "BTCUSDT.P"
EXAMPLE_SYMBOL_ALT = "ETHUSDT.P"
EXAMPLE_CREATED_TIMESTAMP = "2026-03-10T16:15:00+00:00"
EXAMPLE_EVALUATED_AT = "2026-03-10T16:47:00+00:00"
EXAMPLE_LATEST_EVALUATED_AT = "2026-03-10T17:00:00+00:00"
EXAMPLE_SIMULATION_META = '{"simulation": true}'
CSV_MEDIA_TYPE = "text/csv; charset=utf-8"
DESC_COUNT_BEST = "Count mirror for best rows"
DESC_COUNT_WORST = "Count mirror for worst rows"
DESC_ROWS_BEST = "Best VP policy reason cohorts"
DESC_ROWS_WORST = "Worst VP policy reason cohorts"
RECENT_FEATURE_SNAPSHOT_CONTEXT_KEYS = (
    "vp_failed_auction_above",
    "vp_failed_auction_below",
    "vp_reversion_to_value_from_above_context",
    "vp_reversion_to_value_from_below_context",
    "vp_acceptance_outside_value_above",
    "vp_acceptance_outside_value_below",
    "vp_continuation_auction_up",
    "vp_continuation_auction_down",
    "vp_auction_regime",
    "vp_trade_bias",
    "vp_trade_bias_actionable",
    "vp_trade_bias_confidence",
    "vp_trade_bias_score",
    "vp_policy_candidate",
    "vp_policy_side",
    "vp_trade_bias_summary",
    "vp_policy_reason",
)
EXECUTION_JOURNAL_DECISION_CONTEXT_KEYS = (
    "vp_policy_candidate",
    "vp_policy_side",
    "vp_trade_bias_score",
    "vp_policy_reason",
)
EXECUTION_OUTCOME_VP_POLICY_CONTEXT_KEYS = (
    "vp_policy_candidate",
    "vp_policy_side",
    "vp_trade_bias_score",
    "vp_policy_reason",
)
VP_POLICY_FILTER_SCORE_ECHO_DESCRIPTION = "Echoed vp_trade_bias_score filter"
VP_POLICY_FILTER_SIDE_ECHO_DESCRIPTION = "Echoed vp_policy_side filter"
VP_POLICY_FILTER_SINCE_DAYS_ECHO_DESCRIPTION = "Echoed rolling window size in days, anchored to latest stored evaluated_at"
VP_POLICY_FILTER_SINCE_TRADES_ECHO_DESCRIPTION = "Echoed rolling window size in most recent trades"
VP_POLICY_FILTERED_FLAG_DESCRIPTION = "True when score or side filters are applied"
ALLOWED_EXECUTION_STATUSES = ("pending", "submitted", "filled", "rejected", "skipped")
OUTCOME_GROUP_BY_VALUES = ("strategy", "source", "setup_family", "worker_id", "symbol", "direction")
OUTCOME_POLICY_SCORING_MODES = ("expectancy_pct", "expectancy_points", "win_rate", "avg_pnl_pct", "blended")
OUTCOME_POLICY_MATRIX_GROUPS = ("strategy", "source", "setup_family", "worker_id")
RECENT_TRADE_CANDIDATES_DESCRIPTION = (
    "Return newest trade candidates first. "
    "Example filters: "
    f"`/trade_candidates/recent?symbol={EXAMPLE_SYMBOL}`, "
    "`/trade_candidates/recent?direction=long`, "
    f"`/trade_candidates/recent?symbol={EXAMPLE_SYMBOL}&timeframe=1m&derived_from_event=true`."
)

TRADE_CANDIDATE_REQUEST_EXAMPLES: dict[str, Example] = {
    "momentumLong": Example(
        summary="Momentum long candidate",
        value={
            "signal_id": "sig-btc-20260310-001",
            "timestamp": "2026-03-10T16:15:00Z",
            "symbol": EXAMPLE_SYMBOL,
            "direction": "long",
            "entry_price": 82420.0,
            "stop_price": 82190.0,
            "tp1": 82610.0,
            "tp2": 82840.0,
            "confidence": 0.82,
            "setup_family": "momentum",
            "payload_json": {
                "timeframe": "1m",
                "strategy": "adaptive-v2",
                "source": "manual-webhook",
                "event_type": "continuation",
            },
        },
    )
}

TRADE_CANDIDATE_FROM_EVENT_EXAMPLES: dict[str, Example] = {
    "webhookEvent": Example(
        summary="Derived from webhook event",
        value={
            "symbol": EXAMPLE_SYMBOL,
            "timeframe": "1m",
            "side": "buy",
            "price": 82420.0,
            "event_type": "continuation",
            "strategy": "adaptive-v2",
            "source": "webhook",
            "confidence": 0.8,
            "timestamp": "2026-03-10T16:00:00Z",
            "payload_json": {
                "event_id": "evt-001"
            },
        },
    )
}

TRADE_CANDIDATE_CREATE_RESPONSE_EXAMPLE = {
    "status": "stored",
    "id": 101,
    "client": "127.0.0.1",
    "symbol": EXAMPLE_SYMBOL,
    "timestamp": EXAMPLE_CREATED_TIMESTAMP,
    "signal_id": "sig-btc-20260310-001",
    "execution_status": "pending",
}

TRADE_CANDIDATE_FROM_EVENT_RESPONSE_EXAMPLE = {
    "status": "stored",
    "id": 102,
    "client": "127.0.0.1",
    "symbol": EXAMPLE_SYMBOL,
    "timestamp": "2026-03-10T16:00:00+00:00",
    "signal_id": "BTCUSDT.P-long-20260310160000",
    "derived_from_event": True,
    "replayed": False,
}

TRADE_CANDIDATE_FROM_EVENT_DUPLICATE_RESPONSE_EXAMPLE = {
    "status": "duplicate",
    "id": 102,
    "client": "127.0.0.1",
    "symbol": EXAMPLE_SYMBOL,
    "timestamp": "2026-03-10T16:00:00+00:00",
    "signal_id": "BTCUSDT.P-long-20260310160000",
    "derived_from_event": True,
    "replayed": True,
}

TRADE_CANDIDATE_RECENT_RESPONSE_EXAMPLE = {
    "count": 1,
    "limit": 50,
    "rows": [
        {
            "id": 101,
            "signal_id": "sig-btc-20260310-001",
            "timestamp": EXAMPLE_CREATED_TIMESTAMP,
            "symbol": EXAMPLE_SYMBOL,
            "direction": "long",
            "entry_price": 82420.0,
            "stop_price": 82190.0,
            "tp1": 82610.0,
            "tp2": 82840.0,
            "confidence": 0.82,
            "setup_family": "momentum",
            "payload_json": "{\"timeframe\": \"1m\", \"strategy\": \"adaptive-v2\", \"source\": \"manual-webhook\", \"event_type\": \"continuation\"}",
            "execution_status": "pending",
            "execution_note": None,
            "executed_at": None,
        }
    ],
}

TRADE_CANDIDATE_STATUS_UPDATE_EXAMPLE = {
    "execution_status": "filled",
    "execution_note": "Order filled by exchange",
    "executed_at": "2026-03-10T16:30:00Z",
}

TRADE_CANDIDATE_STATUS_UPDATE_RESPONSE_EXAMPLE = {
    "status": "updated",
    "id": 101,
    "execution_status": "filled",
    "execution_note": "Order filled by exchange",
    "executed_at": "2026-03-10T16:30:00+00:00",
}

TRADE_CANDIDATE_CLAIM_RESPONSE_EXAMPLE = {
    "status": "claimed",
    "claim_token": "claim-token-example",
    "row": {
        "id": 101,
        "signal_id": "sig-btc-20260310-001",
        "timestamp": EXAMPLE_CREATED_TIMESTAMP,
        "symbol": EXAMPLE_SYMBOL,
        "direction": "long",
        "entry_price": 82420.0,
        "stop_price": 82190.0,
        "tp1": 82610.0,
        "tp2": 82840.0,
        "confidence": 0.82,
        "setup_family": "momentum",
        "payload_json": "{\"timeframe\": \"1m\", \"strategy\": \"adaptive-v2\", \"source\": \"manual-webhook\", \"event_type\": \"continuation\"}",
        "execution_status": "submitted",
        "execution_note": None,
        "executed_at": "2026-03-10T16:31:00+00:00",
        "claimed_by": "worker-a",
        "claim_token": "claim-token-example",
        "claimed_at": "2026-03-10T16:31:00+00:00",
    },
}

TRADE_CANDIDATE_CLAIM_EMPTY_RESPONSE_EXAMPLE = {
    "status": "empty",
    "row": None,
}

TRADE_CANDIDATE_SUMMARY_RESPONSE_EXAMPLE = {
    "pending": 4,
    "submitted": 2,
    "filled": 1,
    "rejected": 0,
    "skipped": 0,
    "leased_submitted_count": 2,
    "stale_submitted_count": 0,
    "total": 7,
}

TRADE_CANDIDATE_HEARTBEAT_EXAMPLE = {
    "worker_id": "worker-a",
    "claim_token": "claim-token-example",
}

TRADE_CANDIDATE_HEARTBEAT_RESPONSE_EXAMPLE = {
    "status": "ok",
    "id": 101,
    "claimed_by": "worker-a",
    "claim_token": "claim-token-example",
    "claimed_at": "2026-03-10T16:31:30+00:00",
}

TRADE_CANDIDATE_RELEASE_EXAMPLE = {
    "worker_id": "worker-a",
    "claim_token": "claim-token-example",
    "execution_status": "pending",
    "execution_note": "Returning candidate to queue",
}

TRADE_CANDIDATE_RELEASE_RESPONSE_EXAMPLE = {
    "status": "released",
    "id": 101,
    "execution_status": "pending",
    "execution_note": "Returning candidate to queue",
    "claimed_by": None,
    "claim_token": None,
    "claimed_at": None,
}

TRADE_CANDIDATE_CLAIM_REQUEST_EXAMPLE = {
    "worker_id": "worker-a",
}

EXECUTION_JOURNAL_RECENT_RESPONSE_EXAMPLE = {
    "count": 1,
    "limit": 50,
    "rows": [
        {
            "id": 1,
            "candidate_id": 101,
            "signal_id": "sig-btc-20260310-001",
            "worker_id": "worker-a",
            "action": "simulation_decision",
            "execution_status": "filled",
            "execution_note": "simulation_filled_confidence_0.820_gte_0.750",
            "confidence": 0.82,
            "symbol": EXAMPLE_SYMBOL,
            "direction": "long",
            "entry_price": 82420.0,
            "created_at": "2026-03-10T16:32:00+00:00",
            "metadata_json": EXAMPLE_SIMULATION_META,
        }
    ],
}

EXECUTION_JOURNAL_SUMMARY_RESPONSE_EXAMPLE = {
    "filled": 3,
    "skipped": 2,
    "rejected": 1,
    "other": 0,
    "worker_count": 2,
    "latest_created_at": "2026-03-10T16:35:00+00:00",
    "total": 6,
}

EXECUTION_JOURNAL_ANALYTICS_RESPONSE_EXAMPLE = {
    "total_decisions": 6,
    "filled_count": 3,
    "skipped_count": 2,
    "rejected_count": 1,
    "fill_rate": 0.5,
    "skip_rate": 0.3333333333,
    "reject_rate": 0.1666666667,
    "avg_confidence": 0.71,
    "avg_confidence_filled": 0.84,
    "avg_confidence_skipped": 0.49,
    "avg_confidence_rejected": 0.31,
    "by_symbol": {
        EXAMPLE_SYMBOL: 4,
        EXAMPLE_SYMBOL_ALT: 2,
    },
    "by_worker": {
        "worker-a": 4,
        "worker-b": 2,
    },
    "latest_created_at": "2026-03-10T16:35:00+00:00",
}

EXECUTION_JOURNAL_DAILY_ROLLUP_RESPONSE_EXAMPLE = {
    "count": 2,
    "rows": [
        {
            "day": "2026-03-10",
            "total": 4,
            "filled": 2,
            "skipped": 1,
            "rejected": 1,
        },
        {
            "day": "2026-03-09",
            "total": 2,
            "filled": 1,
            "skipped": 1,
            "rejected": 0,
        },
    ],
}

EXECUTION_JOURNAL_DAILY_ROLLUP_FIELDS = ["day", "total", "filled", "skipped", "rejected"]

EXECUTION_OUTCOMES_RECENT_RESPONSE_EXAMPLE = {
    "count": 1,
    "limit": 50,
    "rows": [
        {
            "id": 1,
            "journal_id": 1,
            "candidate_id": 101,
            "signal_id": "sig-btc-20260310-001",
            "worker_id": "worker-a",
            "symbol": EXAMPLE_SYMBOL,
            "direction": "long",
            "entry_price": 82420.0,
            "reference_timestamp": EXAMPLE_CREATED_TIMESTAMP,
            "evaluation_window_minutes": 15,
            "outcome_status": "evaluated",
            "exit_price": 82510.0,
            "pnl_points": 90.0,
            "pnl_pct": 0.109196797864596,
            "max_favorable_excursion": 130.0,
            "max_adverse_excursion": -35.0,
            "evaluated_at": EXAMPLE_EVALUATED_AT,
            "label": "winner",
            "metadata_json": EXAMPLE_SIMULATION_META,
        }
    ],
}

EXECUTION_OUTCOMES_SCORECARD_RESPONSE_EXAMPLE = {
    "total": 4,
    "evaluated_count": 3,
    "labeled_count": 3,
    "winner_count": 1,
    "loser_count": 1,
    "scratch_count": 1,
    "unknown_count": 1,
    "win_rate": 0.3333333333,
    "loss_rate": 0.3333333333,
    "scratch_rate": 0.3333333333,
    "avg_pnl_points": 2.5,
    "avg_pnl_pct": 0.05,
    "expectancy_points": 2.5,
    "expectancy_pct": 0.05,
    "best_pnl_points": 20.0,
    "worst_pnl_points": -10.0,
    "by_symbol": {
        EXAMPLE_SYMBOL: 3,
        EXAMPLE_SYMBOL_ALT: 1,
    },
    "by_direction": {
        "long": 2,
        "short": 2,
    },
    "latest_evaluated_at": EXAMPLE_EVALUATED_AT,
    "win_threshold_pct": 0.1,
    "loss_threshold_pct": -0.1,
}

EXECUTION_OUTCOMES_LEADERBOARD_RESPONSE_EXAMPLE = {
    "count": 2,
    "rows": [
        {
            "cohort_key": "adaptive-v2",
            "total": 3,
            "evaluated_count": 3,
            "winner_count": 2,
            "loser_count": 0,
            "scratch_count": 1,
            "unknown_count": 0,
            "win_rate": 0.6666666667,
            "loss_rate": 0.0,
            "avg_pnl_points": 45.0,
            "avg_pnl_pct": 0.12,
            "expectancy_points": 45.0,
            "expectancy_pct": 0.12,
            "best_pnl_points": 120.0,
            "worst_pnl_points": 5.0,
            "latest_evaluated_at": EXAMPLE_LATEST_EVALUATED_AT,
        },
        {
            "cohort_key": "adaptive-v3",
            "total": 2,
            "evaluated_count": 2,
            "winner_count": 1,
            "loser_count": 1,
            "scratch_count": 0,
            "unknown_count": 0,
            "win_rate": 0.5,
            "loss_rate": 0.5,
            "avg_pnl_points": 10.0,
            "avg_pnl_pct": 0.01,
            "expectancy_points": 10.0,
            "expectancy_pct": 0.01,
            "best_pnl_points": 40.0,
            "worst_pnl_points": -20.0,
            "latest_evaluated_at": "2026-03-10T17:01:00+00:00",
        },
    ],
}

EXECUTION_OUTCOMES_COMPARE_RESPONSE_EXAMPLE = {
    "left": {
        "group_by": "strategy",
        "cohort_key": "adaptive-v2",
        "total": 3,
        "evaluated_count": 3,
        "labeled_count": 3,
        "winner_count": 2,
        "loser_count": 0,
        "scratch_count": 1,
        "unknown_count": 0,
        "win_rate": 0.6666666667,
        "loss_rate": 0.0,
        "scratch_rate": 0.3333333333,
        "avg_pnl_points": 45.0,
        "avg_pnl_pct": 0.12,
        "expectancy_points": 45.0,
        "expectancy_pct": 0.12,
        "best_pnl_points": 120.0,
        "worst_pnl_points": 5.0,
        "latest_evaluated_at": EXAMPLE_LATEST_EVALUATED_AT,
    },
    "right": {
        "group_by": "strategy",
        "cohort_key": "adaptive-v3",
        "total": 2,
        "evaluated_count": 2,
        "labeled_count": 2,
        "winner_count": 1,
        "loser_count": 1,
        "scratch_count": 0,
        "unknown_count": 0,
        "win_rate": 0.5,
        "loss_rate": 0.5,
        "scratch_rate": 0.0,
        "avg_pnl_points": 10.0,
        "avg_pnl_pct": 0.01,
        "expectancy_points": 10.0,
        "expectancy_pct": 0.01,
        "best_pnl_points": 40.0,
        "worst_pnl_points": -20.0,
        "latest_evaluated_at": "2026-03-10T17:01:00+00:00",
    },
    "deltas": {
        "delta_win_rate": 0.1666666667,
        "delta_avg_pnl_points": 35.0,
        "delta_avg_pnl_pct": 0.11,
        "delta_expectancy_points": 35.0,
        "delta_expectancy_pct": 0.11,
    },
}

EXECUTION_OUTCOMES_EXPORT_FIELDS = [
    "id",
    "journal_id",
    "candidate_id",
    "signal_id",
    "worker_id",
    "symbol",
    "direction",
    "entry_price",
    "reference_timestamp",
    "evaluation_window_minutes",
    "outcome_status",
    "label",
    "exit_price",
    "pnl_points",
    "pnl_pct",
    "max_favorable_excursion",
    "max_adverse_excursion",
    "evaluated_at",
    "metadata_json",
]

EXECUTION_OUTCOMES_LEADERBOARD_FIELDS = [
    "cohort_key",
    "total",
    "evaluated_count",
    "winner_count",
    "loser_count",
    "scratch_count",
    "unknown_count",
    "win_rate",
    "loss_rate",
    "avg_pnl_points",
    "avg_pnl_pct",
    "expectancy_points",
    "expectancy_pct",
    "best_pnl_points",
    "worst_pnl_points",
    "latest_evaluated_at",
]

EXECUTION_OUTCOMES_POLICY_RECOMMENDATION_RESPONSE_EXAMPLE = {
    "group_by": "strategy",
    "scoring_mode": "blended",
    "selected_count": 1,
    "rows": [
        {
            "cohort_key": "adaptive-v2",
            "total": 3,
            "evaluated_count": 3,
            "winner_count": 2,
            "loser_count": 0,
            "scratch_count": 1,
            "unknown_count": 0,
            "win_rate": 0.6666666667,
            "loss_rate": 0.0,
            "avg_pnl_points": 45.0,
            "avg_pnl_pct": 0.12,
            "expectancy_points": 45.0,
            "expectancy_pct": 0.12,
            "best_pnl_points": 120.0,
            "worst_pnl_points": 5.0,
            "latest_evaluated_at": EXAMPLE_LATEST_EVALUATED_AT,
            "ranking_score": 0.142,
        }
    ],
    "recommendation_summary": "Selected 1 strategy cohort(s) using blended; top cohort is adaptive-v2.",
    "applied_filters": {
        "group_by": "strategy",
        "since": None,
        "until": None,
        "symbol": None,
        "worker_id": None,
        "direction": None,
        "outcome_status": None,
        "signal_id": None,
        "label": None,
        "min_samples": 1,
        "top_n": 1,
        "scoring_mode": "blended",
    },
}

EXECUTION_OUTCOMES_POLICY_MATRIX_RESPONSE_EXAMPLE = {
    "strategy": [
        {
            "cohort_key": "adaptive-v2",
            "total": 3,
            "evaluated_count": 3,
            "winner_count": 2,
            "loser_count": 0,
            "scratch_count": 1,
            "unknown_count": 0,
            "win_rate": 0.6666666667,
            "loss_rate": 0.0,
            "avg_pnl_points": 45.0,
            "avg_pnl_pct": 0.12,
            "expectancy_points": 45.0,
            "expectancy_pct": 0.12,
            "best_pnl_points": 120.0,
            "worst_pnl_points": 5.0,
            "latest_evaluated_at": EXAMPLE_LATEST_EVALUATED_AT,
            "ranking_score": 0.142,
        }
    ],
    "source": [],
    "setup_family": [],
    "worker_id": [],
}

EXECUTION_OUTCOMES_POLICY_AUDIT_RESPONSE_EXAMPLE = {
    "group_by": "strategy",
    "scoring_mode": "blended",
    "audit_steps": 2,
    "rows": [
        {
            "audit_cutoff": EXAMPLE_LATEST_EVALUATED_AT,
            "recommended_cohort": "adaptive-v2",
            "ranking_score": 0.142,
            "historical_sample_count": 3,
            "forward_sample_count": 2,
            "forward_avg_pnl_points": 30.0,
            "forward_avg_pnl_pct": 0.08,
            "forward_win_rate": 0.5,
            "forward_expectancy_points": 30.0,
            "forward_expectancy_pct": 0.08,
        }
    ],
    "summary": {
        "total_steps": 2,
        "avg_forward_pnl_points": 18.0,
        "avg_forward_pnl_pct": 0.04,
        "avg_forward_win_rate": 0.5,
        "avg_forward_expectancy_points": 18.0,
        "avg_forward_expectancy_pct": 0.04,
        "recommendation_hit_rate": 0.5,
    },
    "applied_filters": {
        "group_by": "strategy",
        "since": None,
        "until": None,
        "symbol": None,
        "direction": None,
        "outcome_status": None,
        "label": None,
        "min_samples": 1,
        "audit_step_size": 1,
        "audit_horizon_samples": 10,
        "top_n": 1,
        "scoring_mode": "blended",
    },
}

EXECUTION_OUTCOMES_POLICY_AUDIT_SUMMARY_RESPONSE_EXAMPLE = {
    "group_by": "strategy",
    "scoring_mode": "blended",
    "audit_steps": 2,
    "summary": {
        "total_steps": 2,
        "avg_forward_pnl_points": 18.0,
        "avg_forward_pnl_pct": 0.04,
        "avg_forward_win_rate": 0.5,
        "avg_forward_expectancy_points": 18.0,
        "avg_forward_expectancy_pct": 0.04,
        "recommendation_hit_rate": 0.5,
    },
    "applied_filters": {
        "group_by": "strategy",
        "since": None,
        "until": None,
        "symbol": None,
        "direction": None,
        "outcome_status": None,
        "label": None,
        "min_samples": 1,
        "audit_step_size": 1,
        "audit_horizon_samples": 10,
        "top_n": 1,
        "scoring_mode": "blended",
    },
}

EXECUTION_OUTCOMES_POLICY_RECOMMENDATION_FIELDS = [
    "cohort_key",
    "total",
    "evaluated_count",
    "winner_count",
    "loser_count",
    "scratch_count",
    "unknown_count",
    "win_rate",
    "loss_rate",
    "avg_pnl_points",
    "avg_pnl_pct",
    "expectancy_points",
    "expectancy_pct",
    "best_pnl_points",
    "worst_pnl_points",
    "latest_evaluated_at",
    "ranking_score",
]

EXECUTION_OUTCOMES_POLICY_AUDIT_FIELDS = [
    "audit_cutoff",
    "recommended_cohort",
    "ranking_score",
    "historical_sample_count",
    "forward_sample_count",
    "forward_avg_pnl_points",
    "forward_avg_pnl_pct",
    "forward_win_rate",
    "forward_expectancy_points",
    "forward_expectancy_pct",
]

EXECUTION_OUTCOMES_SUMMARY_RESPONSE_EXAMPLE = {
    "total": 6,
    "evaluated_count": 5,
    "insufficient_data_count": 1,
    "avg_pnl_points": 42.5,
    "avg_pnl_pct": 0.051,
    "win_rate": 0.6,
    "by_symbol": {
        EXAMPLE_SYMBOL: 4,
        EXAMPLE_SYMBOL_ALT: 2,
    },
    "latest_evaluated_at": EXAMPLE_EVALUATED_AT,
}

EXECUTION_OUTCOMES_VP_POLICY_SUMMARY_RESPONSE_EXAMPLE = {
    "total_rows": 6,
    "candidate_rows": 4,
    "long_candidate_rows": 2,
    "short_candidate_rows": 2,
    "avg_vp_trade_bias_score": 1.5,
}

EXECUTION_JOURNAL_TIMELINE_RESPONSE_EXAMPLE = {
    "count": 1,
    "limit": 50,
    "rows": [
        {
            "journal_id": 1,
            "candidate_id": 101,
            "signal_id": "sig-btc-20260310-001",
            "worker_id": "worker-a",
            "action": "simulation_decision",
            "execution_status": "filled",
            "execution_note": "simulation_filled_confidence_0.820_gte_0.750",
            "confidence": 0.82,
            "symbol": EXAMPLE_SYMBOL,
            "direction": "long",
            "entry_price": 82420.0,
            "candidate_timestamp": EXAMPLE_CREATED_TIMESTAMP,
            "journal_created_at": "2026-03-10T16:32:00+00:00",
            "strategy": "adaptive-v2",
            "source": "manual-webhook",
            "setup_family": "momentum",
            "claimed_by": "worker-a",
            "claim_token": "claim-token-example",
            "metadata_json": EXAMPLE_SIMULATION_META,
        }
    ],
}

EXECUTION_JOURNAL_TIMELINE_FIELDS = [
    "journal_id",
    "candidate_id",
    "signal_id",
    "worker_id",
    "action",
    "execution_status",
    "execution_note",
    "confidence",
    "symbol",
    "direction",
    "entry_price",
    "candidate_timestamp",
    "journal_created_at",
    "strategy",
    "source",
    "setup_family",
    "claimed_by",
    "claim_token",
    "metadata_json",
]


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    init_db()
    ensure_ingest_directories()
    start_ingest_cycle_scheduler()
    try:
        yield
    finally:
        await stop_ingest_cycle_scheduler()


app = FastAPI(title="BTC Adaptive Engine API", lifespan=lifespan)


def verify_signal_key(request: Request) -> None:
    expected_key = os.getenv("SIGNAL_WEBHOOK_KEY")
    provided_key = request.headers.get("X-SIGNAL-KEY")
    if not expected_key or provided_key != expected_key:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")


def _normalize_direction(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    cleaned = value.strip().lower()
    if not cleaned:
        raise ValueError(EMPTY_OPTIONAL_FIELD_ERROR)

    alias_map = {
        "buy": "long",
        "bull": "long",
        "sell": "short",
        "bear": "short",
    }
    normalized = alias_map.get(cleaned, cleaned)
    if normalized not in {"long", "short"}:
        raise ValueError("must be 'long' or 'short'")
    return normalized


class TradingViewPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str = Field(..., min_length=1, max_length=32)
    timeframe: str = Field(..., min_length=1, max_length=16)
    timestamp: datetime
    long_score: Optional[float] = None
    short_score: Optional[float] = None
    no_trade_score: Optional[float] = None
    setup_family: Optional[str] = None
    pressure_index: Optional[float] = None
    volatility_state: Optional[str] = None
    participation_score: Optional[float] = None
    confidence_seed: Optional[float] = None
    payload_json: Optional[Dict[str, Any]] = None

    @field_validator("symbol", "timeframe")
    @classmethod
    def validate_non_empty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(EMPTY_FIELD_ERROR)
        return cleaned


class TradingViewAlertWebhookPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    source: Optional[str] = Field(default="tradingview", max_length=64)
    symbol: str = Field(..., min_length=1, max_length=32)
    timeframe: str = Field(..., min_length=1, max_length=16)
    side: Optional[str] = Field(default=None, max_length=32)
    signal_name: Optional[str] = Field(default=None, max_length=128)
    strategy_id: Optional[str] = Field(default=None, max_length=128)
    score: Optional[float] = None
    bar_time: Optional[datetime] = None
    timestamp: Optional[datetime] = None
    price: Optional[float] = None
    atr: Optional[float] = None
    volume_ratio: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None

    @field_validator("symbol", "timeframe")
    @classmethod
    def validate_non_empty_required_fields(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(EMPTY_FIELD_ERROR)
        return cleaned


class TradeCandidatePayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    signal_id: Optional[str] = Field(default=None, max_length=128)
    timestamp: datetime
    symbol: str = Field(..., min_length=1, max_length=32)
    direction: Optional[str] = Field(default=None, max_length=16)
    entry_price: Optional[float] = None
    stop_price: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    confidence: Optional[float] = None
    setup_family: Optional[str] = Field(default=None, max_length=64)
    payload_json: Optional[Dict[str, Any]] = None

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(EMPTY_FIELD_ERROR)
        return cleaned

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_direction(value)


class TradeCandidateFromEventPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str = Field(..., min_length=1, max_length=32)
    timestamp: datetime
    timeframe: Optional[str] = Field(default=None, max_length=16)
    side: Optional[str] = Field(default=None, max_length=16)
    direction: Optional[str] = Field(default=None, max_length=16)
    price: Optional[float] = None
    entry_price: Optional[float] = None
    event_type: Optional[str] = Field(default=None, max_length=64)
    strategy: Optional[str] = Field(default=None, max_length=64)
    source: Optional[str] = Field(default=None, max_length=64)
    confidence: Optional[float] = None
    signal_id: Optional[str] = Field(default=None, max_length=128)
    setup_family: Optional[str] = Field(default=None, max_length=64)
    stop_price: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    payload_json: Optional[Dict[str, Any]] = None

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(EMPTY_FIELD_ERROR)
        return cleaned

    @field_validator("direction", "side")
    @classmethod
    def validate_side_or_direction(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_direction(value)


class TradeCandidateStatusUpdatePayload(BaseModel):
    execution_status: Literal["pending", "submitted", "filled", "rejected", "skipped"]
    execution_note: Optional[str] = Field(default=None, max_length=256)
    executed_at: Optional[datetime] = None


class TradeCandidateClaimPayload(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=128)

    @field_validator("worker_id")
    @classmethod
    def validate_worker_id(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(EMPTY_FIELD_ERROR)
        return cleaned


class TradeCandidateLeasePayload(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=128)
    claim_token: str = Field(..., min_length=1, max_length=256)

    @field_validator("worker_id", "claim_token")
    @classmethod
    def validate_non_empty_lease_fields(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(EMPTY_FIELD_ERROR)
        return cleaned


class TradeCandidateReleasePayload(TradeCandidateLeasePayload):
    execution_status: Literal["pending", "skipped", "rejected"] = "pending"
    execution_note: Optional[str] = Field(default=None, max_length=256)


class ExecutionOutcomesVpPolicyReasonBestResponse(BaseModel):
    applied_score: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SCORE_ECHO_DESCRIPTION)
    applied_side: Optional[Literal["long", "short"]] = Field(default=None, description=VP_POLICY_FILTER_SIDE_ECHO_DESCRIPTION)
    applied_since_days: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_DAYS_ECHO_DESCRIPTION)
    applied_since_trades: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_TRADES_ECHO_DESCRIPTION)
    is_filtered: bool = Field(description=VP_POLICY_FILTERED_FLAG_DESCRIPTION)
    count: int = Field(ge=0, description="Total rows returned")
    best_count: int = Field(ge=0, description=DESC_COUNT_BEST)
    rows: list[Dict[str, Any]] = Field(default_factory=list, description=DESC_ROWS_BEST)


class ExecutionOutcomesVpPolicyReasonWorstResponse(BaseModel):
    applied_score: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SCORE_ECHO_DESCRIPTION)
    applied_side: Optional[Literal["long", "short"]] = Field(default=None, description=VP_POLICY_FILTER_SIDE_ECHO_DESCRIPTION)
    applied_since_days: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_DAYS_ECHO_DESCRIPTION)
    applied_since_trades: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_TRADES_ECHO_DESCRIPTION)
    is_filtered: bool = Field(description=VP_POLICY_FILTERED_FLAG_DESCRIPTION)
    count: int = Field(ge=0, description="Total rows returned")
    worst_count: int = Field(ge=0, description=DESC_COUNT_WORST)
    rows: list[Dict[str, Any]] = Field(default_factory=list, description=DESC_ROWS_WORST)


class ExecutionOutcomesVpPolicyReasonBestWorstResponse(BaseModel):
    applied_score: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SCORE_ECHO_DESCRIPTION)
    applied_side: Optional[Literal["long", "short"]] = Field(default=None, description=VP_POLICY_FILTER_SIDE_ECHO_DESCRIPTION)
    applied_since_days: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_DAYS_ECHO_DESCRIPTION)
    applied_since_trades: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_TRADES_ECHO_DESCRIPTION)
    is_filtered: bool = Field(description=VP_POLICY_FILTERED_FLAG_DESCRIPTION)
    min_count_applied: int = Field(ge=1, description="Effective minimum cohort sample count applied")
    total_reason_cohorts: int = Field(ge=0, description="Total VP policy reason cohorts after score/side filters")
    eligible_reason_cohorts: int = Field(ge=0, description="Cohorts meeting min_count_applied")
    best_count: int = Field(ge=0, description=DESC_COUNT_BEST)
    worst_count: int = Field(ge=0, description=DESC_COUNT_WORST)
    best: list[Dict[str, Any]] = Field(default_factory=list, description=DESC_ROWS_BEST)
    worst: list[Dict[str, Any]] = Field(default_factory=list, description=DESC_ROWS_WORST)


class ExecutionOutcomesVpPolicyReasonMonitorResponse(BaseModel):
    applied_score: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SCORE_ECHO_DESCRIPTION)
    applied_side: Optional[Literal["long", "short"]] = Field(default=None, description=VP_POLICY_FILTER_SIDE_ECHO_DESCRIPTION)
    applied_since_days: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_DAYS_ECHO_DESCRIPTION)
    applied_since_trades: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SINCE_TRADES_ECHO_DESCRIPTION)
    min_count_applied: int = Field(ge=1, description="Effective minimum cohort sample count applied")
    total_reason_cohorts: int = Field(ge=0, description="Total VP policy reason cohorts after score/side filters")
    eligible_reason_cohorts: int = Field(ge=0, description="Cohorts meeting min_count_applied")
    best_count: int = Field(ge=0, description=DESC_COUNT_BEST)
    worst_count: int = Field(ge=0, description=DESC_COUNT_WORST)
    best: list[Dict[str, Any]] = Field(default_factory=list, description=DESC_ROWS_BEST)
    worst: list[Dict[str, Any]] = Field(default_factory=list, description=DESC_ROWS_WORST)
    monitor_status: Literal["empty", "thin", "healthy"] = Field(
        description="Monitor health based on returned cohort depth"
    )
    top_quality_score: Optional[float] = Field(
        default=None,
        description="quality_score of the first best row, if present",
    )
    bottom_quality_score: Optional[float] = Field(
        default=None,
        description="quality_score of the first worst row, if present",
    )
    quality_spread: Optional[float] = Field(
        default=None,
        description="top_quality_score - bottom_quality_score when both are present",
    )


class VpPolicyReasonPolicyRanking(BaseModel):
    policy: str = Field(description="VP policy reason key")
    score: float = Field(description="Quality score used for policy ranking")
    wins: int = Field(ge=0, description="Estimated count of direction-correct outcomes")
    losses: int = Field(ge=0, description="Estimated count of direction-incorrect outcomes")
    expectancy: float = Field(description="Average pnl_points for the cohort")
    sample_count: int = Field(ge=0, description="Number of outcomes in the cohort")
    direction_correct_rate: float = Field(ge=0.0, le=1.0, description="Direction correctness ratio in [0, 1]")


class ExecutionOutcomesVpPolicyReasonPolicyRankingsResponse(BaseModel):
    applied_score: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SCORE_ECHO_DESCRIPTION)
    applied_side: Optional[Literal["long", "short"]] = Field(default=None, description=VP_POLICY_FILTER_SIDE_ECHO_DESCRIPTION)
    is_filtered: bool = Field(description=VP_POLICY_FILTERED_FLAG_DESCRIPTION)
    count: int = Field(ge=0, description="Total ranked policies returned")
    policies: list[VpPolicyReasonPolicyRanking] = Field(default_factory=list, description="Ranked VP policy reason intelligence")


class VpPolicyReasonSelectorSimulationStep(BaseModel):
    step_index: int = Field(ge=1, description="1-based replay step index")
    evaluated_at: Optional[str] = Field(default=None, description="Evaluated timestamp for the replayed outcome")
    selected_policy: Optional[str] = Field(default=None, description="Policy selected from prior history")
    selected_score: Optional[float] = Field(default=None, description="Quality score for selected policy at this step")
    observed_policy: Optional[str] = Field(default=None, description="Policy observed on the replayed outcome row")
    selected: bool = Field(description="True when observed policy matched selected policy")
    pnl_points: Optional[float] = Field(default=None, description="Outcome pnl_points when selected is true")
    direction_correct: Optional[bool] = Field(default=None, description="Direction correctness when selected is true")


class VpPolicyReasonSelectorSimulationSummary(BaseModel):
    verdict: Literal["higher_cumulative_pnl", "equal_performance", "lower_cumulative_pnl"] = Field(
        description="Compact selector-vs-baseline verdict reason"
    )
    selector_outperformed_baseline: bool = Field(description="True when adaptive selector has higher cumulative pnl than baseline")
    adaptive_pnl: float = Field(description="Adaptive selector cumulative pnl_points")
    baseline_pnl: float = Field(description="Baseline cumulative pnl_points")
    pnl_delta: float = Field(description="adaptive_pnl - baseline_pnl")
    switch_rate: float = Field(ge=0.0, description="policy_switches / (total_selections - 1), or 0.0 when selections < 2")


class ExecutionOutcomesVpPolicyReasonSelectorSimulationResponse(BaseModel):
    applied_score: Optional[int] = Field(default=None, description=VP_POLICY_FILTER_SCORE_ECHO_DESCRIPTION)
    applied_side: Optional[Literal["long", "short"]] = Field(default=None, description=VP_POLICY_FILTER_SIDE_ECHO_DESCRIPTION)
    is_filtered: bool = Field(description=VP_POLICY_FILTERED_FLAG_DESCRIPTION)
    count: int = Field(ge=0, description="Alias for total replay steps")
    total_steps: int = Field(ge=0, description="Total replayed outcomes after filters")
    total_selections: int = Field(ge=0, description="Rows where observed policy matched selected policy")
    policy_switches: int = Field(ge=0, description="Times selected policy changed between consecutive selections")
    switch_rate: float = Field(ge=0.0, description="policy_switches / (total_selections - 1), or 0.0 when selections < 2")
    baseline_policy: Optional[str] = Field(default=None, description="Static baseline policy selected from full filtered window")
    baseline_total_selections: int = Field(ge=0, description="Rows matching baseline policy in replay window")
    baseline_wins: int = Field(ge=0, description="Direction-correct count across baseline selections")
    baseline_losses: int = Field(ge=0, description="Direction-incorrect count across baseline selections")
    baseline_expectancy: float = Field(description="Average pnl_points across baseline selections")
    baseline_cumulative_pnl_points: float = Field(description="Total pnl_points across baseline selections")
    simulated_wins: int = Field(ge=0, description="Direction-correct count across selected rows")
    simulated_losses: int = Field(ge=0, description="Direction-incorrect count across selected rows")
    simulated_expectancy: float = Field(description="Average pnl_points across selected rows")
    cumulative_pnl_points: float = Field(description="Total pnl_points across selected rows")
    expectancy_delta_vs_baseline: float = Field(description="simulated_expectancy - baseline_expectancy")
    cumulative_pnl_delta_vs_baseline: float = Field(description="cumulative_pnl_points - baseline_cumulative_pnl_points")
    win_rate_delta_vs_baseline: float = Field(description="simulated_win_rate - baseline_win_rate")
    selector_outperformed_baseline: bool = Field(description="True when adaptive selector has higher cumulative pnl than baseline")
    selector_outperformance_reason: Literal["higher_cumulative_pnl", "equal_performance", "lower_cumulative_pnl"] = Field(
        description="Verdict reason derived from cumulative pnl comparison against baseline"
    )
    summary: VpPolicyReasonSelectorSimulationSummary = Field(description="Compact top-card summary for selector-vs-baseline performance")
    steps: list[VpPolicyReasonSelectorSimulationStep] = Field(default_factory=list, description="Policy selector replay trace")


def build_trade_candidate_from_event(payload: TradeCandidateFromEventPayload) -> Dict[str, Any]:
    direction = payload.direction if payload.direction is not None else payload.side
    entry_price = payload.entry_price if payload.entry_price is not None else payload.price

    metadata = dict(payload.payload_json) if payload.payload_json is not None else {}
    defaults = {
        "timeframe": payload.timeframe,
        "event_type": payload.event_type,
        "strategy": payload.strategy,
        "source": payload.source,
    }
    for key, value in defaults.items():
        if key not in metadata and value is not None:
            metadata[key] = value
    metadata["derived_from_event"] = True

    generated_signal_id = payload.signal_id
    if generated_signal_id is None:
        direction_tag = direction if direction is not None else "na"
        generated_signal_id = f"{payload.symbol}-{direction_tag}-{payload.timestamp.strftime('%Y%m%d%H%M%S')}"

    setup_family = payload.setup_family or payload.event_type or payload.strategy

    return {
        "signal_id": generated_signal_id,
        "timestamp": payload.timestamp.isoformat(),
        "symbol": payload.symbol,
        "direction": direction,
        "entry_price": entry_price,
        "stop_price": payload.stop_price,
        "tp1": payload.tp1,
        "tp2": payload.tp2,
        "confidence": payload.confidence,
        "setup_family": setup_family,
        "payload_json": metadata or None,
    }


def _shape_recent_feature_snapshot_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    try:
        feature_values = json.loads(shaped_row.get("feature_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        feature_values = {}

    for key in RECENT_FEATURE_SNAPSHOT_CONTEXT_KEYS:
        shaped_row[key] = feature_values.get(key)

    return shaped_row


def _shape_execution_journal_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    try:
        metadata = json.loads(shaped_row.get("metadata_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}

    for key in EXECUTION_JOURNAL_DECISION_CONTEXT_KEYS:
        shaped_row[key] = metadata.get(key)

    return shaped_row


def _shape_execution_outcome_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    try:
        metadata = json.loads(shaped_row.get("metadata_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}

    if not isinstance(metadata, dict):
        metadata = {}

    for key in EXECUTION_OUTCOME_VP_POLICY_CONTEXT_KEYS:
        shaped_row[key] = metadata.get(key)

    return shaped_row


def _parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    return {}


def _shape_raw_webhook_event_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    shaped_row["headers_json"] = _parse_json_object(shaped_row.get("headers_json"))
    shaped_row["payload_json"] = _parse_json_object(shaped_row.get("payload_json"))
    return shaped_row


def _shape_normalized_signal_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    shaped_row["features_json"] = _parse_json_object(shaped_row.get("features_json"))
    return shaped_row


def _shape_strategy_risk_decision_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    shaped_row["decision_json"] = _parse_json_object(shaped_row.get("decision_json"))
    shaped_row["risk_json"] = _parse_json_object(shaped_row.get("risk_json"))
    return shaped_row


def _shape_execution_request_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    shaped_row["request_json"] = _parse_json_object(shaped_row.get("request_json"))
    return shaped_row


def _shape_broker_order_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    shaped_row["lifecycle_json"] = _parse_json_object(shaped_row.get("lifecycle_json"))
    return shaped_row


def _shape_fill_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    shaped_row["metadata_json"] = _parse_json_object(shaped_row.get("metadata_json"))
    return shaped_row


def _shape_position_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shaped_row = dict(row)
    shaped_row["metadata_json"] = _parse_json_object(shaped_row.get("metadata_json"))
    return shaped_row


def _current_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_paper_side(value: Any) -> str:
    cleaned = str(value or "long").strip().lower()
    return cleaned if cleaned in {"long", "short"} else "long"


def _build_paper_order_state(
    *,
    now_iso: str,
    is_actionable: bool,
    risk_payload: Dict[str, Any],
    strategy_payload: Dict[str, Any],
) -> Dict[str, Any]:
    lifecycle_events: list[Dict[str, Any]] = [{"status": "created", "at": now_iso}]
    reason_code = str(risk_payload.get("reason_code") or "execution_request_blocked")

    if is_actionable:
        lifecycle_events.extend(
            [
                {"status": "submitted", "at": now_iso},
                {"status": "accepted", "at": now_iso},
                {"status": "filled", "at": now_iso},
            ]
        )
        return {
            "status": "filled",
            "submitted_at": now_iso,
            "accepted_at": now_iso,
            "filled_at": now_iso,
            "rejected_at": None,
            "error_text": None,
            "lifecycle_json": {
                "events": lifecycle_events,
                "strategy_decision": strategy_payload.get("decision"),
                "risk_decision": risk_payload.get("risk_decision"),
            },
        }

    lifecycle_events.append(
        {
            "status": "rejected",
            "at": now_iso,
            "reason": reason_code,
        }
    )
    return {
        "status": "rejected",
        "submitted_at": None,
        "accepted_at": None,
        "filled_at": None,
        "rejected_at": now_iso,
        "error_text": reason_code,
        "lifecycle_json": {
            "events": lifecycle_events,
            "strategy_decision": strategy_payload.get("decision"),
            "risk_decision": risk_payload.get("risk_decision"),
        },
    }


def _upsert_paper_fill_and_position(
    *,
    now_iso: str,
    event_id: str,
    execution_request_id: str,
    order_id: str,
    broker_order_id: str,
    symbol_value: str,
    side_value: str,
    quantity: Optional[float],
    market_price: Optional[float],
    strategy_payload: Dict[str, Any],
    risk_payload: Dict[str, Any],
) -> Dict[str, Any]:
    fill_insert = insert_fill_event(
        order_id=order_id,
        broker_order_id=broker_order_id,
        execution_request_id=execution_request_id,
        event_id=event_id,
        symbol=symbol_value,
        side=side_value,
        fill_price=market_price,
        fill_qty=quantity,
        fee=0.0,
        fill_status="filled",
        fill_time=now_iso,
        metadata_json={
            "mode": "simulated",
            "strategy_decision": strategy_payload.get("decision"),
            "risk_decision": risk_payload.get("risk_decision"),
        },
    )

    fill_row = get_fill_by_order_id(order_id)
    shaped_fill = _shape_fill_row(fill_row) if fill_row is not None else None
    position_update: Optional[Dict[str, Any]] = None

    if not bool(fill_insert.get("duplicate")):
        position_update = apply_fill_to_positions(
            symbol=symbol_value,
            side=side_value,
            fill_qty=quantity,
            fill_price=market_price,
            order_id=order_id,
            fill_id=str(fill_insert["fill_id"]),
            fill_time=now_iso,
            metadata_json={
                "event_id": event_id,
                "execution_request_id": execution_request_id,
            },
        )
        if isinstance(position_update, dict) and isinstance(position_update.get("position"), dict):
            position_update["position"] = _shape_position_row(position_update["position"])

    return {
        "fill": shaped_fill,
        "fill_duplicate": bool(fill_insert.get("duplicate")),
        "position_update": position_update,
    }


def _run_paper_execution_alpha(
    *,
    event_id: str,
    normalized_id: str,
    normalized_signal: Dict[str, Any],
    strategy_payload: Dict[str, Any],
    risk_payload: Dict[str, Any],
    execution_record: Dict[str, Any],
    execution_payload: Dict[str, Any],
) -> Dict[str, Any]:
    now_iso = _current_utc_iso()
    quantity = _to_optional_float(execution_payload.get("quantity"))
    market_price = _to_optional_float(
        normalized_signal.get("market_price")
        if normalized_signal.get("market_price") is not None
        else normalized_signal.get("price")
    )
    side_value = _resolve_paper_side(execution_payload.get("side"))
    symbol_value = str(execution_payload.get("symbol") or normalized_signal.get("symbol") or "").strip().upper()
    is_actionable = bool(execution_payload.get("is_actionable")) and quantity is not None and quantity > 0.0
    order_state = _build_paper_order_state(
        now_iso=now_iso,
        is_actionable=is_actionable,
        risk_payload=risk_payload,
        strategy_payload=strategy_payload,
    )

    order_insert = insert_broker_order(
        BrokerOrderParams(
            execution_request_id=str(execution_record["execution_request_id"]),
            event_id=event_id,
            signal_id=normalized_id,
            symbol=symbol_value,
            side=side_value,
            order_type=str(execution_payload.get("order_type") or "market"),
            qty=quantity,
            limit_price=None,
            stop_price=None,
            status=str(order_state["status"]),
            submitted_at=order_state["submitted_at"],
            accepted_at=order_state["accepted_at"],
            filled_at=order_state["filled_at"],
            rejected_at=order_state["rejected_at"],
            mode=str(execution_payload.get("mode") or "simulated"),
            lifecycle_json=order_state["lifecycle_json"],
            error_text=order_state["error_text"],
            updated_at=now_iso,
        )
    )

    order_row = get_broker_order_by_execution_request_id(str(execution_record["execution_request_id"])) or {
        "order_id": order_insert["order_id"],
        "broker_order_id": order_insert["broker_order_id"],
        "status": order_state["status"],
        "execution_request_id": execution_record["execution_request_id"],
        "event_id": event_id,
    }
    shaped_order = _shape_broker_order_row(order_row)

    fill_state = {
        "fill": None,
        "fill_duplicate": None,
        "position_update": None,
    }
    if is_actionable:
        fill_state = _upsert_paper_fill_and_position(
            now_iso=now_iso,
            event_id=event_id,
            execution_request_id=str(execution_record["execution_request_id"]),
            order_id=str(order_insert["order_id"]),
            broker_order_id=str(order_insert["broker_order_id"]),
            symbol_value=symbol_value,
            side_value=side_value,
            quantity=quantity,
            market_price=market_price,
            strategy_payload=strategy_payload,
            risk_payload=risk_payload,
        )

    return {
        "order": shaped_order,
        "order_duplicate": bool(order_insert.get("duplicate")),
        "fill": fill_state["fill"],
        "fill_duplicate": fill_state["fill_duplicate"],
        "position_update": fill_state["position_update"],
    }


@app.get("/")
def root():
    return {"status": "ok", "message": "BTC Adaptive Engine API online"}


@app.get("/health")
def health():
    if not is_database_reachable():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API is alive but database is unreachable",
        )

    return {
        "status": "ok",
        "api": "alive",
        "database": "reachable",
    }


@app.get("/bar_states/recent")
def bar_states_recent(limit: Annotated[int, Query(ge=1)] = 50):
    capped_limit = min(limit, 500)

    try:
        rows = get_recent_bar_states(capped_limit)
    except Exception:
        logger.exception("Failed to fetch recent bar_states")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent bar states",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get("/feature_snapshots/recent")
def feature_snapshots_recent(
    limit: Annotated[int, Query(ge=1)] = 20,
    symbol: str | None = None,
    timeframe: str | None = None,
):
    capped_limit = min(limit, 500)

    try:
        rows = get_recent_feature_snapshots(
            capped_limit,
            symbol=symbol,
            timeframe=timeframe,
        )
    except Exception:
        logger.exception("Failed to fetch recent feature_snapshots")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent feature snapshots",
        )

    shaped_rows = [_shape_recent_feature_snapshot_row(row) for row in rows]

    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get("/volume_profile_snapshots/recent")
def volume_profile_snapshots_recent(
    limit: Annotated[int, Query(ge=1)] = 20,
    symbol: str | None = None,
    timeframe: str | None = None,
):
    capped_limit = min(limit, 500)

    try:
        rows = get_recent_volume_profile_snapshots(
            limit=capped_limit,
            symbol=symbol,
            timeframe=timeframe,
        )
    except Exception:
        logger.exception("Failed to fetch recent volume_profile_snapshots")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent volume profile snapshots",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get("/bar_states/{id}")
def bar_state_by_id(id: int):
    try:
        row = get_bar_state_by_id(id)
    except Exception:
        logger.exception("Failed to fetch bar_state by id")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch bar state",
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"bar_state id {id} not found",
        )

    return row


@app.post(
    "/trade_candidates",
    status_code=status.HTTP_201_CREATED,
    summary="Create trade candidate",
    responses={
        201: {
            "description": "Trade candidate stored",
            "content": {"application/json": {"example": TRADE_CANDIDATE_CREATE_RESPONSE_EXAMPLE}},
        },
        403: {"description": "Missing or invalid signal key"},
    },
)
@app.post(
    "/trade_candidate",
    status_code=status.HTTP_201_CREATED,
    summary="Create trade candidate (compat)",
    responses={
        201: {
            "description": "Trade candidate stored",
            "content": {"application/json": {"example": TRADE_CANDIDATE_CREATE_RESPONSE_EXAMPLE}},
        },
        403: {"description": "Missing or invalid signal key"},
    },
)
async def create_trade_candidate(
    payload: Annotated[TradeCandidatePayload, Body(openapi_examples=TRADE_CANDIDATE_REQUEST_EXAMPLES)],
    request: Request,
):
    verify_signal_key(request)
    candidate = payload.model_dump(mode="json")

    try:
        row_id = insert_trade_candidate(candidate)
    except Exception:
        logger.exception("Failed to persist trade candidate")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist trade candidate",
        )

    return {
        "status": "stored",
        "id": row_id,
        "client": request.client.host if request.client else None,
        "symbol": payload.symbol,
        "timestamp": payload.timestamp.isoformat(),
        "signal_id": payload.signal_id,
        "execution_status": "pending",
    }


@app.post(
    "/trade_candidates/from_event",
    status_code=status.HTTP_201_CREATED,
    summary="Create trade candidate from event payload",
    responses={
        200: {
            "description": "Duplicate/replayed event detected",
            "content": {"application/json": {"example": TRADE_CANDIDATE_FROM_EVENT_DUPLICATE_RESPONSE_EXAMPLE}},
        },
        201: {
            "description": "Derived trade candidate stored",
            "content": {"application/json": {"example": TRADE_CANDIDATE_FROM_EVENT_RESPONSE_EXAMPLE}},
        },
        403: {"description": "Missing or invalid signal key"},
    },
)
async def create_trade_candidate_from_event(
    payload: Annotated[
        TradeCandidateFromEventPayload,
        Body(openapi_examples=TRADE_CANDIDATE_FROM_EVENT_EXAMPLES),
    ],
    request: Request,
    response: Response,
):
    verify_signal_key(request)
    candidate = build_trade_candidate_from_event(payload)

    try:
        replay_result = insert_trade_candidate_from_event(candidate)
    except Exception:
        logger.exception("Failed to persist derived trade candidate")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist derived trade candidate",
        )

    if replay_result["duplicate"]:
        response.status_code = status.HTTP_200_OK
        return {
            "status": "duplicate",
            "id": replay_result["id"],
            "client": request.client.host if request.client else None,
            "symbol": candidate["symbol"],
            "timestamp": candidate["timestamp"],
            "signal_id": candidate["signal_id"],
            "derived_from_event": True,
            "replayed": True,
        }

    return {
        "status": "stored",
        "id": replay_result["id"],
        "client": request.client.host if request.client else None,
        "symbol": candidate["symbol"],
        "timestamp": candidate["timestamp"],
        "signal_id": candidate["signal_id"],
        "derived_from_event": True,
        "replayed": False,
    }


@app.patch(
    "/trade_candidates/{candidate_id}/status",
    summary="Update trade candidate execution status",
    responses={
        200: {
            "description": "Execution status updated",
            "content": {"application/json": {"example": TRADE_CANDIDATE_STATUS_UPDATE_RESPONSE_EXAMPLE}},
        },
        403: {"description": "Missing or invalid signal key"},
        404: {"description": "Trade candidate not found"},
    },
)
async def patch_trade_candidate_status(
    candidate_id: int,
    payload: Annotated[
        TradeCandidateStatusUpdatePayload,
        Body(openapi_examples={"filled": Example(summary="Mark candidate filled", value=TRADE_CANDIDATE_STATUS_UPDATE_EXAMPLE)}),
    ],
    request: Request,
):
    verify_signal_key(request)

    update_kwargs: Dict[str, Any] = {
        "candidate_id": candidate_id,
        "execution_status": payload.execution_status,
    }

    if "execution_note" in payload.model_fields_set:
        update_kwargs["execution_note"] = payload.execution_note

    if "executed_at" in payload.model_fields_set:
        update_kwargs["executed_at"] = payload.executed_at.isoformat() if payload.executed_at else None

    updated_row = update_trade_candidate_status(**update_kwargs)
    if updated_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"trade_candidate id {candidate_id} not found",
        )

    return {
        "status": "updated",
        "id": updated_row["id"],
        "execution_status": updated_row["execution_status"],
        "execution_note": updated_row["execution_note"],
        "executed_at": updated_row["executed_at"],
    }


@app.post(
    "/trade_candidates/claim_next",
    summary="Claim next pending trade candidate",
    responses={
        200: {
            "description": "Claim result",
            "content": {
                "application/json": {
                    "examples": {
                        "claimed": {"summary": "Candidate claimed", "value": TRADE_CANDIDATE_CLAIM_RESPONSE_EXAMPLE},
                        "empty": {"summary": "No pending candidates", "value": TRADE_CANDIDATE_CLAIM_EMPTY_RESPONSE_EXAMPLE},
                    }
                }
            },
        },
        403: {"description": "Missing or invalid signal key"},
    },
)
async def claim_next_candidate(
    payload: Annotated[
        TradeCandidateClaimPayload,
        Body(openapi_examples={"worker": Example(summary="Worker claim request", value=TRADE_CANDIDATE_CLAIM_REQUEST_EXAMPLE)}),
    ],
    request: Request,
):
    verify_signal_key(request)

    claimed_row = claim_next_trade_candidate(payload.worker_id)
    if claimed_row is None:
        return {
            "status": "empty",
            "row": None,
        }

    return {
        "status": "claimed",
        "claim_token": claimed_row["claim_token"],
        "row": claimed_row,
    }


@app.post(
    "/trade_candidates/{candidate_id}/heartbeat",
    summary="Refresh worker claim heartbeat",
    responses={
        200: {
            "description": "Heartbeat refreshed",
            "content": {"application/json": {"example": TRADE_CANDIDATE_HEARTBEAT_RESPONSE_EXAMPLE}},
        },
        403: {"description": "Missing or invalid signal key"},
        404: {"description": "Trade candidate not found"},
        409: {"description": "Lease ownership mismatch"},
    },
)
async def trade_candidate_heartbeat(
    candidate_id: int,
    payload: Annotated[
        TradeCandidateLeasePayload,
        Body(openapi_examples={"heartbeat": Example(summary="Heartbeat payload", value=TRADE_CANDIDATE_HEARTBEAT_EXAMPLE)}),
    ],
    request: Request,
):
    verify_signal_key(request)

    heartbeat_result = heartbeat_trade_candidate_claim(
        candidate_id,
        worker_id=payload.worker_id,
        claim_token=payload.claim_token,
    )

    if heartbeat_result["status"] == "not_found":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"trade_candidate id {candidate_id} not found",
        )

    if heartbeat_result["status"] == "lease_mismatch":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Lease ownership mismatch",
        )

    row = heartbeat_result["row"]
    return {
        "status": "ok",
        "id": row["id"],
        "claimed_by": row["claimed_by"],
        "claim_token": row["claim_token"],
        "claimed_at": row["claimed_at"],
    }


@app.post(
    "/trade_candidates/{candidate_id}/release",
    summary="Release worker claim",
    responses={
        200: {
            "description": "Lease released",
            "content": {"application/json": {"example": TRADE_CANDIDATE_RELEASE_RESPONSE_EXAMPLE}},
        },
        403: {"description": "Missing or invalid signal key"},
        404: {"description": "Trade candidate not found"},
        409: {"description": "Lease ownership mismatch"},
    },
)
async def trade_candidate_release(
    candidate_id: int,
    payload: Annotated[
        TradeCandidateReleasePayload,
        Body(openapi_examples={"release": Example(summary="Release payload", value=TRADE_CANDIDATE_RELEASE_EXAMPLE)}),
    ],
    request: Request,
):
    verify_signal_key(request)

    release_kwargs: Dict[str, Any] = {
        "candidate_id": candidate_id,
        "worker_id": payload.worker_id,
        "claim_token": payload.claim_token,
        "execution_status": payload.execution_status,
    }
    if "execution_note" in payload.model_fields_set:
        release_kwargs["execution_note"] = payload.execution_note

    release_result = release_trade_candidate_claim(**release_kwargs)

    if release_result["status"] == "not_found":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"trade_candidate id {candidate_id} not found",
        )

    if release_result["status"] == "lease_mismatch":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Lease ownership mismatch",
        )

    row = release_result["row"]
    return {
        "status": "released",
        "id": row["id"],
        "execution_status": row["execution_status"],
        "execution_note": row["execution_note"],
        "claimed_by": row["claimed_by"],
        "claim_token": row["claim_token"],
        "claimed_at": row["claimed_at"],
    }


@app.get(
    "/trade_candidates/summary",
    summary="Get compact execution summary",
    responses={
        200: {
            "description": "Execution status counts",
            "content": {"application/json": {"example": TRADE_CANDIDATE_SUMMARY_RESPONSE_EXAMPLE}},
        }
    },
)
def trade_candidates_summary():
    return get_trade_candidate_execution_summary()


def _load_execution_journal_timeline_rows(
    *,
    limit: int,
    worker_id: Optional[str] = None,
    execution_status: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    signal_id: Optional[str] = None,
    candidate_id: Optional[int] = None,
    action: Optional[str] = None,
) -> tuple[int, list[Dict[str, Any]]]:
    capped_limit = min(limit, 500)

    try:
        rows = get_execution_journal_timeline(
            capped_limit,
            worker_id=worker_id,
            execution_status=execution_status,
            symbol=symbol,
            direction=direction,
            signal_id=signal_id,
            candidate_id=candidate_id,
            action=action,
        )
    except Exception:
        logger.exception("Failed to fetch execution journal timeline")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution journal timeline",
        )

    return capped_limit, [_shape_execution_journal_row(row) for row in rows]


def _load_execution_journal_daily_rollup_rows(
    *,
    limit: int,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> tuple[int, list[Dict[str, Any]]]:
    capped_limit = min(limit, 500)
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        rows = get_execution_journal_daily_rollup(
            worker_id=worker_id,
            symbol=symbol,
            since=since_iso,
            until=until_iso,
            limit=capped_limit,
        )
    except Exception:
        logger.exception("Failed to fetch execution journal daily rollup")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution journal daily rollup",
        )

    return capped_limit, rows


def _load_recent_execution_outcome_rows(
    *,
    limit: int,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> tuple[int, list[Dict[str, Any]]]:
    capped_limit = min(limit, 500)
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        rows = get_recent_execution_outcomes(
            capped_limit,
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            label=label,
            since=since_iso,
            until=until_iso,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch recent execution outcomes")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes",
        )

    return capped_limit, [_shape_execution_outcome_row(row) for row in rows]


def _load_execution_outcomes_scorecard(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        return get_execution_outcomes_scorecard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            label=label,
            since=since_iso,
            until=until_iso,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch execution outcomes scorecard")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes scorecard",
        )


def _load_execution_outcomes_leaderboard(
    *,
    group_by: str,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    min_samples: int = 1,
    limit: int = 50,
) -> list[Dict[str, Any]]:
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        return get_execution_outcomes_leaderboard(
            group_by=group_by,
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            since=since_iso,
            until=until_iso,
            label=label,
            min_samples=min_samples,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch execution outcomes leaderboard")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes leaderboard",
        )


def _load_execution_outcomes_compare(
    *,
    left_group_by: str,
    left_value: str,
    right_group_by: str,
    right_value: str,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> Dict[str, Any]:
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        return get_execution_outcomes_compare(
            left_group_by=left_group_by,
            left_value=left_value,
            right_group_by=right_group_by,
            right_value=right_value,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            label=label,
            since=since_iso,
            until=until_iso,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch execution outcomes compare")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes compare",
        )


def _load_execution_outcomes_policy_recommendation(
    *,
    group_by: str,
    symbol: Optional[str] = None,
    worker_id: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    min_samples: int = 1,
    top_n: int = 1,
    scoring_mode: str = "blended",
) -> Dict[str, Any]:
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        return get_execution_outcomes_policy_recommendation(
            group_by=group_by,
            symbol=symbol,
            worker_id=worker_id,
            direction=direction,
            outcome_status=outcome_status,
            label=label,
            since=since_iso,
            until=until_iso,
            min_samples=min_samples,
            top_n=top_n,
            scoring_mode=scoring_mode,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch execution outcomes policy recommendation")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes policy recommendation",
        )


def _load_execution_outcomes_policy_matrix(
    *,
    symbol: Optional[str] = None,
    worker_id: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    min_samples: int = 1,
    top_n_per_group: int = 2,
    scoring_mode: str = "blended",
) -> Dict[str, list[Dict[str, Any]]]:
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        return get_execution_outcomes_policy_matrix(
            groupings=OUTCOME_POLICY_MATRIX_GROUPS,
            symbol=symbol,
            worker_id=worker_id,
            direction=direction,
            outcome_status=outcome_status,
            label=label,
            since=since_iso,
            until=until_iso,
            min_samples=min_samples,
            top_n_per_group=top_n_per_group,
            scoring_mode=scoring_mode,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch execution outcomes policy matrix")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes policy matrix",
        )


def _load_execution_outcomes_policy_audit(
    *,
    group_by: str,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    min_samples: int = 1,
    audit_step_size: int = 1,
    audit_horizon_samples: int = 10,
    top_n: int = 1,
    scoring_mode: str = "blended",
) -> Dict[str, Any]:
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        return get_execution_outcomes_policy_audit(
            group_by=group_by,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            label=label,
            since=since_iso,
            until=until_iso,
            min_samples=min_samples,
            audit_step_size=audit_step_size,
            audit_horizon_samples=audit_horizon_samples,
            top_n=top_n,
            scoring_mode=scoring_mode,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch execution outcomes policy audit")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes policy audit",
        )


def _load_execution_outcomes_policy_audit_summary(
    *,
    group_by: str,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    label: Optional[Literal["winner", "loser", "scratch", "unknown"]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    min_samples: int = 1,
    audit_step_size: int = 1,
    audit_horizon_samples: int = 10,
    top_n: int = 1,
    scoring_mode: str = "blended",
) -> Dict[str, Any]:
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        return get_execution_outcomes_policy_audit_summary(
            group_by=group_by,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            label=label,
            since=since_iso,
            until=until_iso,
            min_samples=min_samples,
            audit_step_size=audit_step_size,
            audit_horizon_samples=audit_horizon_samples,
            top_n=top_n,
            scoring_mode=scoring_mode,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        logger.exception("Failed to fetch execution outcomes policy audit summary")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes policy audit summary",
        )


@app.get(
    "/execution_journal/recent",
    summary="Get recent execution journal rows",
    responses={
        200: {
            "description": "Recent journal rows",
            "content": {"application/json": {"example": EXECUTION_JOURNAL_RECENT_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_journal_recent(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    execution_status: Annotated[Optional[str], Query(min_length=1, examples=["filled"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    action: Annotated[Optional[str], Query(min_length=1, examples=["simulation_decision"])] = None,
):
    capped_limit = min(limit, 500)

    try:
        rows = get_recent_execution_journal(
            capped_limit,
            worker_id=worker_id,
            execution_status=execution_status,
            symbol=symbol,
            action=action,
        )
    except Exception:
        logger.exception("Failed to fetch recent execution journal rows")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution journal rows",
        )

    shaped_rows = [_shape_execution_journal_row(row) for row in rows]

    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/execution_journal/timeline",
    summary="Get execution journal timeline",
    responses={
        200: {
            "description": "Timeline rows with journal + candidate context",
            "content": {"application/json": {"example": EXECUTION_JOURNAL_TIMELINE_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_journal_timeline(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    execution_status: Annotated[Optional[str], Query(min_length=1, examples=["filled"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    candidate_id: Annotated[Optional[int], Query(ge=1, examples=[101])] = None,
    action: Annotated[Optional[str], Query(min_length=1, examples=["simulation_decision"])] = None,
):
    capped_limit, rows = _load_execution_journal_timeline_rows(
        limit=limit,
        worker_id=worker_id,
        execution_status=execution_status,
        symbol=symbol,
        direction=direction,
        signal_id=signal_id,
        candidate_id=candidate_id,
        action=action,
    )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get(
    "/execution_journal/export.csv",
    summary="Export execution journal timeline as CSV",
    responses={
        200: {
            "description": "CSV export of execution journal timeline",
        }
    },
)
def execution_journal_export_csv(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[500])] = 500,
    worker_id: Annotated[Optional[str], Query(min_length=1)] = None,
    execution_status: Annotated[Optional[str], Query(min_length=1)] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1)] = None,
    candidate_id: Annotated[Optional[int], Query(ge=1)] = None,
    action: Annotated[Optional[str], Query(min_length=1)] = None,
):
    _, rows = _load_execution_journal_timeline_rows(
        limit=limit,
        worker_id=worker_id,
        execution_status=execution_status,
        symbol=symbol,
        direction=direction,
        signal_id=signal_id,
        candidate_id=candidate_id,
        action=action,
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXECUTION_JOURNAL_TIMELINE_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in EXECUTION_JOURNAL_TIMELINE_FIELDS})

    return Response(
        content=output.getvalue(),
        media_type=CSV_MEDIA_TYPE,
        headers={"Content-Disposition": "attachment; filename=execution_journal_export.csv"},
    )


@app.get(
    "/execution_journal/analytics",
    summary="Get execution journal analytics",
    responses={
        200: {
            "description": "Compact simulation performance metrics",
            "content": {"application/json": {"example": EXECUTION_JOURNAL_ANALYTICS_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_journal_analytics(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    execution_status: Annotated[Optional[str], Query(min_length=1, examples=["filled"])] = None,
    since: Annotated[Optional[datetime], Query(description="Include journal rows at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include journal rows at/before this timestamp")] = None,
    limit: Annotated[Optional[int], Query(ge=1, le=5000, description="Optional cap for bounded analytics window")] = None,
):
    since_iso = since.isoformat() if since is not None else None
    until_iso = until.isoformat() if until is not None else None

    try:
        analytics = get_execution_journal_analytics(
            worker_id=worker_id,
            symbol=symbol,
            execution_status=execution_status,
            since=since_iso,
            until=until_iso,
            limit=limit,
        )
    except Exception:
        logger.exception("Failed to fetch execution journal analytics")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution journal analytics",
        )

    return analytics


@app.get(
    "/execution_journal/daily_rollup",
    summary="Get execution journal daily rollup",
    responses={
        200: {
            "description": "Date-bucketed decision counts",
            "content": {"application/json": {"example": EXECUTION_JOURNAL_DAILY_ROLLUP_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_journal_daily_rollup(
    limit: Annotated[int, Query(ge=1, description="Max days to return (capped at 500)", examples=[30])] = 30,
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    since: Annotated[Optional[datetime], Query(description="Include journal rows at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include journal rows at/before this timestamp")] = None,
):
    _, rows = _load_execution_journal_daily_rollup_rows(
        limit=limit,
        worker_id=worker_id,
        symbol=symbol,
        since=since,
        until=until,
    )

    return {
        "count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_journal/daily_rollup.csv",
    summary="Export execution journal daily rollup as CSV",
    responses={
        200: {
            "description": "CSV export of execution journal daily rollup",
        }
    },
)
def execution_journal_daily_rollup_csv(
    limit: Annotated[int, Query(ge=1, description="Max days to return (capped at 500)", examples=[365])] = 365,
    worker_id: Annotated[Optional[str], Query(min_length=1)] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    since: Annotated[Optional[datetime], Query()] = None,
    until: Annotated[Optional[datetime], Query()] = None,
):
    _, rows = _load_execution_journal_daily_rollup_rows(
        limit=limit,
        worker_id=worker_id,
        symbol=symbol,
        since=since,
        until=until,
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXECUTION_JOURNAL_DAILY_ROLLUP_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in EXECUTION_JOURNAL_DAILY_ROLLUP_FIELDS})

    return Response(
        content=output.getvalue(),
        media_type=CSV_MEDIA_TYPE,
        headers={"Content-Disposition": "attachment; filename=execution_journal_daily_rollup.csv"},
    )


@app.get(
    "/execution_journal/summary",
    summary="Get execution journal summary",
    responses={
        200: {
            "description": "Execution journal status counts",
            "content": {"application/json": {"example": EXECUTION_JOURNAL_SUMMARY_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_journal_summary():
    return get_execution_journal_summary()


@app.get(
    "/execution_outcomes/recent",
    summary="Get recent execution outcomes",
    responses={
        200: {
            "description": "Recent paper-P&L outcomes",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_RECENT_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_recent(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query(examples=["winner"])] = None,
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
):
    capped_limit, rows = _load_recent_execution_outcome_rows(
        limit=limit,
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        label=label,
        since=since,
        until=until,
    )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/scorecard",
    summary="Get labeled execution outcomes scorecard",
    responses={
        200: {
            "description": "Compact labeled outcome performance metrics",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_SCORECARD_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_scorecard(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query(examples=["winner"])] = None,
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
    limit: Annotated[Optional[int], Query(ge=1, le=5000, description="Optional cap for bounded scorecard windows")] = None,
):
    return _load_execution_outcomes_scorecard(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        label=label,
        since=since,
        until=until,
        limit=limit,
    )


@app.get(
    "/execution_outcomes/leaderboard",
    summary="Get outcomes cohort leaderboard",
    responses={
        200: {
            "description": "Ranked cohort performance rows",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_LEADERBOARD_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_leaderboard(
    group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(description="Cohort grouping key", examples=["strategy"]),
    ],
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    worker_id: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1, description="Minimum rows per cohort", examples=[1])] = 1,
    limit: Annotated[int, Query(ge=1, le=500, description="Max cohorts to return", examples=[50])] = 50,
):
    rows = _load_execution_outcomes_leaderboard(
        group_by=group_by,
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        limit=limit,
    )

    return {
        "count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/compare",
    summary="Compare two outcomes cohorts",
    responses={
        200: {
            "description": "Side-by-side cohort metrics with deltas",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_COMPARE_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_compare(
    left_group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(examples=["strategy"]),
    ],
    left_value: Annotated[str, Query(min_length=1, examples=["adaptive-v2"])],
    right_group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(examples=["strategy"]),
    ],
    right_value: Annotated[str, Query(min_length=1, examples=["adaptive-v3"])],
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
):
    return _load_execution_outcomes_compare(
        left_group_by=left_group_by,
        left_value=left_value,
        right_group_by=right_group_by,
        right_value=right_value,
        symbol=symbol,
        direction=direction,
        label=label,
        outcome_status=outcome_status,
        since=since,
        until=until,
    )


@app.get(
    "/execution_outcomes/policy_recommendation",
    summary="Get recommended outcomes policy cohort(s)",
    responses={
        200: {
            "description": "Top ranked cohort recommendation rows",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_POLICY_RECOMMENDATION_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_policy_recommendation(
    group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(description="Cohort grouping key", examples=["strategy"]),
    ] = "strategy",
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1, description="Minimum rows per cohort", examples=[1])] = 1,
    top_n: Annotated[int, Query(ge=1, le=500, description="Number of cohorts to select", examples=[1])] = 1,
    scoring_mode: Annotated[
        Literal["expectancy_pct", "expectancy_points", "win_rate", "avg_pnl_pct", "blended"],
        Query(examples=["blended"]),
    ] = "blended",
):
    return _load_execution_outcomes_policy_recommendation(
        group_by=group_by,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        top_n=top_n,
        scoring_mode=scoring_mode,
    )


@app.get(
    "/execution_outcomes/policy_matrix",
    summary="Get policy matrix across core groupings",
    responses={
        200: {
            "description": "Top ranked cohort rows for strategy/source/setup_family/worker_id",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_POLICY_MATRIX_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_policy_matrix(
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1, description="Minimum rows per cohort", examples=[1])] = 1,
    top_n_per_group: Annotated[int, Query(ge=1, le=100, description="Rows per grouping", examples=[2])] = 2,
    scoring_mode: Annotated[
        Literal["expectancy_pct", "expectancy_points", "win_rate", "avg_pnl_pct", "blended"],
        Query(examples=["blended"]),
    ] = "blended",
):
    return _load_execution_outcomes_policy_matrix(
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        top_n_per_group=top_n_per_group,
        scoring_mode=scoring_mode,
    )


@app.get(
    "/execution_outcomes/policy_audit",
    summary="Audit historical policy recommendation outcomes",
    responses={
        200: {
            "description": "Historical policy recommendation audit rows and aggregate summary",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_POLICY_AUDIT_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_policy_audit(
    group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(description="Cohort grouping key", examples=["strategy"]),
    ] = "strategy",
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1, le=5000, description="Minimum rows per cohort", examples=[1])] = 1,
    audit_step_size: Annotated[int, Query(ge=1, le=5000, description="Historical step size in samples", examples=[1])] = 1,
    audit_horizon_samples: Annotated[int, Query(ge=1, le=5000, description="Forward window size in samples", examples=[10])] = 10,
    top_n: Annotated[int, Query(ge=1, le=500, description="Ranked candidate count before selecting", examples=[1])] = 1,
    scoring_mode: Annotated[
        Literal["expectancy_pct", "expectancy_points", "win_rate", "avg_pnl_pct", "blended"],
        Query(examples=["blended"]),
    ] = "blended",
):
    return _load_execution_outcomes_policy_audit(
        group_by=group_by,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        audit_step_size=audit_step_size,
        audit_horizon_samples=audit_horizon_samples,
        top_n=top_n,
        scoring_mode=scoring_mode,
    )


@app.get(
    "/execution_outcomes/policy_audit_summary",
    summary="Get compact policy audit aggregates",
    responses={
        200: {
            "description": "Aggregate-only policy audit response",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_POLICY_AUDIT_SUMMARY_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_policy_audit_summary(
    group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(description="Cohort grouping key", examples=["strategy"]),
    ] = "strategy",
    since: Annotated[Optional[datetime], Query(description="Include outcomes at/after this timestamp")] = None,
    until: Annotated[Optional[datetime], Query(description="Include outcomes at/before this timestamp")] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1, le=5000)] = 1,
    audit_step_size: Annotated[int, Query(ge=1, le=5000)] = 1,
    audit_horizon_samples: Annotated[int, Query(ge=1, le=5000)] = 10,
    top_n: Annotated[int, Query(ge=1, le=500)] = 1,
    scoring_mode: Annotated[
        Literal["expectancy_pct", "expectancy_points", "win_rate", "avg_pnl_pct", "blended"],
        Query(),
    ] = "blended",
):
    return _load_execution_outcomes_policy_audit_summary(
        group_by=group_by,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        audit_step_size=audit_step_size,
        audit_horizon_samples=audit_horizon_samples,
        top_n=top_n,
        scoring_mode=scoring_mode,
    )


@app.get(
    "/execution_outcomes/summary",
    summary="Get execution outcomes summary",
    responses={
        200: {
            "description": "Compact paper-P&L summary metrics",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_SUMMARY_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_summary(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
):
    try:
        summary = get_execution_outcomes_summary(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes summary")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes summary",
        )

    return summary


def _build_vp_policy_reason_policy_rankings(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    policy_rankings: list[Dict[str, Any]] = []
    for row in rows:
        sample_count = int(row.get("row_count") or 0)
        direction_correct_rate_value = row.get("direction_correct_rate")
        direction_correct_rate = float(direction_correct_rate_value) if direction_correct_rate_value is not None else 0.0

        wins = int(round(sample_count * direction_correct_rate))
        wins = max(0, min(sample_count, wins))
        losses = max(0, sample_count - wins)

        avg_pnl_value = row.get("avg_pnl")
        quality_score_value = row.get("quality_score")

        policy_rankings.append(
            {
                "policy": str(row.get("vp_policy_reason") or "unknown"),
                "score": float(quality_score_value) if quality_score_value is not None else 0.0,
                "wins": wins,
                "losses": losses,
                "expectancy": float(avg_pnl_value) if avg_pnl_value is not None else 0.0,
                "sample_count": sample_count,
                "direction_correct_rate": direction_correct_rate,
            }
        )

    return policy_rankings


def _to_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_vp_policy_side_for_simulation(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"long", "short"} else "unknown"


def _is_vp_policy_direction_correct_for_simulation(row: Dict[str, Any]) -> Optional[bool]:
    policy_side = _normalize_vp_policy_side_for_simulation(row.get("vp_policy_side"))
    row_direction = str(row.get("direction") or "").strip().lower()
    pnl_points = _to_optional_float(row.get("pnl_points"))

    if policy_side not in {"long", "short"}:
        return None
    if row_direction not in {"long", "short"}:
        return None
    if pnl_points is None:
        return None

    if row_direction == policy_side:
        return pnl_points > 0
    return pnl_points < 0


def _build_vp_policy_reason_quality_stats(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    buckets: dict[str, list[Dict[str, Any]]] = {}
    for row in rows:
        policy = str(row.get("vp_policy_reason") or "").strip()
        if not policy:
            continue
        buckets.setdefault(policy, []).append(row)

    stats: list[Dict[str, Any]] = []
    for policy, bucket_rows in buckets.items():
        sample_count = len(bucket_rows)
        pnl_values = [_to_optional_float(r.get("pnl_points")) for r in bucket_rows]
        pnl_values = [pnl for pnl in pnl_values if pnl is not None]

        avg_pnl = (sum(pnl_values) / len(pnl_values)) if pnl_values else 0.0

        direction_correct_flags = [
            flag for flag in (_is_vp_policy_direction_correct_for_simulation(r) for r in bucket_rows) if flag is not None
        ]
        direction_correct_rate = (
            sum(1 for flag in direction_correct_flags if flag) / len(direction_correct_flags)
            if direction_correct_flags
            else 0.0
        )

        stats.append(
            {
                "policy": policy,
                "sample_count": sample_count,
                "expectancy": avg_pnl,
                "direction_correct_rate": direction_correct_rate,
                "score": avg_pnl * direction_correct_rate,
            }
        )

    return stats


def _select_top_vp_policy_reason_from_history(
    history_rows: list[Dict[str, Any]],
    *,
    min_count: int,
) -> tuple[Optional[str], Optional[float]]:
    eligible_stats = [
        stat
        for stat in _build_vp_policy_reason_quality_stats(history_rows)
        if int(stat.get("sample_count") or 0) >= min_count
    ]
    if not eligible_stats:
        return None, None

    eligible_stats.sort(
        key=lambda stat: (
            -float(stat.get("score") or 0.0),
            -float(stat.get("expectancy") or 0.0),
            -float(stat.get("direction_correct_rate") or 0.0),
            str(stat.get("policy") or ""),
        )
    )
    top = eligible_stats[0]
    return str(top.get("policy") or ""), float(top.get("score") or 0.0)


def _build_vp_policy_selector_step(
    row: Dict[str, Any],
    *,
    step_index: int,
    selected_policy: Optional[str],
    selected_score: Optional[float],
) -> Dict[str, Any]:
    observed_policy = str(row.get("vp_policy_reason") or "").strip() or None
    selected = selected_policy is not None and observed_policy == selected_policy
    step_pnl_points = _to_optional_float(row.get("pnl_points")) if selected else None
    direction_correct = _is_vp_policy_direction_correct_for_simulation(row) if selected else None

    return {
        "step_index": step_index,
        "evaluated_at": row.get("evaluated_at"),
        "selected_policy": selected_policy,
        "selected_score": selected_score,
        "observed_policy": observed_policy,
        "selected": selected,
        "pnl_points": step_pnl_points,
        "direction_correct": direction_correct,
    }


def _build_vp_policy_baseline_summary(
    rows: list[Dict[str, Any]],
    *,
    baseline_policy: Optional[str],
) -> Dict[str, Any]:
    if baseline_policy is None:
        return {
            "baseline_policy": None,
            "baseline_total_selections": 0,
            "baseline_wins": 0,
            "baseline_losses": 0,
            "baseline_expectancy": 0.0,
            "baseline_cumulative_pnl_points": 0.0,
        }

    baseline_pnl_points: list[float] = []
    baseline_wins = 0
    baseline_losses = 0
    baseline_total_selections = 0

    for row in rows:
        observed_policy = str(row.get("vp_policy_reason") or "").strip()
        if observed_policy != baseline_policy:
            continue

        baseline_total_selections += 1

        direction_correct = _is_vp_policy_direction_correct_for_simulation(row)
        if direction_correct is True:
            baseline_wins += 1
        elif direction_correct is False:
            baseline_losses += 1

        pnl_points = _to_optional_float(row.get("pnl_points"))
        if pnl_points is not None:
            baseline_pnl_points.append(pnl_points)

    baseline_cumulative_pnl_points = float(sum(baseline_pnl_points))
    baseline_expectancy = (
        baseline_cumulative_pnl_points / len(baseline_pnl_points)
        if baseline_pnl_points
        else 0.0
    )

    return {
        "baseline_policy": baseline_policy,
        "baseline_total_selections": baseline_total_selections,
        "baseline_wins": baseline_wins,
        "baseline_losses": baseline_losses,
        "baseline_expectancy": baseline_expectancy,
        "baseline_cumulative_pnl_points": baseline_cumulative_pnl_points,
    }


def _accumulate_selector_step_metrics(
    step: Dict[str, Any],
    *,
    selected_pnl_points: list[float],
    simulated_wins: int,
    simulated_losses: int,
) -> tuple[int, int]:
    if not step.get("selected"):
        return simulated_wins, simulated_losses

    direction_correct = step.get("direction_correct")
    if direction_correct is True:
        simulated_wins += 1
    elif direction_correct is False:
        simulated_losses += 1

    pnl_points = _to_optional_float(step.get("pnl_points"))
    if pnl_points is not None:
        selected_pnl_points.append(pnl_points)

    return simulated_wins, simulated_losses


def _compute_selector_switch_summary(steps: list[Dict[str, Any]]) -> Dict[str, Any]:
    selected_policies = [
        str(step.get("selected_policy"))
        for step in steps
        if step.get("selected") and step.get("selected_policy") is not None
    ]

    total_selections = len(selected_policies)
    policy_switches = sum(
        1
        for previous_policy, current_policy in zip(selected_policies, selected_policies[1:])
        if previous_policy != current_policy
    )
    switch_rate = (policy_switches / (total_selections - 1)) if total_selections >= 2 else 0.0

    return {
        "total_selections": total_selections,
        "policy_switches": policy_switches,
        "switch_rate": switch_rate,
    }


def _compute_selector_delta_summary(
    *,
    simulated_wins: int,
    simulated_losses: int,
    simulated_expectancy: float,
    cumulative_pnl_points: float,
    baseline_summary: Dict[str, Any],
) -> Dict[str, float]:
    baseline_expectancy = float(baseline_summary.get("baseline_expectancy") or 0.0)
    baseline_cumulative_pnl_points = float(baseline_summary.get("baseline_cumulative_pnl_points") or 0.0)
    baseline_wins = int(baseline_summary.get("baseline_wins") or 0)
    baseline_losses = int(baseline_summary.get("baseline_losses") or 0)

    simulated_labeled_total = simulated_wins + simulated_losses
    baseline_labeled_total = baseline_wins + baseline_losses
    simulated_win_rate = (simulated_wins / simulated_labeled_total) if simulated_labeled_total else 0.0
    baseline_win_rate = (baseline_wins / baseline_labeled_total) if baseline_labeled_total else 0.0

    return {
        "expectancy_delta_vs_baseline": simulated_expectancy - baseline_expectancy,
        "cumulative_pnl_delta_vs_baseline": cumulative_pnl_points - baseline_cumulative_pnl_points,
        "win_rate_delta_vs_baseline": simulated_win_rate - baseline_win_rate,
    }


def _compute_selector_outperformance_summary(
    *,
    cumulative_pnl_delta_vs_baseline: float,
) -> Dict[str, Any]:
    if cumulative_pnl_delta_vs_baseline > 0:
        return {
            "selector_outperformed_baseline": True,
            "selector_outperformance_reason": "higher_cumulative_pnl",
        }
    if cumulative_pnl_delta_vs_baseline < 0:
        return {
            "selector_outperformed_baseline": False,
            "selector_outperformance_reason": "lower_cumulative_pnl",
        }
    return {
        "selector_outperformed_baseline": False,
        "selector_outperformance_reason": "equal_performance",
    }


def _build_selector_compact_summary(
    *,
    outperformance_summary: Dict[str, Any],
    cumulative_pnl_points: float,
    baseline_cumulative_pnl_points: float,
    cumulative_pnl_delta_vs_baseline: float,
    switch_rate: float,
) -> Dict[str, Any]:
    verdict = str(outperformance_summary.get("selector_outperformance_reason") or "equal_performance")

    return {
        "verdict": verdict,
        "selector_outperformed_baseline": bool(outperformance_summary.get("selector_outperformed_baseline")),
        "adaptive_pnl": cumulative_pnl_points,
        "baseline_pnl": baseline_cumulative_pnl_points,
        "pnl_delta": cumulative_pnl_delta_vs_baseline,
        "switch_rate": switch_rate,
    }


def _simulate_vp_policy_reason_selector(rows: list[Dict[str, Any]], *, min_count: int) -> Dict[str, Any]:
    safe_min_count = max(1, int(min_count))
    chronological_rows = sorted(
        rows,
        key=lambda row: (str(row.get("evaluated_at") or ""), int(row.get("id") or 0)),
    )

    history_rows: list[Dict[str, Any]] = []
    steps: list[Dict[str, Any]] = []
    selected_pnl_points: list[float] = []
    simulated_wins = 0
    simulated_losses = 0

    for step_index, row in enumerate(chronological_rows, start=1):
        selected_policy, selected_score = _select_top_vp_policy_reason_from_history(
            history_rows,
            min_count=safe_min_count,
        )
        step = _build_vp_policy_selector_step(
            row,
            step_index=step_index,
            selected_policy=selected_policy,
            selected_score=selected_score,
        )

        simulated_wins, simulated_losses = _accumulate_selector_step_metrics(
            step,
            selected_pnl_points=selected_pnl_points,
            simulated_wins=simulated_wins,
            simulated_losses=simulated_losses,
        )

        steps.append(step)

        history_rows.append(row)

    cumulative_pnl_points = float(sum(selected_pnl_points))
    simulated_expectancy = cumulative_pnl_points / len(selected_pnl_points) if selected_pnl_points else 0.0
    total_steps = len(steps)
    switch_summary = _compute_selector_switch_summary(steps)
    baseline_policy, _ = _select_top_vp_policy_reason_from_history(
        chronological_rows,
        min_count=safe_min_count,
    )
    baseline_summary = _build_vp_policy_baseline_summary(
        chronological_rows,
        baseline_policy=baseline_policy,
    )
    delta_summary = _compute_selector_delta_summary(
        simulated_wins=simulated_wins,
        simulated_losses=simulated_losses,
        simulated_expectancy=simulated_expectancy,
        cumulative_pnl_points=cumulative_pnl_points,
        baseline_summary=baseline_summary,
    )
    cumulative_pnl_delta_vs_baseline = float(delta_summary.get("cumulative_pnl_delta_vs_baseline") or 0.0)
    outperformance_summary = _compute_selector_outperformance_summary(
        cumulative_pnl_delta_vs_baseline=cumulative_pnl_delta_vs_baseline,
    )
    compact_summary = _build_selector_compact_summary(
        outperformance_summary=outperformance_summary,
        cumulative_pnl_points=cumulative_pnl_points,
        baseline_cumulative_pnl_points=float(baseline_summary.get("baseline_cumulative_pnl_points") or 0.0),
        cumulative_pnl_delta_vs_baseline=cumulative_pnl_delta_vs_baseline,
        switch_rate=float(switch_summary.get("switch_rate") or 0.0),
    )

    return {
        "count": total_steps,
        "total_steps": total_steps,
        **switch_summary,
        **baseline_summary,
        "simulated_wins": simulated_wins,
        "simulated_losses": simulated_losses,
        "simulated_expectancy": simulated_expectancy,
        "cumulative_pnl_points": cumulative_pnl_points,
        **delta_summary,
        **outperformance_summary,
        "summary": compact_summary,
        "steps": steps,
    }


@app.get(
    "/execution_outcomes/vp_policy_summary",
    summary="Get execution outcomes VP policy summary",
    responses={
        200: {
            "description": "Compact VP policy summary metrics over stored execution outcomes",
            "content": {"application/json": {"example": EXECUTION_OUTCOMES_VP_POLICY_SUMMARY_RESPONSE_EXAMPLE}},
        }
    },
)
def execution_outcomes_vp_policy_summary(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
):
    try:
        return get_execution_outcomes_vp_policy_summary(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy summary")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy summary",
        )


@app.get(
    "/execution_outcomes/vp_policy_cohorts",
    summary="Get execution outcomes VP policy cohorts",
    responses={
        200: {
            "description": "VP policy cohort rows grouped by stored side and trade bias score",
        }
    },
)
def execution_outcomes_vp_policy_cohorts(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
):
    try:
        rows = get_execution_outcomes_vp_policy_cohorts(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy cohorts")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy cohorts",
        )

    return {
        "count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_cohorts",
    summary="Get execution outcomes VP policy reason cohorts",
    responses={
        200: {
            "description": "VP policy cohort rows grouped by stored policy reason",
        }
    },
)
def execution_outcomes_vp_policy_reason_cohorts(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
):
    try:
        rows = get_execution_outcomes_vp_policy_reason_cohorts(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy reason cohorts")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy reason cohorts",
        )

    return {
        "count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason/policy_rankings",
    summary="Get VP policy reason policy rankings",
    response_model=ExecutionOutcomesVpPolicyReasonPolicyRankingsResponse,
    responses={
        200: {
            "description": "Quality-ranked VP policy reason intelligence with derived wins/losses and expectancy",
        }
    },
)
def execution_outcomes_vp_policy_reason_policy_rankings(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    score: Annotated[Optional[int], Query(ge=0, description="Optional vp_trade_bias_score filter", examples=[2])] = None,
    side: Annotated[Optional[Literal["long", "short"]], Query(description="Optional vp_policy_side filter", examples=["long"])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[int, Query(ge=1, description="Maximum number of ranked policies to return", examples=[10])] = 10,
):
    try:
        rows = get_execution_outcomes_vp_policy_reason_leaderboard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=None if score is None else float(score),
            vp_policy_side=side,
            min_count=min_count,
            limit=limit,
            sort="quality",
        )
    except Exception:
        logger.exception("Failed to fetch VP policy reason policy rankings")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch VP policy reason policy rankings",
        )

    policy_rankings = _build_vp_policy_reason_policy_rankings(rows)

    return {
        "applied_score": score,
        "applied_side": side,
        "is_filtered": score is not None or side is not None,
        "count": len(policy_rankings),
        "policies": policy_rankings,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason/policy_selector_simulation",
    summary="Simulate selecting top VP policy reason over time",
    response_model=ExecutionOutcomesVpPolicyReasonSelectorSimulationResponse,
    responses={
        200: {
            "description": "Stepwise selector replay using top quality-ranked VP policy reason from prior history",
        }
    },
)
def execution_outcomes_vp_policy_reason_policy_selector_simulation(
    limit: Annotated[int, Query(ge=1, le=500, description="Maximum outcomes to replay", examples=[200])] = 200,
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = "evaluated",
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    score: Annotated[Optional[int], Query(ge=0, description="Optional vp_trade_bias_score filter", examples=[2])] = None,
    side: Annotated[Optional[Literal["long", "short"]], Query(description="Optional vp_policy_side filter", examples=["long"])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum historical samples before policy is selectable", examples=[2])] = 2,
):
    _, rows = _load_recent_execution_outcome_rows(
        limit=limit,
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        label=None,
        since=None,
        until=None,
    )

    filtered_rows: list[Dict[str, Any]] = []
    for row in rows:
        policy_reason = str(row.get("vp_policy_reason") or "").strip()
        if not policy_reason:
            continue

        row_score = _to_optional_float(row.get("vp_trade_bias_score"))
        if score is not None and (row_score is None or row_score != float(score)):
            continue

        row_side = _normalize_vp_policy_side_for_simulation(row.get("vp_policy_side"))
        if side is not None and row_side != side:
            continue

        filtered_rows.append(row)

    simulation = _simulate_vp_policy_reason_selector(filtered_rows, min_count=min_count)

    return {
        "applied_score": score,
        "applied_side": side,
        "is_filtered": score is not None or side is not None,
        **simulation,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_best",
    summary="Get best execution outcomes VP policy reasons",
    response_model=ExecutionOutcomesVpPolicyReasonBestResponse,
    responses={
        200: {
            "description": "Top VP policy reason rows using quality-first ranking with default cohort and result limits",
        }
    },
)
def execution_outcomes_vp_policy_reason_best(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    score: Annotated[Optional[int], Query(ge=0, description="Optional vp_trade_bias_score filter", examples=[2])] = None,
    side: Annotated[Optional[Literal["long", "short"]], Query(description="Optional vp_policy_side filter", examples=["long"])] = None,
    since_days: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in days based on latest stored evaluated_at", examples=[30])] = None,
    since_trades: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in most recent trades", examples=[5000])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[int, Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = 5,
):
    try:
        rows = get_execution_outcomes_vp_policy_reason_leaderboard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=None if score is None else float(score),
            vp_policy_side=side,
            since_days=since_days,
            since_trades=since_trades,
            min_count=min_count,
            limit=limit,
            sort="quality",
        )
    except Exception:
        logger.exception("Failed to fetch best execution outcomes VP policy reasons")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch best execution outcomes VP policy reasons",
        )

    return {
        "applied_score": score,
        "applied_side": side,
        "applied_since_days": since_days,
        "applied_since_trades": since_trades,
        "is_filtered": score is not None or side is not None,
        "count": len(rows),
        "best_count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_worst",
    summary="Get worst execution outcomes VP policy reasons",
    response_model=ExecutionOutcomesVpPolicyReasonWorstResponse,
    responses={
        200: {
            "description": "Lowest-quality VP policy reason rows using quality-first ranking with default cohort and result limits",
        }
    },
)
def execution_outcomes_vp_policy_reason_worst(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    score: Annotated[Optional[int], Query(ge=0, description="Optional vp_trade_bias_score filter", examples=[2])] = None,
    side: Annotated[Optional[Literal["long", "short"]], Query(description="Optional vp_policy_side filter", examples=["long"])] = None,
    since_days: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in days based on latest stored evaluated_at", examples=[30])] = None,
    since_trades: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in most recent trades", examples=[5000])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[int, Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = 5,
):
    try:
        rows = get_execution_outcomes_vp_policy_reason_laggards(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=None if score is None else float(score),
            vp_policy_side=side,
            since_days=since_days,
            since_trades=since_trades,
            min_count=min_count,
            limit=limit,
            sort="quality",
        )
    except Exception:
        logger.exception("Failed to fetch worst execution outcomes VP policy reasons")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch worst execution outcomes VP policy reasons",
        )

    return {
        "applied_score": score,
        "applied_side": side,
        "applied_since_days": since_days,
        "applied_since_trades": since_trades,
        "is_filtered": score is not None or side is not None,
        "count": len(rows),
        "worst_count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_best_worst",
    summary="Get best and worst execution outcomes VP policy reasons",
    response_model=ExecutionOutcomesVpPolicyReasonBestWorstResponse,
    responses={
        200: {
            "description": "Best and worst VP policy reason rows using quality-first ranking with default cohort and result limits",
        }
    },
)
def execution_outcomes_vp_policy_reason_best_worst(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    score: Annotated[Optional[int], Query(ge=0, description="Optional vp_trade_bias_score filter", examples=[2])] = None,
    side: Annotated[Optional[Literal["long", "short"]], Query(description="Optional vp_policy_side filter", examples=["long"])] = None,
    since_days: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in days based on latest stored evaluated_at", examples=[30])] = None,
    since_trades: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in most recent trades", examples=[5000])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[int, Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = 5,
):
    safe_min_count = max(1, int(min_count))
    try:
        cohort_rows = get_execution_outcomes_vp_policy_reason_cohorts(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=None if score is None else float(score),
            vp_policy_side=side,
            since_days=since_days,
            since_trades=since_trades,
        )
        eligible_reason_cohorts = sum(
            1 for row in cohort_rows if int(row.get("row_count") or 0) >= safe_min_count
        )
        best = get_execution_outcomes_vp_policy_reason_leaderboard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=None if score is None else float(score),
            vp_policy_side=side,
            since_days=since_days,
            since_trades=since_trades,
            min_count=safe_min_count,
            limit=limit,
            sort="quality",
        )
        worst = get_execution_outcomes_vp_policy_reason_laggards(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=None if score is None else float(score),
            vp_policy_side=side,
            since_days=since_days,
            since_trades=since_trades,
            min_count=safe_min_count,
            limit=limit,
            sort="quality",
        )
    except Exception:
        logger.exception("Failed to fetch best and worst execution outcomes VP policy reasons")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch best and worst execution outcomes VP policy reasons",
        )

    return {
        "applied_score": score,
        "applied_side": side,
        "applied_since_days": since_days,
        "applied_since_trades": since_trades,
        "is_filtered": score is not None or side is not None,
        "min_count_applied": safe_min_count,
        "total_reason_cohorts": len(cohort_rows),
        "eligible_reason_cohorts": eligible_reason_cohorts,
        "best_count": len(best),
        "worst_count": len(worst),
        "best": best,
        "worst": worst,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_monitor",
    summary="Get monitor summary for execution outcomes VP policy reason quality",
    response_model=ExecutionOutcomesVpPolicyReasonMonitorResponse,
    responses={
        200: {
            "description": "Compact monitor summary built on the quality-ranked best/worst VP policy reason surface",
        }
    },
)
def execution_outcomes_vp_policy_reason_monitor(
    score: Annotated[Optional[int], Query(ge=0, description="Optional vp_trade_bias_score filter", examples=[2])] = None,
    side: Annotated[Optional[Literal["long", "short"]], Query(description="Optional vp_policy_side filter", examples=["long"])] = None,
    since_days: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in days based on latest stored evaluated_at", examples=[30])] = None,
    since_trades: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in most recent trades", examples=[5000])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[int, Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = 5,
):
    best_worst_payload = execution_outcomes_vp_policy_reason_best_worst(
        score=score,
        side=side,
        since_days=since_days,
        since_trades=since_trades,
        min_count=min_count,
        limit=limit,
    )

    best = list(best_worst_payload.get("best") or [])
    worst = list(best_worst_payload.get("worst") or [])
    best_count = int(best_worst_payload.get("best_count") or 0)
    worst_count = int(best_worst_payload.get("worst_count") or 0)
    eligible_reason_cohorts = int(best_worst_payload.get("eligible_reason_cohorts") or 0)

    top_quality_score = _to_optional_float(best[0].get("quality_score")) if best else None
    bottom_quality_score = _to_optional_float(worst[0].get("quality_score")) if worst else None
    quality_spread = (
        top_quality_score - bottom_quality_score
        if top_quality_score is not None and bottom_quality_score is not None
        else None
    )

    if best_count == 0 and worst_count == 0:
        monitor_status: Literal["empty", "thin", "healthy"] = "empty"
    elif eligible_reason_cohorts < 3:
        monitor_status = "thin"
    else:
        monitor_status = "healthy"

    return {
        "applied_score": best_worst_payload.get("applied_score"),
        "applied_side": best_worst_payload.get("applied_side"),
        "applied_since_days": best_worst_payload.get("applied_since_days"),
        "applied_since_trades": best_worst_payload.get("applied_since_trades"),
        "min_count_applied": int(best_worst_payload.get("min_count_applied") or 1),
        "total_reason_cohorts": int(best_worst_payload.get("total_reason_cohorts") or 0),
        "eligible_reason_cohorts": eligible_reason_cohorts,
        "best_count": best_count,
        "worst_count": worst_count,
        "best": best,
        "worst": worst,
        "monitor_status": monitor_status,
        "top_quality_score": top_quality_score,
        "bottom_quality_score": bottom_quality_score,
        "quality_spread": quality_spread,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_leaderboard",
    summary="Get execution outcomes VP policy reason leaderboard",
    responses={
        200: {
            "description": "VP policy reason leaderboard rows filtered to cohorts with at least two samples",
        }
    },
)
def execution_outcomes_vp_policy_reason_leaderboard(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    since_days: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in days based on latest stored evaluated_at", examples=[30])] = None,
    since_trades: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in most recent trades", examples=[5000])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[Optional[int], Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = None,
    sort: Annotated[Literal["pnl", "accuracy", "quality"], Query(description="Ranking priority for returned cohorts", examples=["pnl"])] = "pnl",
):
    try:
        rows = get_execution_outcomes_vp_policy_reason_leaderboard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            since_days=since_days,
            since_trades=since_trades,
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy reason leaderboard")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy reason leaderboard",
        )

    return {
        "count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_laggards",
    summary="Get execution outcomes VP policy reason laggards",
    responses={
        200: {
            "description": "VP policy reason laggard rows filtered to cohorts with at least two samples",
        }
    },
)
def execution_outcomes_vp_policy_reason_laggards(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    since_days: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in days based on latest stored evaluated_at", examples=[30])] = None,
    since_trades: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in most recent trades", examples=[5000])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[Optional[int], Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = None,
    sort: Annotated[Literal["pnl", "accuracy", "quality"], Query(description="Ranking priority for returned cohorts", examples=["pnl"])] = "pnl",
):
    try:
        rows = get_execution_outcomes_vp_policy_reason_laggards(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            since_days=since_days,
            since_trades=since_trades,
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy reason laggards")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy reason laggards",
        )

    return {
        "count": len(rows),
        "rows": rows,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_extremes",
    summary="Get execution outcomes VP policy reason extremes",
    responses={
        200: {
            "description": "VP policy reason leaders and laggards returned together for quick inspection",
        }
    },
)
def execution_outcomes_vp_policy_reason_extremes(
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    since_days: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in days based on latest stored evaluated_at", examples=[30])] = None,
    since_trades: Annotated[Optional[int], Query(ge=1, description="Optional rolling window in most recent trades", examples=[5000])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[Optional[int], Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = None,
    sort: Annotated[Literal["pnl", "accuracy", "quality"], Query(description="Ranking priority for returned cohorts", examples=["pnl"])] = "pnl",
):
    try:
        leaders = get_execution_outcomes_vp_policy_reason_leaderboard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            since_days=since_days,
            since_trades=since_trades,
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
        laggards = get_execution_outcomes_vp_policy_reason_laggards(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            since_days=since_days,
            since_trades=since_trades,
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy reason extremes")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy reason extremes",
        )

    return {
        "leaders": leaders,
        "laggards": laggards,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_extremes_by_score/{score}",
    summary="Get execution outcomes VP policy reason extremes for one score band",
    responses={
        200: {
            "description": "VP policy reason leaders and laggards filtered to one stored vp_trade_bias_score band",
        }
    },
)
def execution_outcomes_vp_policy_reason_extremes_by_score(
    score: Annotated[int, Path(ge=0, description="Requested vp_trade_bias_score band", examples=[2])],
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[Optional[int], Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = None,
    sort: Annotated[Literal["pnl", "accuracy", "quality"], Query(description="Ranking priority for returned cohorts", examples=["pnl"])] = "pnl",
):
    try:
        leaders = get_execution_outcomes_vp_policy_reason_leaderboard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=float(score),
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
        laggards = get_execution_outcomes_vp_policy_reason_laggards(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=float(score),
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy reason extremes by score")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy reason extremes by score",
        )

    return {
        "leaders": leaders,
        "laggards": laggards,
    }


@app.get(
    "/execution_outcomes/vp_policy_reason_extremes_by_score/{score}/{side}",
    summary="Get execution outcomes VP policy reason extremes for one score band and side",
    responses={
        200: {
            "description": "VP policy reason leaders and laggards filtered to one stored vp_trade_bias_score band and one policy side",
        }
    },
)
def execution_outcomes_vp_policy_reason_extremes_by_score_and_side(
    score: Annotated[int, Path(ge=0, description="Requested vp_trade_bias_score band", examples=[2])],
    side: Annotated[Literal["long", "short"], Path(description="Requested vp_policy_side", examples=["long"])],
    worker_id: Annotated[Optional[str], Query(min_length=1, examples=["worker-a"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1, examples=["evaluated"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    min_count: Annotated[int, Query(ge=1, description="Minimum cohort sample count", examples=[2])] = 2,
    limit: Annotated[Optional[int], Query(ge=1, description="Maximum number of cohorts to return", examples=[5])] = None,
    sort: Annotated[Literal["pnl", "accuracy", "quality"], Query(description="Ranking priority for returned cohorts", examples=["pnl"])] = "pnl",
):
    try:
        leaders = get_execution_outcomes_vp_policy_reason_leaderboard(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=float(score),
            vp_policy_side=side,
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
        laggards = get_execution_outcomes_vp_policy_reason_laggards(
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            signal_id=signal_id,
            vp_trade_bias_score=float(score),
            vp_policy_side=side,
            min_count=min_count,
            limit=limit,
            sort=sort,
        )
    except Exception:
        logger.exception("Failed to fetch execution outcomes VP policy reason extremes by score and side")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch execution outcomes VP policy reason extremes by score and side",
        )

    return {
        "leaders": leaders,
        "laggards": laggards,
    }


@app.get(
    "/execution_outcomes/export.csv",
    summary="Export execution outcomes as CSV",
    responses={
        200: {
            "description": "CSV export of recent execution outcomes with derived labels",
        }
    },
)
def execution_outcomes_export_csv(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[500])] = 500,
    worker_id: Annotated[Optional[str], Query(min_length=1)] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    since: Annotated[Optional[datetime], Query()] = None,
    until: Annotated[Optional[datetime], Query()] = None,
):
    _, rows = _load_recent_execution_outcome_rows(
        limit=limit,
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        label=label,
        since=since,
        until=until,
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXECUTION_OUTCOMES_EXPORT_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in EXECUTION_OUTCOMES_EXPORT_FIELDS})

    return Response(
        content=output.getvalue(),
        media_type=CSV_MEDIA_TYPE,
        headers={"Content-Disposition": "attachment; filename=execution_outcomes_export.csv"},
    )


@app.get(
    "/execution_outcomes/policy_recommendation.csv",
    summary="Export policy recommendation rows as CSV",
    responses={
        200: {
            "description": "CSV export of selected policy recommendation cohorts",
        }
    },
)
def execution_outcomes_policy_recommendation_csv(
    group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(description="Cohort grouping key", examples=["strategy"]),
    ] = "strategy",
    since: Annotated[Optional[datetime], Query()] = None,
    until: Annotated[Optional[datetime], Query()] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1)] = 1,
    top_n: Annotated[int, Query(ge=1, le=500)] = 1,
    scoring_mode: Annotated[
        Literal["expectancy_pct", "expectancy_points", "win_rate", "avg_pnl_pct", "blended"],
        Query(),
    ] = "blended",
):
    recommendation = _load_execution_outcomes_policy_recommendation(
        group_by=group_by,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        top_n=top_n,
        scoring_mode=scoring_mode,
    )

    rows = recommendation["rows"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXECUTION_OUTCOMES_POLICY_RECOMMENDATION_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in EXECUTION_OUTCOMES_POLICY_RECOMMENDATION_FIELDS})

    return Response(
        content=output.getvalue(),
        media_type=CSV_MEDIA_TYPE,
        headers={"Content-Disposition": "attachment; filename=execution_outcomes_policy_recommendation.csv"},
    )


@app.get(
    "/execution_outcomes/policy_audit.csv",
    summary="Export policy audit rows as CSV",
    responses={
        200: {
            "description": "CSV export of row-level policy audit results",
        }
    },
)
def execution_outcomes_policy_audit_csv(
    group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(description="Cohort grouping key", examples=["strategy"]),
    ] = "strategy",
    since: Annotated[Optional[datetime], Query()] = None,
    until: Annotated[Optional[datetime], Query()] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1, le=5000)] = 1,
    audit_step_size: Annotated[int, Query(ge=1, le=5000)] = 1,
    audit_horizon_samples: Annotated[int, Query(ge=1, le=5000)] = 10,
    top_n: Annotated[int, Query(ge=1, le=500)] = 1,
    scoring_mode: Annotated[
        Literal["expectancy_pct", "expectancy_points", "win_rate", "avg_pnl_pct", "blended"],
        Query(),
    ] = "blended",
):
    audit = _load_execution_outcomes_policy_audit(
        group_by=group_by,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        audit_step_size=audit_step_size,
        audit_horizon_samples=audit_horizon_samples,
        top_n=top_n,
        scoring_mode=scoring_mode,
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXECUTION_OUTCOMES_POLICY_AUDIT_FIELDS)
    writer.writeheader()
    for row in audit["rows"]:
        writer.writerow({field: row.get(field) for field in EXECUTION_OUTCOMES_POLICY_AUDIT_FIELDS})

    return Response(
        content=output.getvalue(),
        media_type=CSV_MEDIA_TYPE,
        headers={"Content-Disposition": "attachment; filename=execution_outcomes_policy_audit.csv"},
    )


@app.get(
    "/execution_outcomes/leaderboard.csv",
    summary="Export outcomes cohort leaderboard as CSV",
    responses={
        200: {
            "description": "CSV export of grouped outcomes leaderboard",
        }
    },
)
def execution_outcomes_leaderboard_csv(
    group_by: Annotated[
        Literal["strategy", "source", "setup_family", "worker_id", "symbol", "direction"],
        Query(description="Cohort grouping key", examples=["worker_id"]),
    ],
    since: Annotated[Optional[datetime], Query()] = None,
    until: Annotated[Optional[datetime], Query()] = None,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    worker_id: Annotated[Optional[str], Query(min_length=1)] = None,
    direction: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(min_length=1)] = None,
    label: Annotated[Optional[Literal["winner", "loser", "scratch", "unknown"]], Query()] = None,
    min_samples: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=500)] = 500,
):
    rows = _load_execution_outcomes_leaderboard(
        group_by=group_by,
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        label=label,
        since=since,
        until=until,
        min_samples=min_samples,
        limit=limit,
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXECUTION_OUTCOMES_LEADERBOARD_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in EXECUTION_OUTCOMES_LEADERBOARD_FIELDS})

    return Response(
        content=output.getvalue(),
        media_type=CSV_MEDIA_TYPE,
        headers={"Content-Disposition": "attachment; filename=execution_outcomes_leaderboard.csv"},
    )


@app.get(
    "/trade_candidates/recent",
    summary="Get recent trade candidates",
    description=RECENT_TRADE_CANDIDATES_DESCRIPTION,
    responses={
        200: {
            "description": "Recent trade candidates",
            "content": {"application/json": {"example": TRADE_CANDIDATE_RECENT_RESPONSE_EXAMPLE}},
        }
    },
)
def trade_candidates_recent(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=[EXAMPLE_SYMBOL])] = None,
    timeframe: Annotated[Optional[str], Query(min_length=1, examples=["1m"])] = None,
    direction: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    strategy: Annotated[Optional[str], Query(min_length=1, examples=["adaptive-v2"])] = None,
    source: Annotated[Optional[str], Query(min_length=1, examples=["webhook"])] = None,
    event_type: Annotated[Optional[str], Query(min_length=1, examples=["continuation"])] = None,
    signal_id: Annotated[Optional[str], Query(min_length=1, examples=["sig-btc-20260310-001"])] = None,
    derived_from_event: Annotated[Optional[bool], Query(description="Filter derived rows only")] = None,
    execution_status: Annotated[Optional[str], Query(examples=["pending", "filled"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_direction: Optional[str]
    if direction is None:
        normalized_direction = None
    else:
        try:
            normalized_direction = _normalize_direction(direction)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    try:
        rows = get_recent_trade_candidates(
            capped_limit,
            symbol=symbol,
            timeframe=timeframe,
            direction=normalized_direction,
            strategy=strategy,
            source=source,
            event_type=event_type,
            signal_id=signal_id,
            derived_from_event=derived_from_event,
            execution_status=execution_status,
        )
    except Exception:
        logger.exception("Failed to fetch recent trade candidates")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent trade candidates",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get(
    "/webhooks/events/recent",
    summary="Get recent raw TradingView webhook events",
)
def webhooks_events_recent(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    source: Annotated[Optional[str], Query(min_length=1, examples=["tradingview"])] = None,
    event_id: Annotated[Optional[str], Query(min_length=1, examples=["tv_abc123"])] = None,
    payload_hash: Annotated[Optional[str], Query(min_length=1)] = None,
):
    capped_limit = min(limit, 500)

    try:
        rows = get_recent_raw_webhook_events(
            capped_limit,
            source=source,
            event_id=event_id,
            payload_hash=payload_hash,
        )
    except Exception:
        logger.exception("Failed to fetch recent raw webhook events")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent raw webhook events",
        )

    shaped_rows = [_shape_raw_webhook_event_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/signals/recent",
    summary="Get recent normalized signals",
)
def signals_recent(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    event_id: Annotated[Optional[str], Query(min_length=1, examples=["tv_abc123"])] = None,
    normalized_id: Annotated[Optional[str], Query(min_length=1, examples=["sig_abc123"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=["BTCUSDT"])] = None,
    timeframe: Annotated[Optional[str], Query(min_length=1, examples=["1m"])] = None,
    side: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    signal_name: Annotated[Optional[str], Query(min_length=1, examples=["vp_breakout_long"])] = None,
    strategy_id: Annotated[Optional[str], Query(min_length=1, examples=["smart_algo_v1"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        try:
            normalized_side = _normalize_direction(side)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    try:
        rows = get_recent_normalized_signals(
            capped_limit,
            event_id=event_id,
            normalized_id=normalized_id,
            symbol=symbol,
            timeframe=timeframe,
            side=normalized_side,
            signal_name=signal_name,
            strategy_id=strategy_id,
        )
    except Exception:
        logger.exception("Failed to fetch recent normalized signals")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent normalized signals",
        )

    shaped_rows = [_shape_normalized_signal_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/decisions/recent",
    summary="Get recent strategy and risk decisions",
)
def decisions_recent(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    event_id: Annotated[Optional[str], Query(min_length=1, examples=["tv_abc123"])] = None,
    normalized_id: Annotated[Optional[str], Query(min_length=1, examples=["sig_abc123"])] = None,
    strategy_decision_id: Annotated[Optional[str], Query(min_length=1, examples=["strat_abc123"])] = None,
    strategy_decision: Annotated[Optional[str], Query(min_length=1, examples=["approve"])] = None,
    risk_decision: Annotated[Optional[str], Query(min_length=1, examples=["approve"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=["BTCUSDT"])] = None,
    side: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        try:
            normalized_side = _normalize_direction(side)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    try:
        rows = get_recent_strategy_risk_decisions(
            capped_limit,
            event_id=event_id,
            normalized_id=normalized_id,
            strategy_decision_id=strategy_decision_id,
            strategy_decision=strategy_decision,
            risk_decision=risk_decision,
            symbol=symbol,
            side=normalized_side,
        )
    except Exception:
        logger.exception("Failed to fetch recent strategy and risk decisions")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent strategy and risk decisions",
        )

    shaped_rows = [_shape_strategy_risk_decision_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/execution/requests/recent",
    summary="Get recent simulated execution requests",
)
def execution_requests_recent(
    limit: Annotated[int, Query(ge=1, description="Max rows to return (capped at 500)", examples=[50])] = 50,
    event_id: Annotated[Optional[str], Query(min_length=1, examples=["tv_abc123"])] = None,
    normalized_id: Annotated[Optional[str], Query(min_length=1, examples=["sig_abc123"])] = None,
    strategy_decision_id: Annotated[Optional[str], Query(min_length=1, examples=["strat_abc123"])] = None,
    risk_event_id: Annotated[Optional[str], Query(min_length=1, examples=["risk_abc123"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=["BTCUSDT"])] = None,
    side: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    mode: Annotated[Optional[str], Query(min_length=1, examples=["simulated"])] = None,
    execution_status: Annotated[Optional[str], Query(min_length=1, examples=["ready_simulated"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        try:
            normalized_side = _normalize_direction(side)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    try:
        rows = get_recent_execution_requests(
            capped_limit,
            event_id=event_id,
            normalized_id=normalized_id,
            strategy_decision_id=strategy_decision_id,
            risk_event_id=risk_event_id,
            symbol=symbol,
            side=normalized_side,
            mode=mode,
            execution_status=execution_status,
        )
    except Exception:
        logger.exception("Failed to fetch recent execution requests")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent execution requests",
        )

    shaped_rows = [_shape_execution_request_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/orders/recent",
    summary="Get recent simulated broker orders",
    responses={
        200: {
            "description": "Recent simulated broker order lifecycle rows",
        },
        422: {
            "description": "Invalid side value",
        },
        500: {
            "description": "Internal server error fetching broker orders",
        },
    },
)
def orders_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    event_id: Annotated[Optional[str], Query(min_length=1, examples=["evt_abc123"])] = None,
    execution_request_id: Annotated[Optional[str], Query(min_length=1, examples=["exec_abc123"])] = None,
    order_id: Annotated[Optional[str], Query(min_length=1, examples=["ord_abc123"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=["BTCUSDT"])] = None,
    side: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    status: Annotated[Optional[str], Query(min_length=1, examples=["filled"])] = None,
    mode: Annotated[Optional[str], Query(min_length=1, examples=["simulated"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        try:
            normalized_side = _normalize_direction(side)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        rows = get_recent_broker_orders(
            capped_limit,
            event_id=event_id,
            execution_request_id=execution_request_id,
            order_id=order_id,
            symbol=symbol,
            side=normalized_side,
            status=status,
            mode=mode,
        )
    except Exception:
        logger.exception("Failed to fetch recent broker orders")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch recent broker orders",
        )

    shaped_rows = [_shape_broker_order_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/fills/recent",
    summary="Get recent simulated fills",
    responses={
        200: {
            "description": "Recent simulated fill events",
        }
    },
)
def fills_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    event_id: Annotated[Optional[str], Query(min_length=1, examples=["evt_abc123"])] = None,
    execution_request_id: Annotated[Optional[str], Query(min_length=1, examples=["exec_abc123"])] = None,
    order_id: Annotated[Optional[str], Query(min_length=1, examples=["ord_abc123"])] = None,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=["BTCUSDT"])] = None,
    side: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        try:
            normalized_side = _normalize_direction(side)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    try:
        rows = get_recent_fills(
            capped_limit,
            event_id=event_id,
            execution_request_id=execution_request_id,
            order_id=order_id,
            symbol=symbol,
            side=normalized_side,
        )
    except Exception:
        logger.exception("Failed to fetch recent fills")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent fills",
        )

    shaped_rows = [_shape_fill_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/positions/open",
    summary="Get current open simulated positions",
    responses={
        200: {
            "description": "Open simulated positions",
        }
    },
)
def positions_open(
    limit: Annotated[int, Query(ge=1)] = 50,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=["BTCUSDT"])] = None,
    side: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        try:
            normalized_side = _normalize_direction(side)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    try:
        rows = get_recent_positions(
            capped_limit,
            symbol=symbol,
            side=normalized_side,
            open_only=True,
        )
    except Exception:
        logger.exception("Failed to fetch open positions")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch open positions",
        )

    shaped_rows = [_shape_position_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.get(
    "/positions/recent",
    summary="Get recent simulated positions",
    responses={
        200: {
            "description": "Recent simulated positions across open and closed states",
        }
    },
)
def positions_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    symbol: Annotated[Optional[str], Query(min_length=1, examples=["BTCUSDT"])] = None,
    side: Annotated[Optional[str], Query(min_length=1, examples=["long"])] = None,
    status_filter: Annotated[Optional[str], Query(alias="status", min_length=1, examples=["open", "closed"])] = None,
):
    capped_limit = min(limit, 500)

    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        try:
            normalized_side = _normalize_direction(side)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    if status_filter is not None and status_filter not in {"open", "closed"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="status must be 'open' or 'closed'")

    try:
        rows = get_recent_positions(
            capped_limit,
            symbol=symbol,
            side=normalized_side,
            status=status_filter,
            open_only=False,
        )
    except Exception:
        logger.exception("Failed to fetch recent positions")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent positions",
        )

    shaped_rows = [_shape_position_row(row) for row in rows]
    return {
        "count": len(shaped_rows),
        "limit": capped_limit,
        "rows": shaped_rows,
    }


@app.post("/webhook/tradingview", status_code=status.HTTP_201_CREATED)
async def tradingview_webhook(payload: TradingViewPayload, request: Request):
    payload_dump = payload.model_dump(mode="json")
    raw_payload = payload.payload_json if payload.payload_json is not None else payload_dump

    try:
        row_id = insert_bar_state(
            timestamp=payload.timestamp.isoformat(),
            symbol=payload.symbol,
            timeframe=payload.timeframe,
            long_score=payload.long_score,
            short_score=payload.short_score,
            no_trade_score=payload.no_trade_score,
            setup_family=payload.setup_family,
            pressure_index=payload.pressure_index,
            volatility_state=payload.volatility_state,
            participation_score=payload.participation_score,
            confidence_seed=payload.confidence_seed,
            payload_json=raw_payload,
        )
    except Exception:
        logger.exception("Failed to persist TradingView payload")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist webhook payload",
        )

    feature_snapshot_id: Optional[int] = None
    try:
        feature_result = run_feature_pipeline_for_latest_bar(
            symbol=payload.symbol,
            timeframe=payload.timeframe,
            source_bar_id=row_id,
        )
        if feature_result is not None:
            snapshot_candidate = feature_result.get("snapshot_id")
            if isinstance(snapshot_candidate, int):
                feature_snapshot_id = snapshot_candidate
    except Exception:
        logger.exception("Feature pipeline failed for bar_state id=%s", row_id)

    response_payload = {
        "status": "stored",
        "id": row_id,
        "client": request.client.host if request.client else None,
        "symbol": payload.symbol,
        "timeframe": payload.timeframe,
        "timestamp": payload.timestamp.isoformat(),
    }

    if feature_snapshot_id is not None:
        response_payload["feature_snapshot_id"] = feature_snapshot_id

    return response_payload


def _coerce_optional_int(value: Any) -> Optional[int]:
    numeric_value = _to_optional_float(value)
    if numeric_value is None:
        return None
    return int(round(numeric_value))


def _bound_confidence(value: float) -> float:
    return max(0.05, min(0.98, value))


def _extract_signal_market_bias_payload(strategy_payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = strategy_payload.get("signal_market_bias")
    return payload if isinstance(payload, dict) else {}


def _apply_market_bias_to_strategy_decision(
    *,
    decision: str,
    reason_code: str,
    confidence: float,
    bias_payload: Dict[str, Any],
) -> tuple[str, str, float]:
    sample_count = _coerce_optional_int(bias_payload.get("sample_count")) or 0
    if str(bias_payload.get("status") or "") != "ready" or sample_count < 20:
        return decision, reason_code, _bound_confidence(confidence)

    continuation_bias = _to_optional_float(bias_payload.get("continuation_bias")) or 0.5
    reversion_bias = _to_optional_float(bias_payload.get("reversion_bias")) or 0.5

    next_decision = decision
    next_reason_code = reason_code
    next_confidence = confidence

    if reversion_bias >= 0.62:
        if decision == "approve":
            next_decision = "downgrade"
        elif decision == "downgrade":
            next_decision = "reject"
        next_reason_code = "historical_reversion_risk"
        next_confidence -= 0.12
    elif continuation_bias >= 0.62:
        if decision == "downgrade":
            next_decision = "approve"
            next_reason_code = "historical_continuation_support"
        next_confidence += 0.10

    return next_decision, next_reason_code, _bound_confidence(next_confidence)


def _apply_market_bias_to_risk_payload(
    *,
    risk_decision: str,
    reason_code: str,
    position_size: float,
    risk_profile: str,
    bias_payload: Dict[str, Any],
) -> tuple[str, str, float, str]:
    sample_count = _coerce_optional_int(bias_payload.get("sample_count")) or 0
    if str(bias_payload.get("status") or "") != "ready" or sample_count < 20:
        return risk_decision, reason_code, position_size, risk_profile

    continuation_bias = _to_optional_float(bias_payload.get("continuation_bias")) or 0.5
    reversion_bias = _to_optional_float(bias_payload.get("reversion_bias")) or 0.5

    if reversion_bias >= 0.65 and risk_decision == "approve":
        return "resize", "historical_reversion_resize", max(0.05, position_size * 0.5), "reduced_historical"

    if continuation_bias >= 0.65 and risk_decision == "approve":
        return "approve", "risk_checks_passed_historical_support", min(1.0, position_size * 1.15), "historical_support"

    return risk_decision, reason_code, position_size, risk_profile


def _build_phase_one_strategy_decision_payload(normalized_signal: Dict[str, Any]) -> Dict[str, Any]:
    score_value = _to_optional_float(normalized_signal.get("score"))
    score_filter = _coerce_optional_int(score_value)
    side_value = str(normalized_signal.get("side") or "").strip().lower()
    side_map: dict[str, Literal["long", "short"]] = {"long": "long", "short": "short"}
    side_filter: Optional[Literal["long", "short"]] = side_map.get(side_value)

    monitor_payload = execution_outcomes_vp_policy_reason_monitor(
        score=score_filter,
        side=side_filter,
        since_days=30,
        since_trades=5000,
        min_count=2,
        limit=5,
    )
    monitor_status = str(monitor_payload.get("monitor_status") or "empty")
    signal_market_bias = compute_market_bias_preview_for_normalized_signal(
        normalized_signal=normalized_signal
    )

    if monitor_status == "empty":
        decision = "defer"
        reason_code = "reason_monitor_empty"
        confidence = 0.25
    elif monitor_status == "thin":
        decision = "downgrade"
        reason_code = "reason_monitor_thin"
        confidence = 0.45
    elif score_value is None or score_value < 1.0:
        decision = "reject"
        reason_code = "score_below_strategy_threshold"
        confidence = 0.30
    else:
        decision = "approve"
        reason_code = "signal_quality_approved"
        confidence = min(0.95, 0.55 + (score_value * 0.08))

    decision, reason_code, confidence = _apply_market_bias_to_strategy_decision(
        decision=decision,
        reason_code=reason_code,
        confidence=confidence,
        bias_payload=signal_market_bias,
    )

    return {
        "decision": decision,
        "reason_code": reason_code,
        "confidence": confidence,
        "policy_version": "vp_policy_v4_bias_bridge",
        "monitor_status": monitor_status,
        "monitor_snapshot": monitor_payload,
        "signal_market_bias": signal_market_bias,
    }


def _build_phase_one_risk_payload(
    normalized_signal: Dict[str, Any],
    strategy_payload: Dict[str, Any],
) -> Dict[str, Any]:
    strategy_decision = str(strategy_payload.get("decision") or "reject")
    score_value = _to_optional_float(normalized_signal.get("score")) or 0.0
    features = normalized_signal.get("features") if isinstance(normalized_signal.get("features"), dict) else {}
    atr_value = _to_optional_float(features.get("atr")) if isinstance(features, dict) else None
    stop_loss_distance = atr_value if atr_value is not None and atr_value > 0 else 150.0
    take_profit_distance = stop_loss_distance * 2.0

    if strategy_decision in {"reject", "defer"}:
        return {
            "risk_decision": "deny",
            "reason_code": "strategy_gate_blocked",
            "position_size": 0.0,
            "stop_loss_distance": stop_loss_distance,
            "take_profit_distance": take_profit_distance,
            "risk_profile": "blocked",
        }

    if score_value < 1.0:
        return {
            "risk_decision": "deny",
            "reason_code": "score_below_risk_threshold",
            "position_size": 0.0,
            "stop_loss_distance": stop_loss_distance,
            "take_profit_distance": take_profit_distance,
            "risk_profile": "blocked",
        }

    base_size = max(0.10, min(1.0, score_value / 10.0))
    risk_decision = "approve"
    reason_code = "risk_checks_passed"
    risk_profile = "normal"

    if strategy_decision == "downgrade":
        risk_decision = "resize"
        reason_code = "strategy_downgrade_resized"
        risk_profile = "reduced"
        base_size = max(0.05, base_size * 0.5)

    risk_decision, reason_code, base_size, risk_profile = _apply_market_bias_to_risk_payload(
        risk_decision=risk_decision,
        reason_code=reason_code,
        position_size=base_size,
        risk_profile=risk_profile,
        bias_payload=_extract_signal_market_bias_payload(strategy_payload),
    )

    return {
        "risk_decision": risk_decision,
        "reason_code": reason_code,
        "position_size": base_size,
        "stop_loss_distance": stop_loss_distance,
        "take_profit_distance": take_profit_distance,
        "risk_profile": risk_profile,
    }


def _build_phase_one_execution_request_payload(
    normalized_signal: Dict[str, Any],
    strategy_payload: Dict[str, Any],
    risk_payload: Dict[str, Any],
) -> Dict[str, Any]:
    risk_decision = str(risk_payload.get("risk_decision") or "deny")
    is_actionable = risk_decision in {"approve", "resize"}
    quantity = _to_optional_float(risk_payload.get("position_size")) or 0.0
    side_value = str(normalized_signal.get("side") or "").strip().lower()

    return {
        "symbol": str(normalized_signal.get("broker_symbol") or normalized_signal.get("symbol") or ""),
        "side": side_value if side_value in {"long", "short"} else "long",
        "order_type": "market",
        "quantity": quantity if is_actionable else 0.0,
        "mode": "simulated",
        "execution_status": "ready_simulated" if is_actionable else "blocked",
        "is_actionable": is_actionable,
        "decision_trace": {
            "strategy_decision": strategy_payload.get("decision"),
            "strategy_reason": strategy_payload.get("reason_code"),
            "risk_decision": risk_payload.get("risk_decision"),
            "risk_reason": risk_payload.get("reason_code"),
        },
    }


@app.post(
    "/webhooks/tradingview",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest TradingView alert and run Phase 1 dry-run pipeline",
    responses={
        201: {
            "description": "Raw event stored, normalized signal persisted, strategy/risk decisions computed, and simulated execution request written",
        }
    },
)
async def tradingview_webhooks_phase_one(payload: TradingViewAlertWebhookPayload, request: Request):
    verify_signal_key(request)
    payload_dump = payload.model_dump(mode="json", exclude_none=True)

    try:
        raw_event = insert_raw_webhook_event(
            source=str(payload_dump.get("source") or "tradingview"),
            source_ip=request.client.host if request.client else None,
            headers_json=dict(request.headers),
            payload_json=payload_dump,
            schema_version="tv_alert_v1",
        )

        normalized_signal = normalize_tradingview_alert(
            event_id=str(raw_event["event_id"]),
            payload=payload_dump,
            received_at=str(raw_event["received_at"]),
        )
        normalized_record = insert_normalized_signal(
            event_id=normalized_signal["event_id"],
            source=str(normalized_signal.get("source") or "tradingview"),
            symbol=str(normalized_signal["symbol"]),
            broker_symbol=str(normalized_signal["broker_symbol"]),
            timeframe=str(normalized_signal["timeframe"]),
            side=str(normalized_signal["side"]),
            signal_name=str(normalized_signal.get("signal_name") or ""),
            strategy_id=str(normalized_signal.get("strategy_id") or ""),
            score=_to_optional_float(normalized_signal.get("score")),
            bar_time=str(normalized_signal.get("bar_time") or "") or None,
            market_price=_to_optional_float(normalized_signal.get("market_price")),
            features_json=normalized_signal.get("features") if isinstance(normalized_signal.get("features"), dict) else {},
        )

        strategy_payload = _build_phase_one_strategy_decision_payload(normalized_signal)
        strategy_record = insert_strategy_decision(
            normalized_id=str(normalized_record["normalized_id"]),
            decision=str(strategy_payload["decision"]),
            reason_code=str(strategy_payload["reason_code"]),
            confidence=_to_optional_float(strategy_payload.get("confidence")),
            policy_version=str(strategy_payload.get("policy_version") or "vp_policy_v3_dryrun"),
            decision_json=strategy_payload,
        )

        risk_payload = _build_phase_one_risk_payload(normalized_signal, strategy_payload)
        risk_record = insert_risk_event(
            strategy_decision_id=str(strategy_record["strategy_decision_id"]),
            risk_decision=str(risk_payload["risk_decision"]),
            reason_code=str(risk_payload["reason_code"]),
            position_size=_to_optional_float(risk_payload.get("position_size")),
            stop_loss_distance=_to_optional_float(risk_payload.get("stop_loss_distance")),
            take_profit_distance=_to_optional_float(risk_payload.get("take_profit_distance")),
            risk_json=risk_payload,
        )

        execution_payload = _build_phase_one_execution_request_payload(
            normalized_signal,
            strategy_payload,
            risk_payload,
        )
        execution_record = insert_execution_request(
            strategy_decision_id=str(strategy_record["strategy_decision_id"]),
            risk_event_id=str(risk_record["risk_event_id"]),
            normalized_id=str(normalized_record["normalized_id"]),
            event_id=str(raw_event["event_id"]),
            symbol=str(execution_payload["symbol"]),
            side=str(execution_payload["side"]),
            order_type=str(execution_payload["order_type"]),
            quantity=_to_optional_float(execution_payload.get("quantity")),
            mode=str(execution_payload["mode"]),
            execution_status=str(execution_payload["execution_status"]),
            request_json=execution_payload,
        )

        paper_execution = _run_paper_execution_alpha(
            event_id=str(raw_event["event_id"]),
            normalized_id=str(normalized_record["normalized_id"]),
            normalized_signal=normalized_signal,
            strategy_payload=strategy_payload,
            risk_payload=risk_payload,
            execution_record=execution_record,
            execution_payload=execution_payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except Exception:
        logger.exception("Failed to process /webhooks/tradingview Phase 1 pipeline")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process TradingView webhook through Phase 1 pipeline",
        )

    return {
        "status": "processed",
        "mode": "simulated",
        "event_id": raw_event["event_id"],
        "raw_webhook_event_id": raw_event["id"],
        "raw_event_duplicate": bool(raw_event.get("duplicate")),
        "normalized_signal_id": normalized_record["normalized_id"],
        "normalized_duplicate": bool(normalized_record.get("duplicate")),
        "strategy_decision_id": strategy_record["strategy_decision_id"],
        "strategy_duplicate": bool(strategy_record.get("duplicate")),
        "strategy_decision": strategy_payload,
        "risk_event_id": risk_record["risk_event_id"],
        "risk_duplicate": bool(risk_record.get("duplicate")),
        "risk_decision": risk_payload,
        "execution_request_id": execution_record["execution_request_id"],
        "execution_duplicate": bool(execution_record.get("duplicate")),
        "execution_request": execution_payload,
        "order": paper_execution["order"],
        "order_duplicate": bool(paper_execution.get("order_duplicate")),
        "fill": paper_execution.get("fill"),
        "fill_duplicate": paper_execution.get("fill_duplicate"),
        "position_update": paper_execution.get("position_update"),
    }


@app.post(
    "/webhooks/tradingview/batch",
    status_code=status.HTTP_201_CREATED,
    response_model=TradingViewIngestAcceptResponse,
    summary="Ingest TradingView batch payload into active intake",
)
async def tradingview_batch_ingest(
    payload: TradingViewBatchPayload,
    request: Request,
    signal_key: Annotated[Optional[str], Query(min_length=1)] = None,
    x_signal_key: Annotated[Optional[str], Header(alias="X-SIGNAL-KEY")] = None,
):
    try:
        request_body = await request.body()
        return accept_tradingview_batch(
            payload=payload,
            payload_size_bytes=len(request_body),
            query_signal_key=signal_key,
            header_signal_key=x_signal_key,
            source_ip=request.client.host if request.client else None,
            request_headers=dict(request.headers),
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except Exception:
        logger.exception("Failed to ingest TradingView batch payload")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to ingest TradingView batch payload",
        )


@app.get(
    "/webhooks/tradingview/batches/recent",
    summary="Get recent TradingView ingest batches",
)
def tradingview_batches_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    status_filter: Annotated[Optional[str], Query(alias="status", min_length=1)] = None,
):
    capped_limit = min(limit, 500)
    try:
        rows = get_recent_batch_rows(capped_limit, status=status_filter)
    except Exception:
        logger.exception("Failed to fetch recent TradingView ingest batches")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent TradingView ingest batches",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get(
    "/webhooks/tradingview/events/recent",
    summary="Get recent normalized TradingView ingest events",
)
def tradingview_events_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    side: Annotated[Optional[str], Query(min_length=1)] = None,
    signal_type: Annotated[Optional[str], Query(min_length=1)] = None,
    confirmed: Annotated[Optional[bool], Query()] = None,
):
    capped_limit = min(limit, 500)
    try:
        rows = get_recent_event_rows(
            capped_limit,
            symbol=symbol,
            side=side,
            signal_type=signal_type,
            confirmed=confirmed,
        )
    except Exception:
        logger.exception("Failed to fetch recent TradingView ingest events")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch recent TradingView ingest events",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get(
    "/webhooks/tradingview/signal-journal/recent",
    summary="Get recent signal journal snapshots with derived context metrics",
)
def tradingview_signal_journal_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    side: Annotated[Optional[str], Query(min_length=1)] = None,
    signal_name: Annotated[Optional[str], Query(min_length=1)] = None,
    timeframe: Annotated[Optional[str], Query(min_length=1)] = None,
):
    capped_limit = min(limit, 500)
    try:
        rows = get_recent_signal_journal_rows(
            capped_limit,
            symbol=symbol,
            side=side,
            signal_name=signal_name,
            timeframe=timeframe,
        )
    except Exception:
        logger.exception("Failed to fetch recent TradingView signal journal rows")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch TradingView signal journal rows",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get(
    "/webhooks/tradingview/signal-outcomes/recent",
    summary="Get recent signal outcomes for reversion vs continuation behavior",
)
def tradingview_signal_outcomes_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    side: Annotated[Optional[str], Query(min_length=1)] = None,
    signal_name: Annotated[Optional[str], Query(min_length=1)] = None,
    timeframe: Annotated[Optional[str], Query(min_length=1)] = None,
    outcome_status: Annotated[Optional[str], Query(alias="status", min_length=1)] = None,
):
    capped_limit = min(limit, 500)
    try:
        rows = get_recent_signal_outcome_rows(
            capped_limit,
            symbol=symbol,
            side=side,
            signal_name=signal_name,
            timeframe=timeframe,
            status=outcome_status,
        )
    except Exception:
        logger.exception("Failed to fetch recent TradingView signal outcomes")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch TradingView signal outcomes",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.get(
    "/webhooks/tradingview/market-bias/recent",
    summary="Get recent market bias scores generated from signal outcome cohorts",
)
def tradingview_market_bias_recent(
    limit: Annotated[int, Query(ge=1)] = 50,
    symbol: Annotated[Optional[str], Query(min_length=1)] = None,
    side: Annotated[Optional[str], Query(min_length=1)] = None,
    signal_name: Annotated[Optional[str], Query(min_length=1)] = None,
    confidence: Annotated[Optional[str], Query(min_length=1)] = None,
    status_filter: Annotated[Optional[str], Query(alias="status", min_length=1)] = None,
):
    capped_limit = min(limit, 500)
    try:
        rows = get_recent_market_bias_rows(
            capped_limit,
            symbol=symbol,
            side=side,
            signal_name=signal_name,
            confidence=confidence,
            status=status_filter,
        )
    except Exception:
        logger.exception("Failed to fetch recent TradingView market bias rows")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch TradingView market bias rows",
        )

    return {
        "count": len(rows),
        "limit": capped_limit,
        "rows": rows,
    }


@app.post(
    "/webhooks/tradingview/signal-outcomes/run",
    summary="Run one signal outcome and market bias update pass",
)
def tradingview_run_signal_outcomes(
    min_future_bars: Annotated[Optional[int], Query(ge=1)] = None,
):
    defaults = get_outcome_engine_defaults()
    resolved_min_future_bars = int(min_future_bars or defaults["min_future_bars"])

    try:
        journal_backfill = backfill_signal_snapshots_from_storage()
        outcome_summary = run_signal_outcome_evaluation_once(
            min_future_bars=resolved_min_future_bars,
            continuation_threshold_pct=float(defaults["continuation_threshold_pct"]),
            horizon_bars=tuple(defaults["horizon_bars"]),
        )
        bias_summary = compute_and_store_signal_bias()
    except Exception:
        logger.exception("Failed to run TradingView signal outcome/bias pass")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to run TradingView signal outcome/bias pass",
        )

    return {
        "status": "ok",
        "journal_backfill": journal_backfill,
        "outcome": outcome_summary,
        "bias": bias_summary,
    }


@app.get(
    "/webhooks/tradingview/batch/{batch_id}",
    summary="Get TradingView ingest batch by batch_id",
)
def tradingview_batch_by_id(batch_id: Annotated[str, Path(min_length=1, max_length=128)]):
    try:
        row = get_batch_by_id(batch_id)
    except Exception:
        logger.exception("Failed to fetch TradingView ingest batch_id=%s", batch_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch TradingView batch",
        )

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="batch not found")
    return row


@app.get(
    "/webhooks/tradingview/event/{event_id}",
    summary="Get TradingView ingest event by event_id",
)
def tradingview_event_by_id(event_id: Annotated[str, Path(min_length=1, max_length=128)]):
    try:
        row = get_event_by_id(event_id)
    except Exception:
        logger.exception("Failed to fetch TradingView ingest event_id=%s", event_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch TradingView event",
        )

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="event not found")
    return row


@app.post(
    "/webhooks/tradingview/batch/{batch_id}/replay",
    summary="Replay normalized ingestion for one batch",
)
def tradingview_replay_batch(
    batch_id: Annotated[str, Path(min_length=1, max_length=128)],
    overwrite: Annotated[bool, Query()] = False,
):
    try:
        return replay_batch_once(batch_id=batch_id, overwrite=overwrite)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to replay TradingView ingest batch_id=%s", batch_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to replay TradingView batch",
        )


@app.post(
    "/webhooks/tradingview/cycle/run",
    response_model=TradingViewCycleSummary,
    summary="Run one TradingView ingest commit cycle",
)
def tradingview_run_ingest_cycle():
    try:
        return run_ingest_cycle_once(trigger="manual")
    except Exception:
        logger.exception("Failed to run TradingView ingest cycle")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to run TradingView ingest cycle",
        )


# ─────────────────────────────────────────────────────────────────────
# Release Cohort Scoring + Feature Lifecycle Endpoints
# ─────────────────────────────────────────────────────────────────────


@app.post(
    "/cohorts/score",
    summary="Run release cohort scoring across all horizons",
)
def cohorts_run_scoring():
    try:
        result = run_cohort_scoring()
    except Exception:
        logger.exception("Failed to run cohort scoring")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to run cohort scoring",
        )
    return result


@app.get(
    "/cohorts/scores",
    summary="Get release cohort scores with optional filters",
)
def cohorts_get_scores(
    horizon: Annotated[Optional[str], Query(examples=["short", "medium", "long"])] = None,
    release_version: Annotated[Optional[str], Query(examples=["2.1.0"])] = None,
    release_channel: Annotated[Optional[str], Query(examples=["production"])] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
):
    try:
        rows = get_cohort_scores(
            horizon=horizon,
            release_version=release_version,
            release_channel=release_channel,
            limit=limit,
        )
    except Exception:
        logger.exception("Failed to fetch cohort scores")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch cohort scores",
        )
    return {"count": len(rows), "rows": rows}


@app.get(
    "/cohorts/leaderboard",
    summary="Rank cohorts by a scoring metric within a horizon",
)
def cohorts_leaderboard(
    horizon: Annotated[str, Query(examples=["medium"])] = "medium",
    sort_by: Annotated[str, Query(examples=["quality_score"])] = "quality_score",
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    try:
        rows = get_cohort_leaderboard(horizon=horizon, sort_by=sort_by, limit=limit)
    except Exception:
        logger.exception("Failed to fetch cohort leaderboard")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch cohort leaderboard",
        )
    return {"horizon": horizon, "sort_by": sort_by, "count": len(rows), "rows": rows}


@app.get(
    "/cohorts/compare",
    summary="Compare two release versions side-by-side",
)
def cohorts_compare(
    left_version: Annotated[str, Query(min_length=1, examples=["2.0.0"])],
    right_version: Annotated[str, Query(min_length=1, examples=["2.1.0"])],
    horizon: Annotated[str, Query(examples=["medium"])] = "medium",
):
    try:
        result = compare_release_cohorts(
            left_version=left_version,
            right_version=right_version,
            horizon=horizon,
        )
    except Exception:
        logger.exception("Failed to compare release cohorts")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to compare release cohorts",
        )
    return result


@app.get(
    "/cohorts/features",
    summary="Get feature lifecycle status for research fields",
)
def cohorts_feature_lifecycle(
    status_filter: Annotated[Optional[str], Query(examples=["observe", "candidate", "promote", "decay", "retire"])] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
):
    try:
        rows = get_feature_lifecycle_status(status_filter=status_filter, limit=limit)
    except Exception:
        logger.exception("Failed to fetch feature lifecycle")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch feature lifecycle",
        )
    return {"count": len(rows), "rows": rows}


@app.get(
    "/cohorts/history",
    summary="Get temporal history of cohort scoring snapshots",
)
def cohorts_score_history(
    cohort_key: Annotated[Optional[str], Query()] = None,
    horizon: Annotated[Optional[str], Query(examples=["short", "medium", "long"])] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
):
    try:
        rows = get_cohort_score_history(
            cohort_key=cohort_key,
            horizon=horizon,
            limit=limit,
        )
    except Exception:
        logger.exception("Failed to fetch cohort score history")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch cohort score history",
        )
    return {"count": len(rows), "rows": rows}
