import json
import math
import os
import sqlite3
from dataclasses import dataclass, field
from hashlib import sha256
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from uuid import uuid4

from backend.feature_math import to_optional_float as shared_to_optional_float


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "database" / "system.db"
SCHEMA_PATH = PROJECT_ROOT / "database" / "schema.sql"
UNSET = object()
DEFAULT_CLAIM_TIMEOUT_SECONDS = 120
DEFAULT_OUTCOME_WIN_THRESHOLD_PCT = 0.10
DEFAULT_OUTCOME_LOSS_THRESHOLD_PCT = -0.10
VALID_OUTCOME_LABELS = {"winner", "loser", "scratch", "unknown"}
VALID_OUTCOME_COHORT_GROUP_BYS = {
    "strategy",
    "source",
    "setup_family",
    "worker_id",
    "symbol",
    "direction",
}
VALID_OUTCOME_POLICY_SCORING_MODES = {
    "expectancy_pct",
    "expectancy_points",
    "win_rate",
    "avg_pnl_pct",
    "blended",
}
DEFAULT_POLICY_MATRIX_GROUP_BYS = ("strategy", "source", "setup_family", "worker_id")

# SQL fragment constants (S1192)
_REAL_NOT_NULL_DEFAULT_0 = "REAL NOT NULL DEFAULT 0.0"
_WHERE = " WHERE "
_AND = " AND "
_LIMIT = " LIMIT ?"
_WC_EVENT_ID = "event_id = ?"
_WC_SYMBOL = "symbol = ?"
_WC_TIMEFRAME = "timeframe = ?"
_WC_SIDE = "side = ?"
_WC_EXECUTION_STATUS = "execution_status = ?"
_SQL_SELECT_POSITION_BY_ID = "SELECT * FROM positions WHERE id = ? LIMIT 1"
_SQL_SELECT_TRADE_CANDIDATE_BY_ID = "SELECT * FROM trade_candidates WHERE id = ? LIMIT 1"


def _where_clause(where_clauses: list[str]) -> str:
    """Build `` WHERE c1 AND c2 …`` from a list of SQL predicates."""
    return _WHERE + _AND.join(where_clauses)


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _require_lastrowid(cursor: sqlite3.Cursor) -> int:
    """Return the integer row ID from a cursor after an INSERT.

    ``sqlite3.Cursor.lastrowid`` is typed as ``int | None`` by the standard
    library stubs, but is only ``None`` when no row has been inserted yet.
    This helper asserts that an INSERT actually occurred and returns the
    resolved ``int``, satisfying Pylance's type checker without suppressing the
    warning or silently swallowing a ``None`` with a magic fallback value.
    """
    row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("INSERT statement did not return a lastrowid")
    return int(row_id)


def init_db() -> None:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_PATH}")

    with get_connection() as conn:
        with SCHEMA_PATH.open("r", encoding="utf-8") as f:
            conn.executescript(f.read())
        _ensure_trade_candidates_columns(conn)
        _ensure_execution_journal_table(conn)
        _ensure_execution_outcomes_table(conn)
        _ensure_feature_tables(conn)
        _ensure_volume_profile_snapshot_columns(conn)
        _ensure_phase_one_pipeline_tables(conn)
        _ensure_paper_execution_tables(conn)
        _ensure_cohort_tables(conn)
        conn.commit()


def _ensure_trade_candidates_columns(conn: sqlite3.Connection) -> None:
    required_columns = {
        "execution_status": "TEXT DEFAULT 'pending'",
        "execution_note": "TEXT",
        "executed_at": "TEXT",
        "claimed_by": "TEXT",
        "claim_token": "TEXT",
        "claimed_at": "TEXT",
    }

    existing_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(trade_candidates)").fetchall()
    }

    for column_name, definition in required_columns.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE trade_candidates ADD COLUMN {column_name} {definition}")

    conn.execute(
        """
        UPDATE trade_candidates
        SET execution_status = 'pending'
        WHERE execution_status IS NULL OR TRIM(execution_status) = ''
        """
    )


def _ensure_execution_journal_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS execution_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL,
            signal_id TEXT,
            worker_id TEXT NOT NULL,
            action TEXT NOT NULL,
            execution_status TEXT NOT NULL,
            execution_note TEXT,
            confidence REAL,
            symbol TEXT,
            direction TEXT,
            entry_price REAL,
            created_at TEXT NOT NULL,
            metadata_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_execution_journal_created_at
        ON execution_journal(created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_journal_candidate_id
        ON execution_journal(candidate_id);

        CREATE INDEX IF NOT EXISTS idx_execution_journal_worker_created
        ON execution_journal(worker_id, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_journal_status_created
        ON execution_journal(execution_status, created_at DESC);
        """
    )


def _ensure_execution_outcomes_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS execution_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id INTEGER NOT NULL UNIQUE,
            candidate_id INTEGER NOT NULL,
            signal_id TEXT,
            worker_id TEXT,
            symbol TEXT,
            direction TEXT,
            entry_price REAL,
            reference_timestamp TEXT,
            evaluation_window_minutes INTEGER NOT NULL,
            outcome_status TEXT NOT NULL,
            exit_price REAL,
            pnl_points REAL,
            pnl_pct REAL,
            max_favorable_excursion REAL,
            max_adverse_excursion REAL,
            evaluated_at TEXT NOT NULL,
            metadata_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_execution_outcomes_evaluated_at
        ON execution_outcomes(evaluated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_outcomes_symbol_evaluated
        ON execution_outcomes(symbol, evaluated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_outcomes_worker_evaluated
        ON execution_outcomes(worker_id, evaluated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_outcomes_status_evaluated
        ON execution_outcomes(outcome_status, evaluated_at DESC);
        """
    )


def _ensure_feature_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS feature_registry (
            engine_name TEXT NOT NULL,
            feature_key TEXT NOT NULL,
            value_type TEXT NOT NULL,
            description TEXT,
            unit TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (engine_name, feature_key)
        );

        CREATE TABLE IF NOT EXISTS feature_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            source_bar_id INTEGER,
            feature_version TEXT NOT NULL,
            regime_id TEXT,
            regime_confidence REAL,
            transition_risk REAL,
            long_probability REAL,
            short_probability REAL,
            no_trade_probability REAL,
            expected_excursion REAL,
            setup_trust_score REAL,
            feature_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS feature_snapshot_values (
            snapshot_id INTEGER NOT NULL,
            feature_key TEXT NOT NULL,
            feature_value REAL,
            feature_text TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (snapshot_id, feature_key),
            FOREIGN KEY (snapshot_id) REFERENCES feature_snapshots(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_feature_snapshots_symbol_timeframe_timestamp
        ON feature_snapshots(symbol, timeframe, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_feature_snapshot_values_feature_key
        ON feature_snapshot_values(feature_key);
        """
    )


def _ensure_volume_profile_snapshot_columns(conn: sqlite3.Connection) -> None:
    required_columns = {
        "profile_range": _REAL_NOT_NULL_DEFAULT_0,
        "value_area_width": _REAL_NOT_NULL_DEFAULT_0,
        "value_area_width_pct": _REAL_NOT_NULL_DEFAULT_0,
        "poc_relative": "REAL NOT NULL DEFAULT 0.5",
        "poc_distance_from_mid": _REAL_NOT_NULL_DEFAULT_0,
        "close_position_in_profile": "REAL",
        "distance_to_poc": "REAL",
        "distance_to_vah": "REAL",
        "distance_to_val": "REAL",
        "distance_to_poc_pct": "REAL",
        "distance_to_vah_pct": "REAL",
        "distance_to_val_pct": "REAL",
        "inside_value_area": "INTEGER",
        "above_vah": "INTEGER",
        "below_val": "INTEGER",
    }

    existing_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(volume_profile_snapshots)").fetchall()
    }

    for column_name, definition in required_columns.items():
        if column_name not in existing_columns:
            conn.execute(
                f"ALTER TABLE volume_profile_snapshots ADD COLUMN {column_name} {definition}"
            )


def _ensure_phase_one_pipeline_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL,
            received_at TEXT NOT NULL,
            source_ip TEXT,
            headers_json TEXT,
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL UNIQUE,
            schema_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS normalized_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            normalized_id TEXT NOT NULL UNIQUE,
            event_id TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL,
            symbol TEXT NOT NULL,
            broker_symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            side TEXT NOT NULL,
            signal_name TEXT,
            strategy_id TEXT,
            score REAL,
            bar_time TEXT,
            market_price REAL,
            features_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS strategy_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_decision_id TEXT NOT NULL UNIQUE,
            normalized_id TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            confidence REAL,
            policy_version TEXT,
            decision_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS risk_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            risk_event_id TEXT NOT NULL UNIQUE,
            strategy_decision_id TEXT NOT NULL,
            risk_decision TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            position_size REAL,
            stop_loss_distance REAL,
            take_profit_distance REAL,
            risk_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS execution_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execution_request_id TEXT NOT NULL UNIQUE,
            strategy_decision_id TEXT NOT NULL,
            risk_event_id TEXT NOT NULL,
            normalized_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity REAL,
            mode TEXT NOT NULL,
            execution_status TEXT NOT NULL,
            request_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_raw_webhook_events_received_at
        ON raw_webhook_events(received_at DESC);

        CREATE INDEX IF NOT EXISTS idx_raw_webhook_events_source_received
        ON raw_webhook_events(source, received_at DESC);

        CREATE INDEX IF NOT EXISTS idx_normalized_signals_created_at
        ON normalized_signals(created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_normalized_signals_symbol_timeframe_created
        ON normalized_signals(symbol, timeframe, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_strategy_decisions_created_at
        ON strategy_decisions(created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_strategy_decisions_decision_created
        ON strategy_decisions(decision, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_risk_events_created_at
        ON risk_events(created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_risk_events_risk_decision_created
        ON risk_events(risk_decision, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_requests_created_at
        ON execution_requests(created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_requests_mode_created
        ON execution_requests(mode, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_requests_status_created
        ON execution_requests(execution_status, created_at DESC);
        """
    )


def _ensure_paper_execution_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS broker_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL UNIQUE,
            broker_order_id TEXT NOT NULL UNIQUE,
            execution_request_id TEXT NOT NULL UNIQUE,
            event_id TEXT NOT NULL,
            signal_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            qty REAL,
            limit_price REAL,
            stop_price REAL,
            status TEXT NOT NULL,
            submitted_at TEXT,
            accepted_at TEXT,
            filled_at TEXT,
            rejected_at TEXT,
            updated_at TEXT NOT NULL,
            mode TEXT NOT NULL,
            lifecycle_json TEXT,
            error_text TEXT
        );

        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fill_id TEXT NOT NULL UNIQUE,
            order_id TEXT NOT NULL,
            broker_order_id TEXT NOT NULL,
            execution_request_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            fill_price REAL,
            fill_qty REAL,
            fee REAL,
            fill_status TEXT NOT NULL,
            fill_time TEXT NOT NULL,
            metadata_json TEXT
        );

        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            qty REAL NOT NULL,
            avg_entry REAL,
            realized_pnl REAL NOT NULL DEFAULT 0.0,
            unrealized_pnl REAL NOT NULL DEFAULT 0.0,
            status TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            closed_at TEXT,
            last_order_id TEXT,
            last_fill_id TEXT,
            metadata_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_broker_orders_updated_at
        ON broker_orders(updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_broker_orders_status_updated
        ON broker_orders(status, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_broker_orders_symbol_updated
        ON broker_orders(symbol, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_fills_fill_time
        ON fills(fill_time DESC);

        CREATE INDEX IF NOT EXISTS idx_fills_symbol_fill_time
        ON fills(symbol, fill_time DESC);

        CREATE INDEX IF NOT EXISTS idx_fills_order_id
        ON fills(order_id);

        CREATE INDEX IF NOT EXISTS idx_positions_status_updated
        ON positions(status, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_positions_symbol_status
        ON positions(symbol, status, updated_at DESC);
        """
    )


def _ensure_cohort_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS release_cohort_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cohort_key TEXT NOT NULL,
            release_id TEXT,
            release_version TEXT,
            release_channel TEXT,
            strategy_id TEXT,
            symbol TEXT,
            side TEXT,
            horizon TEXT NOT NULL,
            window_bars INTEGER NOT NULL,
            sample_count INTEGER NOT NULL DEFAULT 0,
            win_count INTEGER NOT NULL DEFAULT 0,
            loss_count INTEGER NOT NULL DEFAULT 0,
            scratch_count INTEGER NOT NULL DEFAULT 0,
            win_rate REAL,
            avg_pnl_pct REAL,
            avg_mfe_pct REAL,
            avg_mae_pct REAL,
            avg_continuation_strength REAL,
            avg_reversion_strength REAL,
            continuation_hit_rate REAL,
            reversion_hit_rate REAL,
            promotion_score REAL NOT NULL DEFAULT 0.0,
            decay_score REAL NOT NULL DEFAULT 0.0,
            confidence_score REAL NOT NULL DEFAULT 0.0,
            quality_score REAL NOT NULL DEFAULT 0.0,
            scored_at TEXT NOT NULL,
            metadata_json TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_release_cohort_key_horizon
        ON release_cohort_scores(cohort_key, horizon);

        CREATE INDEX IF NOT EXISTS idx_release_cohort_scores_version
        ON release_cohort_scores(release_version, horizon);

        CREATE INDEX IF NOT EXISTS idx_release_cohort_scores_channel
        ON release_cohort_scores(release_channel, horizon);

        CREATE INDEX IF NOT EXISTS idx_release_cohort_scores_scored_at
        ON release_cohort_scores(scored_at DESC);

        CREATE TABLE IF NOT EXISTS feature_lifecycle (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feature_key TEXT NOT NULL UNIQUE,
            layer TEXT NOT NULL DEFAULT 'research',
            current_status TEXT NOT NULL DEFAULT 'observe',
            previous_status TEXT,
            sample_count INTEGER NOT NULL DEFAULT 0,
            promotion_score REAL NOT NULL DEFAULT 0.0,
            decay_score REAL NOT NULL DEFAULT 0.0,
            confidence_score REAL NOT NULL DEFAULT 0.0,
            correlation_with_win REAL,
            correlation_with_continuation REAL,
            avg_value_winners REAL,
            avg_value_losers REAL,
            last_evaluated_at TEXT,
            status_changed_at TEXT,
            created_at TEXT NOT NULL,
            metadata_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_feature_lifecycle_status
        ON feature_lifecycle(current_status);

        CREATE INDEX IF NOT EXISTS idx_feature_lifecycle_layer
        ON feature_lifecycle(layer, current_status);
        """
    )


def insert_bar_state(
    *,
    timestamp: str,
    symbol: str,
    timeframe: str,
    long_score: Optional[float] = None,
    short_score: Optional[float] = None,
    no_trade_score: Optional[float] = None,
    setup_family: Optional[str] = None,
    pressure_index: Optional[float] = None,
    volatility_state: Optional[str] = None,
    participation_score: Optional[float] = None,
    confidence_seed: Optional[float] = None,
    payload_json: Optional[Dict[str, Any]] = None,
) -> int:
    serialized_payload = json.dumps(payload_json, ensure_ascii=False) if payload_json is not None else None

    query = """
        INSERT INTO bar_states (
            timestamp,
            symbol,
            timeframe,
            long_score,
            short_score,
            no_trade_score,
            setup_family,
            pressure_index,
            volatility_state,
            participation_score,
            confidence_seed,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        timestamp,
        symbol,
        timeframe,
        long_score,
        short_score,
        no_trade_score,
        setup_family,
        pressure_index,
        volatility_state,
        participation_score,
        confidence_seed,
        serialized_payload,
    )

    with get_connection() as conn:
        cursor = conn.execute(query, values)
        conn.commit()
        return _require_lastrowid(cursor)


def insert_raw_webhook_event(
    *,
    source: str,
    source_ip: Optional[str],
    headers_json: Optional[Dict[str, Any]],
    payload_json: Dict[str, Any],
    schema_version: str = "tv_alert_v1",
) -> Dict[str, Any]:
    normalized_source = (source or "tradingview").strip() or "tradingview"
    received_at = _current_utc_iso()
    serialized_headers = json.dumps(headers_json or {}, ensure_ascii=False, sort_keys=True)
    serialized_payload = json.dumps(payload_json or {}, ensure_ascii=False, sort_keys=True)
    payload_hash = sha256(serialized_payload.encode("utf-8")).hexdigest()

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id, event_id, received_at, payload_hash
            FROM raw_webhook_events
            WHERE payload_hash = ?
            LIMIT 1
            """,
            (payload_hash,),
        ).fetchone()

        if existing is not None:
            return {
                "id": int(existing["id"]),
                "event_id": str(existing["event_id"]),
                "received_at": str(existing["received_at"]),
                "payload_hash": str(existing["payload_hash"]),
                "duplicate": True,
            }

        event_id = f"tv_{uuid4().hex}"
        cursor = conn.execute(
            """
            INSERT INTO raw_webhook_events (
                event_id,
                source,
                received_at,
                source_ip,
                headers_json,
                payload_json,
                payload_hash,
                schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                normalized_source,
                received_at,
                source_ip,
                serialized_headers,
                serialized_payload,
                payload_hash,
                schema_version,
            ),
        )
        conn.commit()

        return {
            "id": _require_lastrowid(cursor),
            "event_id": event_id,
            "received_at": received_at,
            "payload_hash": payload_hash,
            "duplicate": False,
        }


def insert_normalized_signal(
    *,
    event_id: str,
    source: str,
    symbol: str,
    broker_symbol: str,
    timeframe: str,
    side: str,
    signal_name: Optional[str],
    strategy_id: Optional[str],
    score: Optional[float],
    bar_time: Optional[str],
    market_price: Optional[float],
    features_json: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    created_at = _current_utc_iso()
    serialized_features = json.dumps(features_json or {}, ensure_ascii=False, sort_keys=True)

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id, normalized_id, created_at
            FROM normalized_signals
            WHERE event_id = ?
            LIMIT 1
            """,
            (event_id,),
        ).fetchone()

        if existing is not None:
            return {
                "id": int(existing["id"]),
                "normalized_id": str(existing["normalized_id"]),
                "created_at": str(existing["created_at"]),
                "duplicate": True,
            }

        normalized_id = f"sig_{uuid4().hex}"
        cursor = conn.execute(
            """
            INSERT INTO normalized_signals (
                normalized_id,
                event_id,
                source,
                symbol,
                broker_symbol,
                timeframe,
                side,
                signal_name,
                strategy_id,
                score,
                bar_time,
                market_price,
                features_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_id,
                event_id,
                source,
                symbol,
                broker_symbol,
                timeframe,
                side,
                signal_name,
                strategy_id,
                score,
                bar_time,
                market_price,
                serialized_features,
                created_at,
            ),
        )
        conn.commit()

        return {
            "id": _require_lastrowid(cursor),
            "normalized_id": normalized_id,
            "created_at": created_at,
            "duplicate": False,
        }


def insert_strategy_decision(
    *,
    normalized_id: str,
    decision: str,
    reason_code: str,
    confidence: Optional[float],
    policy_version: Optional[str],
    decision_json: Optional[Dict[str, Any]],
    created_at: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_created_at = created_at or _current_utc_iso()
    serialized_decision = json.dumps(decision_json or {}, ensure_ascii=False, sort_keys=True)

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id, strategy_decision_id, created_at
            FROM strategy_decisions
            WHERE normalized_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (normalized_id,),
        ).fetchone()

        if existing is not None:
            return {
                "id": int(existing["id"]),
                "strategy_decision_id": str(existing["strategy_decision_id"]),
                "created_at": str(existing["created_at"]),
                "duplicate": True,
            }

        strategy_decision_id = f"strat_{uuid4().hex}"
        cursor = conn.execute(
            """
            INSERT INTO strategy_decisions (
                strategy_decision_id,
                normalized_id,
                decision,
                reason_code,
                confidence,
                policy_version,
                decision_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strategy_decision_id,
                normalized_id,
                decision,
                reason_code,
                confidence,
                policy_version,
                serialized_decision,
                resolved_created_at,
            ),
        )
        conn.commit()

        return {
            "id": _require_lastrowid(cursor),
            "strategy_decision_id": strategy_decision_id,
            "created_at": resolved_created_at,
            "duplicate": False,
        }


def insert_risk_event(
    *,
    strategy_decision_id: str,
    risk_decision: str,
    reason_code: str,
    position_size: Optional[float],
    stop_loss_distance: Optional[float],
    take_profit_distance: Optional[float],
    risk_json: Optional[Dict[str, Any]],
    created_at: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_created_at = created_at or _current_utc_iso()
    serialized_risk = json.dumps(risk_json or {}, ensure_ascii=False, sort_keys=True)

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id, risk_event_id, created_at
            FROM risk_events
            WHERE strategy_decision_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (strategy_decision_id,),
        ).fetchone()

        if existing is not None:
            return {
                "id": int(existing["id"]),
                "risk_event_id": str(existing["risk_event_id"]),
                "created_at": str(existing["created_at"]),
                "duplicate": True,
            }

        risk_event_id = f"risk_{uuid4().hex}"
        cursor = conn.execute(
            """
            INSERT INTO risk_events (
                risk_event_id,
                strategy_decision_id,
                risk_decision,
                reason_code,
                position_size,
                stop_loss_distance,
                take_profit_distance,
                risk_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                risk_event_id,
                strategy_decision_id,
                risk_decision,
                reason_code,
                position_size,
                stop_loss_distance,
                take_profit_distance,
                serialized_risk,
                resolved_created_at,
            ),
        )
        conn.commit()

        return {
            "id": _require_lastrowid(cursor),
            "risk_event_id": risk_event_id,
            "created_at": resolved_created_at,
            "duplicate": False,
        }


def insert_execution_request(
    *,
    strategy_decision_id: str,
    risk_event_id: str,
    normalized_id: str,
    event_id: str,
    symbol: str,
    side: str,
    order_type: str,
    quantity: Optional[float],
    mode: str,
    execution_status: str,
    request_json: Optional[Dict[str, Any]],
    created_at: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_created_at = created_at or _current_utc_iso()
    serialized_request = json.dumps(request_json or {}, ensure_ascii=False, sort_keys=True)

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id, execution_request_id, created_at
            FROM execution_requests
            WHERE normalized_id = ? AND mode = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (normalized_id, mode),
        ).fetchone()

        if existing is not None:
            return {
                "id": int(existing["id"]),
                "execution_request_id": str(existing["execution_request_id"]),
                "created_at": str(existing["created_at"]),
                "duplicate": True,
            }

        execution_request_id = f"exec_req_{uuid4().hex}"
        cursor = conn.execute(
            """
            INSERT INTO execution_requests (
                execution_request_id,
                strategy_decision_id,
                risk_event_id,
                normalized_id,
                event_id,
                symbol,
                side,
                order_type,
                quantity,
                mode,
                execution_status,
                request_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                execution_request_id,
                strategy_decision_id,
                risk_event_id,
                normalized_id,
                event_id,
                symbol,
                side,
                order_type,
                quantity,
                mode,
                execution_status,
                serialized_request,
                resolved_created_at,
            ),
        )
        conn.commit()

        return {
            "id": _require_lastrowid(cursor),
            "execution_request_id": execution_request_id,
            "created_at": resolved_created_at,
            "duplicate": False,
        }


def get_recent_raw_webhook_events(
    limit: int,
    *,
    event_id: Optional[str] = None,
    source: Optional[str] = None,
    payload_hash: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    if event_id is not None:
        where_clauses.append(_WC_EVENT_ID)
        params.append(event_id)

    if source is not None:
        where_clauses.append("source = ?")
        params.append(source)

    if payload_hash is not None:
        where_clauses.append("payload_hash = ?")
        params.append(payload_hash)

    query = """
        SELECT *
        FROM raw_webhook_events
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY received_at DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_recent_normalized_signals(
    limit: int,
    *,
    event_id: Optional[str] = None,
    normalized_id: Optional[str] = None,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    side: Optional[str] = None,
    signal_name: Optional[str] = None,
    strategy_id: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    if event_id is not None:
        where_clauses.append(_WC_EVENT_ID)
        params.append(event_id)

    if normalized_id is not None:
        where_clauses.append("normalized_id = ?")
        params.append(normalized_id)

    if symbol is not None:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)

    if timeframe is not None:
        where_clauses.append(_WC_TIMEFRAME)
        params.append(timeframe)

    if side is not None:
        where_clauses.append(_WC_SIDE)
        params.append(side)

    if signal_name is not None:
        where_clauses.append("signal_name = ?")
        params.append(signal_name)

    if strategy_id is not None:
        where_clauses.append("strategy_id = ?")
        params.append(strategy_id)

    query = """
        SELECT *
        FROM normalized_signals
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_recent_strategy_risk_decisions(
    limit: int,
    *,
    event_id: Optional[str] = None,
    normalized_id: Optional[str] = None,
    strategy_decision_id: Optional[str] = None,
    strategy_decision: Optional[str] = None,
    risk_decision: Optional[str] = None,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    if event_id is not None:
        where_clauses.append("ns.event_id = ?")
        params.append(event_id)

    if normalized_id is not None:
        where_clauses.append("sd.normalized_id = ?")
        params.append(normalized_id)

    if strategy_decision_id is not None:
        where_clauses.append("sd.strategy_decision_id = ?")
        params.append(strategy_decision_id)

    if strategy_decision is not None:
        where_clauses.append("sd.decision = ?")
        params.append(strategy_decision)

    if risk_decision is not None:
        where_clauses.append("re.risk_decision = ?")
        params.append(risk_decision)

    if symbol is not None:
        where_clauses.append("ns.symbol = ?")
        params.append(symbol)

    if side is not None:
        where_clauses.append("ns.side = ?")
        params.append(side)

    query = """
        SELECT
            sd.strategy_decision_id,
            sd.normalized_id,
            ns.event_id,
            ns.symbol,
            ns.timeframe,
            ns.side,
            ns.signal_name,
            ns.strategy_id,
            sd.decision AS strategy_decision,
            sd.reason_code AS strategy_reason_code,
            sd.confidence AS strategy_confidence,
            sd.policy_version,
            sd.decision_json,
            sd.created_at AS strategy_created_at,
            re.risk_event_id,
            re.risk_decision,
            re.reason_code AS risk_reason_code,
            re.position_size,
            re.stop_loss_distance,
            re.take_profit_distance,
            re.risk_json,
            re.created_at AS risk_created_at
        FROM strategy_decisions sd
        JOIN normalized_signals ns ON ns.normalized_id = sd.normalized_id
        LEFT JOIN risk_events re ON re.strategy_decision_id = sd.strategy_decision_id
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY sd.created_at DESC, sd.id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_recent_execution_requests(
    limit: int,
    *,
    event_id: Optional[str] = None,
    normalized_id: Optional[str] = None,
    strategy_decision_id: Optional[str] = None,
    risk_event_id: Optional[str] = None,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    mode: Optional[str] = None,
    execution_status: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    if event_id is not None:
        where_clauses.append("er.event_id = ?")
        params.append(event_id)

    if normalized_id is not None:
        where_clauses.append("er.normalized_id = ?")
        params.append(normalized_id)

    if strategy_decision_id is not None:
        where_clauses.append("er.strategy_decision_id = ?")
        params.append(strategy_decision_id)

    if risk_event_id is not None:
        where_clauses.append("er.risk_event_id = ?")
        params.append(risk_event_id)

    if symbol is not None:
        where_clauses.append("er.symbol = ?")
        params.append(symbol)

    if side is not None:
        where_clauses.append("er.side = ?")
        params.append(side)

    if mode is not None:
        where_clauses.append("er.mode = ?")
        params.append(mode)

    if execution_status is not None:
        where_clauses.append("er.execution_status = ?")
        params.append(execution_status)

    query = """
        SELECT
            er.*,
            ns.timeframe,
            ns.signal_name,
            ns.strategy_id AS normalized_strategy_id
        FROM execution_requests er
        LEFT JOIN normalized_signals ns ON ns.normalized_id = er.normalized_id
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY er.created_at DESC, er.id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_broker_order_by_execution_request_id(execution_request_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM broker_orders
            WHERE execution_request_id = ?
            LIMIT 1
            """,
            (execution_request_id,),
        ).fetchone()

    return dict(row) if row is not None else None


@dataclass
class BrokerOrderParams:
    """Groups all fields required to insert a broker order row.

    Introduced to keep :func:`insert_broker_order` within the recommended
    parameter-count limit (SonarQube python:S107).
    """

    execution_request_id: str
    event_id: str
    symbol: str
    side: str
    order_type: str
    status: str
    mode: str
    signal_id: Optional[str] = None
    qty: Optional[float] = None
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    submitted_at: Optional[str] = None
    accepted_at: Optional[str] = None
    filled_at: Optional[str] = None
    rejected_at: Optional[str] = None
    lifecycle_json: Optional[Dict[str, Any]] = field(default=None)
    error_text: Optional[str] = None
    updated_at: Optional[str] = None


def insert_broker_order(params: BrokerOrderParams) -> Dict[str, Any]:
    resolved_updated_at = params.updated_at or _current_utc_iso()
    serialized_lifecycle = json.dumps(params.lifecycle_json or {}, ensure_ascii=False, sort_keys=True)

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id, order_id, broker_order_id, updated_at
            FROM broker_orders
            WHERE execution_request_id = ?
            LIMIT 1
            """,
            (params.execution_request_id,),
        ).fetchone()

        if existing is not None:
            return {
                "id": int(existing["id"]),
                "order_id": str(existing["order_id"]),
                "broker_order_id": str(existing["broker_order_id"]),
                "updated_at": str(existing["updated_at"]),
                "duplicate": True,
            }

        order_id = f"ord_{uuid4().hex}"
        broker_order_id = f"paper_{uuid4().hex[:16]}"
        cursor = conn.execute(
            """
            INSERT INTO broker_orders (
                order_id,
                broker_order_id,
                execution_request_id,
                event_id,
                signal_id,
                symbol,
                side,
                order_type,
                qty,
                limit_price,
                stop_price,
                status,
                submitted_at,
                accepted_at,
                filled_at,
                rejected_at,
                updated_at,
                mode,
                lifecycle_json,
                error_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                broker_order_id,
                params.execution_request_id,
                params.event_id,
                params.signal_id,
                params.symbol,
                params.side,
                params.order_type,
                params.qty,
                params.limit_price,
                params.stop_price,
                params.status,
                params.submitted_at,
                params.accepted_at,
                params.filled_at,
                params.rejected_at,
                resolved_updated_at,
                params.mode,
                serialized_lifecycle,
                params.error_text,
            ),
        )
        conn.commit()

        return {
            "id": _require_lastrowid(cursor),
            "order_id": order_id,
            "broker_order_id": broker_order_id,
            "updated_at": resolved_updated_at,
            "duplicate": False,
        }


def get_fill_by_order_id(order_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM fills
            WHERE order_id = ?
            ORDER BY fill_time DESC, id DESC
            LIMIT 1
            """,
            (order_id,),
        ).fetchone()

    return dict(row) if row is not None else None


def insert_fill_event(
    *,
    order_id: str,
    broker_order_id: str,
    execution_request_id: str,
    event_id: str,
    symbol: str,
    side: str,
    fill_price: Optional[float],
    fill_qty: Optional[float],
    fee: Optional[float],
    fill_status: str,
    fill_time: Optional[str],
    metadata_json: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    resolved_fill_time = fill_time or _current_utc_iso()
    serialized_metadata = json.dumps(metadata_json or {}, ensure_ascii=False, sort_keys=True)

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id, fill_id, fill_time
            FROM fills
            WHERE order_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (order_id,),
        ).fetchone()

        if existing is not None:
            return {
                "id": int(existing["id"]),
                "fill_id": str(existing["fill_id"]),
                "fill_time": str(existing["fill_time"]),
                "duplicate": True,
            }

        fill_id = f"fill_{uuid4().hex}"
        cursor = conn.execute(
            """
            INSERT INTO fills (
                fill_id,
                order_id,
                broker_order_id,
                execution_request_id,
                event_id,
                symbol,
                side,
                fill_price,
                fill_qty,
                fee,
                fill_status,
                fill_time,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill_id,
                order_id,
                broker_order_id,
                execution_request_id,
                event_id,
                symbol,
                side,
                fill_price,
                fill_qty,
                fee,
                fill_status,
                resolved_fill_time,
                serialized_metadata,
            ),
        )
        conn.commit()

        return {
            "id": _require_lastrowid(cursor),
            "fill_id": fill_id,
            "fill_time": resolved_fill_time,
            "duplicate": False,
        }


# ---------------------------------------------------------------------------
# Private helpers for apply_fill_to_positions
# ---------------------------------------------------------------------------

def _fetch_position_row(conn: sqlite3.Connection, position_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a single position row by primary key and return it as a dict."""
    row = conn.execute(_SQL_SELECT_POSITION_BY_ID, (position_id,)).fetchone()
    return dict(row) if row is not None else None


_SQL_INSERT_OPEN_POSITION = """
    INSERT INTO positions (
        symbol,
        side,
        qty,
        avg_entry,
        realized_pnl,
        unrealized_pnl,
        status,
        opened_at,
        updated_at,
        closed_at,
        last_order_id,
        last_fill_id,
        metadata_json
    ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?, NULL, ?, ?, ?)
"""


def _insert_open_position(
    conn: sqlite3.Connection,
    symbol: str,
    side: str,
    qty: float,
    avg_entry: Optional[float],
    resolved_time: str,
    order_id: str,
    fill_id: str,
    serialized_metadata: str,
) -> int:
    """Insert a new open position row and return its row ID."""
    cursor = conn.execute(
        _SQL_INSERT_OPEN_POSITION,
        (symbol, side, qty, avg_entry, 0.0, 0.0, resolved_time, resolved_time, order_id, fill_id, serialized_metadata),
    )
    return _require_lastrowid(cursor)


def _compute_realized_delta(
    existing_side: str,
    existing_avg: Optional[float],
    price_value: Optional[float],
    close_qty: float,
) -> float:
    """Compute the realized PnL delta for a partial or full position close."""
    if existing_avg is None or price_value is None:
        return 0.0
    if existing_side == "long":
        return (price_value - existing_avg) * close_qty
    return (existing_avg - price_value) * close_qty


def _apply_same_side_fill(
    conn: sqlite3.Connection,
    existing: Dict[str, Any],
    qty_value: float,
    price_value: Optional[float],
    resolved_time: str,
    order_id: str,
    fill_id: str,
    serialized_metadata: str,
) -> Dict[str, Any]:
    """Scale into an existing position on the same side and return the result."""
    existing_qty = shared_to_optional_float(existing.get("qty")) or 0.0
    existing_avg = shared_to_optional_float(existing.get("avg_entry"))
    new_qty = existing_qty + qty_value

    if existing_avg is None or price_value is None or existing_qty <= 0.0:
        new_avg = price_value
    else:
        new_avg = ((existing_avg * existing_qty) + (price_value * qty_value)) / new_qty

    conn.execute(
        """
        UPDATE positions
        SET qty = ?,
            avg_entry = ?,
            updated_at = ?,
            last_order_id = ?,
            last_fill_id = ?,
            metadata_json = ?
        WHERE id = ?
        """,
        (new_qty, new_avg, resolved_time, order_id, fill_id, serialized_metadata, int(existing["id"])),
    )
    conn.commit()
    return {"status": "scaled", "position": _fetch_position_row(conn, int(existing["id"]))}


def _apply_opposite_side_fill(
    conn: sqlite3.Connection,
    existing: Dict[str, Any],
    normalized_side: str,
    symbol: str,
    qty_value: float,
    price_value: Optional[float],
    resolved_time: str,
    order_id: str,
    fill_id: str,
    serialized_metadata: str,
) -> Dict[str, Any]:
    """Reduce, close, or flip an existing position against an opposite-side fill."""
    existing_side = str(existing.get("side") or "").strip().lower()
    existing_qty = shared_to_optional_float(existing.get("qty")) or 0.0
    existing_realized = shared_to_optional_float(existing.get("realized_pnl")) or 0.0

    close_qty = min(existing_qty, qty_value)
    realized_delta = _compute_realized_delta(existing_side, shared_to_optional_float(existing.get("avg_entry")), price_value, close_qty)
    remaining_existing = max(0.0, existing_qty - close_qty)
    remaining_new = max(0.0, qty_value - close_qty)
    updated_realized = existing_realized + realized_delta

    _update_existing_position(conn, existing, remaining_existing, updated_realized, resolved_time, order_id, fill_id, serialized_metadata)

    opened_position: Optional[Dict[str, Any]] = None
    if remaining_new > 0.0:
        new_id = _insert_open_position(conn, symbol, normalized_side, remaining_new, price_value, resolved_time, order_id, fill_id, serialized_metadata)
        opened_position = _fetch_position_row(conn, new_id)

    conn.commit()

    if opened_position is not None:
        return {"status": "flipped", "position": opened_position}
    if remaining_existing > 0.0:
        return {"status": "reduced", "position": _fetch_position_row(conn, int(existing["id"]))}
    return {"status": "closed", "position": None}


def _update_existing_position(
    conn: sqlite3.Connection,
    existing: Dict[str, Any],
    remaining_existing: float,
    updated_realized: float,
    resolved_time: str,
    order_id: str,
    fill_id: str,
    serialized_metadata: str,
) -> None:
    """Apply a reduce or close UPDATE to an existing position row."""
    position_id = int(existing["id"])
    if remaining_existing > 0.0:
        conn.execute(
            """
            UPDATE positions
            SET qty = ?,
                realized_pnl = ?,
                updated_at = ?,
                last_order_id = ?,
                last_fill_id = ?,
                metadata_json = ?
            WHERE id = ?
            """,
            (remaining_existing, updated_realized, resolved_time, order_id, fill_id, serialized_metadata, position_id),
        )
    else:
        conn.execute(
            """
            UPDATE positions
            SET qty = 0,
                realized_pnl = ?,
                status = 'closed',
                updated_at = ?,
                closed_at = ?,
                last_order_id = ?,
                last_fill_id = ?,
                metadata_json = ?
            WHERE id = ?
            """,
            (updated_realized, resolved_time, resolved_time, order_id, fill_id, serialized_metadata, position_id),
        )


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def apply_fill_to_positions(
    *,
    symbol: str,
    side: str,
    fill_qty: Optional[float],
    fill_price: Optional[float],
    order_id: str,
    fill_id: str,
    fill_time: Optional[str],
    metadata_json: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    _raw_qty = shared_to_optional_float(fill_qty)
    qty_value = _raw_qty if _raw_qty is not None else 0.0
    price_value = shared_to_optional_float(fill_price)
    if qty_value <= 0.0:
        return {"status": "no_position_change", "position": None}

    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"long", "short"}:
        raise ValueError("fill side must be long or short")

    resolved_time = fill_time or _current_utc_iso()
    serialized_metadata = json.dumps(metadata_json or {}, ensure_ascii=False, sort_keys=True)

    with get_connection() as conn:
        open_row = conn.execute(
            """
            SELECT *
            FROM positions
            WHERE symbol = ? AND status = 'open'
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()

        if open_row is None:
            new_id = _insert_open_position(conn, symbol, normalized_side, qty_value, price_value, resolved_time, order_id, fill_id, serialized_metadata)
            conn.commit()
            return {"status": "opened", "position": _fetch_position_row(conn, new_id)}

        existing = dict(open_row)
        existing_side = str(existing.get("side") or "").strip().lower()

        if existing_side == normalized_side:
            return _apply_same_side_fill(conn, existing, qty_value, price_value, resolved_time, order_id, fill_id, serialized_metadata)

        return _apply_opposite_side_fill(conn, existing, normalized_side, symbol, qty_value, price_value, resolved_time, order_id, fill_id, serialized_metadata)


def get_recent_broker_orders(
    limit: int,
    *,
    event_id: Optional[str] = None,
    execution_request_id: Optional[str] = None,
    order_id: Optional[str] = None,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    status: Optional[str] = None,
    mode: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    if event_id is not None:
        where_clauses.append(_WC_EVENT_ID)
        params.append(event_id)

    if execution_request_id is not None:
        where_clauses.append("execution_request_id = ?")
        params.append(execution_request_id)

    if order_id is not None:
        where_clauses.append("order_id = ?")
        params.append(order_id)

    if symbol is not None:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)

    if side is not None:
        where_clauses.append(_WC_SIDE)
        params.append(side)

    if status is not None:
        where_clauses.append("status = ?")
        params.append(status)

    if mode is not None:
        where_clauses.append("mode = ?")
        params.append(mode)

    query = """
        SELECT *
        FROM broker_orders
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY updated_at DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_recent_fills(
    limit: int,
    *,
    event_id: Optional[str] = None,
    execution_request_id: Optional[str] = None,
    order_id: Optional[str] = None,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    if event_id is not None:
        where_clauses.append(_WC_EVENT_ID)
        params.append(event_id)

    if execution_request_id is not None:
        where_clauses.append("execution_request_id = ?")
        params.append(execution_request_id)

    if order_id is not None:
        where_clauses.append("order_id = ?")
        params.append(order_id)

    if symbol is not None:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)

    if side is not None:
        where_clauses.append(_WC_SIDE)
        params.append(side)

    query = """
        SELECT *
        FROM fills
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY fill_time DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_recent_positions(
    limit: int,
    *,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    status: Optional[str] = None,
    open_only: bool = False,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    if symbol is not None:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)

    if side is not None:
        where_clauses.append(_WC_SIDE)
        params.append(side)

    if open_only:
        where_clauses.append("status = 'open'")
    elif status is not None:
        where_clauses.append("status = ?")
        params.append(status)

    query = """
        SELECT *
        FROM positions
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY updated_at DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def is_database_reachable() -> bool:
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return True
    except sqlite3.Error:
        return False


def get_recent_bar_states(limit: int) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    query = """
        SELECT *
        FROM bar_states
        ORDER BY id DESC
        LIMIT ?
    """

    with get_connection() as conn:
        rows = conn.execute(query, (safe_limit,)).fetchall()

    return [dict(row) for row in rows]


def get_recent_feature_snapshots(
    limit: int,
    *,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    where_clauses: list[str] = []
    params: list[Any] = []

    normalized_symbol = symbol.strip() if symbol is not None else None
    if normalized_symbol:
        where_clauses.append(_WC_SYMBOL)
        params.append(normalized_symbol)

    normalized_timeframe = timeframe.strip() if timeframe is not None else None
    if normalized_timeframe:
        where_clauses.append(_WC_TIMEFRAME)
        params.append(normalized_timeframe)

    query = """
        SELECT
            id,
            timestamp,
            symbol,
            timeframe,
            source_bar_id,
            regime_id,
            long_probability,
            short_probability,
            no_trade_probability,
            feature_json
        FROM feature_snapshots
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY timestamp DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_bar_state_by_id(id: int) -> Optional[Dict[str, Any]]:
    query = """
        SELECT *
        FROM bar_states
        WHERE id = ?
        LIMIT 1
    """

    with get_connection() as conn:
        row = conn.execute(query, (id,)).fetchone()

    return dict(row) if row is not None else None


def get_recent_bar_states_for_symbol_timeframe(
    *,
    symbol: str,
    timeframe: str,
    limit: int = 500,
    ascending: bool = False,
) -> list[Dict[str, Any]]:
    normalized_symbol = symbol.strip()
    normalized_timeframe = timeframe.strip()
    if not normalized_symbol or not normalized_timeframe:
        raise ValueError("symbol and timeframe must not be empty")

    safe_limit = max(1, min(int(limit), 5000))
    order_clause = "ORDER BY julianday(timestamp) ASC, id ASC" if ascending else "ORDER BY id DESC"

    query = f"""
        SELECT *
        FROM bar_states
        WHERE symbol = ? AND timeframe = ?
        {order_clause}
        LIMIT ?
    """

    with get_connection() as conn:
        rows = conn.execute(query, (normalized_symbol, normalized_timeframe, safe_limit)).fetchall()

    return [dict(row) for row in rows]


def insert_regime_state(
    *,
    timestamp: str,
    symbol: str,
    regime_id: Optional[str],
    regime_confidence: Any,
    transition_risk: Any,
) -> int:
    query = """
        INSERT INTO regime_states (
            timestamp,
            symbol,
            regime_id,
            regime_confidence,
            transition_risk
        ) VALUES (?, ?, ?, ?, ?)
    """

    values = (
        timestamp,
        symbol,
        regime_id,
        shared_to_optional_float(regime_confidence),
        shared_to_optional_float(transition_risk),
    )

    with get_connection() as conn:
        cursor = conn.execute(query, values)
        conn.commit()
        return _require_lastrowid(cursor)


def insert_model_prediction(
    *,
    timestamp: str,
    symbol: str,
    long_probability: Any,
    short_probability: Any,
    no_trade_probability: Any,
    expected_excursion: Any,
    setup_trust_score: Any,
) -> int:
    query = """
        INSERT INTO model_predictions (
            timestamp,
            symbol,
            long_probability,
            short_probability,
            no_trade_probability,
            expected_excursion,
            setup_trust_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        timestamp,
        symbol,
        shared_to_optional_float(long_probability),
        shared_to_optional_float(short_probability),
        shared_to_optional_float(no_trade_probability),
        shared_to_optional_float(expected_excursion),
        shared_to_optional_float(setup_trust_score),
    )

    with get_connection() as conn:
        cursor = conn.execute(query, values)
        conn.commit()
        return _require_lastrowid(cursor)


def register_feature_registry_entries(specs: Iterable[Any]) -> None:
    now_iso = _current_utc_iso()

    def _get_value(spec: Any, key: str) -> Any:
        if isinstance(spec, dict):
            return spec.get(key)
        return getattr(spec, key, None)

    with get_connection() as conn:
        for spec in specs:
            engine_name = _get_value(spec, "engine")
            feature_key = _get_value(spec, "key")
            value_type = _get_value(spec, "value_type") or "float"
            description = _get_value(spec, "description")
            unit = _get_value(spec, "unit")

            if not engine_name or not feature_key:
                continue

            conn.execute(
                """
                INSERT INTO feature_registry (
                    engine_name,
                    feature_key,
                    value_type,
                    description,
                    unit,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(engine_name, feature_key)
                DO UPDATE SET
                    value_type = excluded.value_type,
                    description = excluded.description,
                    unit = excluded.unit,
                    updated_at = excluded.updated_at
                """,
                (
                    str(engine_name),
                    str(feature_key),
                    str(value_type),
                    str(description) if description is not None else None,
                    str(unit) if unit is not None else None,
                    now_iso,
                    now_iso,
                ),
            )
        conn.commit()


def insert_feature_snapshot(
    *,
    timestamp: str,
    symbol: str,
    timeframe: str,
    source_bar_id: Optional[int],
    feature_version: str,
    feature_values: Dict[str, Any],
    regime_output: Dict[str, Any],
    model_output: Dict[str, Any],
) -> int:
    now_iso = _current_utc_iso()
    serialized_feature_json = json.dumps(feature_values, ensure_ascii=False)

    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO feature_snapshots (
                timestamp,
                symbol,
                timeframe,
                source_bar_id,
                feature_version,
                regime_id,
                regime_confidence,
                transition_risk,
                long_probability,
                short_probability,
                no_trade_probability,
                expected_excursion,
                setup_trust_score,
                feature_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                symbol,
                timeframe,
                source_bar_id,
                feature_version,
                regime_output.get("regime_id"),
                shared_to_optional_float(regime_output.get("regime_confidence")),
                shared_to_optional_float(regime_output.get("transition_risk")),
                shared_to_optional_float(model_output.get("long_probability")),
                shared_to_optional_float(model_output.get("short_probability")),
                shared_to_optional_float(model_output.get("no_trade_probability")),
                shared_to_optional_float(model_output.get("expected_excursion")),
                shared_to_optional_float(model_output.get("setup_trust_score")),
                serialized_feature_json,
                now_iso,
            ),
        )
        snapshot_id = _require_lastrowid(cursor)

        for feature_key, raw_value in feature_values.items():
            numeric_value = shared_to_optional_float(raw_value)
            text_value: Optional[str] = None

            if isinstance(raw_value, bool):
                numeric_value = 1.0 if raw_value else 0.0
                text_value = "true" if raw_value else "false"
            elif numeric_value is None and raw_value is not None:
                text_value = str(raw_value)

            conn.execute(
                """
                INSERT OR REPLACE INTO feature_snapshot_values (
                    snapshot_id,
                    feature_key,
                    feature_value,
                    feature_text,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    str(feature_key),
                    numeric_value,
                    text_value,
                    now_iso,
                ),
            )

        conn.commit()

    return snapshot_id


def attach_feature_snapshot_to_bar_state(
    *,
    bar_state_id: int,
    snapshot_id: int,
    feature_values: Dict[str, Any],
    regime_output: Dict[str, Any],
    model_output: Dict[str, Any],
) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT payload_json FROM bar_states WHERE id = ? LIMIT 1",
            (bar_state_id,),
        ).fetchone()
        if row is None:
            return False

        existing_payload = row["payload_json"]
        if isinstance(existing_payload, str):
            try:
                payload = json.loads(existing_payload)
            except json.JSONDecodeError:
                payload = {}
        elif isinstance(existing_payload, dict):
            payload = dict(existing_payload)
        else:
            payload = {}

        if not isinstance(payload, dict):
            payload = {}

        payload["feature_snapshot_id"] = snapshot_id
        payload["computed_features"] = feature_values
        payload["computed_regime"] = regime_output
        payload["computed_model"] = model_output

        conn.execute(
            "UPDATE bar_states SET payload_json = ? WHERE id = ?",
            (json.dumps(payload, ensure_ascii=False), bar_state_id),
        )
        conn.commit()

    return True


def insert_trade_candidate(candidate: dict) -> int:
    timestamp = candidate.get("timestamp")
    symbol = candidate.get("symbol")
    if not timestamp or not symbol:
        raise ValueError("candidate must include timestamp and symbol")

    payload = candidate.get("payload_json")
    if payload is None:
        serialized_payload = None
    elif isinstance(payload, str):
        serialized_payload = payload
    else:
        serialized_payload = json.dumps(payload, ensure_ascii=False)

    query = """
        INSERT INTO trade_candidates (
            signal_id,
            timestamp,
            symbol,
            direction,
            entry_price,
            stop_price,
            tp1,
            tp2,
            confidence,
            setup_family,
            payload_json,
            execution_status,
            execution_note,
            executed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        candidate.get("signal_id"),
        timestamp,
        symbol,
        candidate.get("direction"),
        candidate.get("entry_price"),
        candidate.get("stop_price"),
        candidate.get("tp1"),
        candidate.get("tp2"),
        candidate.get("confidence"),
        candidate.get("setup_family"),
        serialized_payload,
        candidate.get("execution_status") or "pending",
        candidate.get("execution_note"),
        candidate.get("executed_at"),
    )

    with get_connection() as conn:
        cursor = conn.execute(query, values)
        conn.commit()
        return _require_lastrowid(cursor)


def _extract_payload_value(payload: Any, key: str) -> Optional[str]:
    if payload is None:
        return None

    if isinstance(payload, dict):
        value = payload.get(key)
        return str(value) if value is not None else None

    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            value = parsed.get(key)
            return str(value) if value is not None else None

    return None


def _find_existing_trade_candidate_for_replay(candidate: dict) -> Optional[Dict[str, Any]]:
    event_id = _extract_payload_value(candidate.get("payload_json"), "event_id")
    signal_id = candidate.get("signal_id")
    symbol = candidate.get("symbol")
    direction = candidate.get("direction")
    timestamp = candidate.get("timestamp")

    with get_connection() as conn:
        if event_id:
            row = conn.execute(
                """
                SELECT *
                FROM trade_candidates
                WHERE json_valid(payload_json)
                  AND json_extract(payload_json, '$.event_id') = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (event_id,),
            ).fetchone()
            return dict(row) if row is not None else None

        if signal_id:
            row = conn.execute(
                """
                SELECT *
                FROM trade_candidates
                WHERE signal_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (signal_id,),
            ).fetchone()
            return dict(row) if row is not None else None

        if not symbol or not timestamp:
            return None

        if direction is None:
            row = conn.execute(
                """
                SELECT *
                FROM trade_candidates
                WHERE symbol = ?
                  AND timestamp = ?
                  AND direction IS NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (symbol, timestamp),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT *
                FROM trade_candidates
                WHERE symbol = ?
                  AND direction = ?
                  AND timestamp = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (symbol, direction, timestamp),
            ).fetchone()

    return dict(row) if row is not None else None


def insert_trade_candidate_from_event(candidate: dict) -> Dict[str, Any]:
    existing = _find_existing_trade_candidate_for_replay(candidate)
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "inserted": False,
            "duplicate": True,
        }

    row_id = insert_trade_candidate(candidate)
    return {
        "id": row_id,
        "inserted": True,
        "duplicate": False,
    }


def update_trade_candidate_status(
    candidate_id: int,
    *,
    execution_status: str,
    execution_note: Any = UNSET,
    executed_at: Any = UNSET,
) -> Optional[Dict[str, Any]]:
    assignments = [_WC_EXECUTION_STATUS]
    params: list[Any] = [execution_status]

    if execution_note is not UNSET:
        assignments.append("execution_note = ?")
        params.append(execution_note)

    if executed_at is not UNSET:
        assignments.append("executed_at = ?")
        params.append(executed_at)

    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM trade_candidates WHERE id = ? LIMIT 1",
            (candidate_id,),
        ).fetchone()
        if existing is None:
            return None

        params.append(candidate_id)
        conn.execute(
            f"UPDATE trade_candidates SET {', '.join(assignments)} WHERE id = ?",
            tuple(params),
        )
        conn.commit()

        row = conn.execute(
            _SQL_SELECT_TRADE_CANDIDATE_BY_ID,
            (candidate_id,),
        ).fetchone()

    return dict(row) if row is not None else None


def _get_claim_timeout_seconds(override: Optional[int] = None) -> int:
    if override is not None:
        return max(0, int(override))

    raw_value = os.getenv("SIGNAL_CLAIM_TIMEOUT_SECONDS", str(DEFAULT_CLAIM_TIMEOUT_SECONDS))
    try:
        parsed_value = int(raw_value)
    except ValueError:
        parsed_value = DEFAULT_CLAIM_TIMEOUT_SECONDS
    return max(0, parsed_value)


def _get_env_float(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return float(default)

    try:
        return float(raw_value)
    except ValueError:
        return float(default)


def _current_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_stale_clause() -> str:
    return "(claimed_at IS NULL OR julianday(claimed_at) <= julianday(?))"


def claim_next_trade_candidate(worker_id: str, *, timeout_seconds: Optional[int] = None) -> Optional[Dict[str, Any]]:
    normalized_worker = worker_id.strip()
    if not normalized_worker:
        raise ValueError("worker_id must not be empty")

    now_utc = datetime.now(timezone.utc)
    claim_token = uuid4().hex
    claimed_at = now_utc.isoformat()
    stale_before = (now_utc - timedelta(seconds=_get_claim_timeout_seconds(timeout_seconds))).isoformat()

    with get_connection() as conn:
        conn.execute("BEGIN IMMEDIATE")

        candidate = conn.execute(
            f"""
            SELECT id
            FROM trade_candidates
            WHERE execution_status = 'pending'
               OR (execution_status = 'submitted' AND {_is_stale_clause()})
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """,
            (stale_before,),
        ).fetchone()

        if candidate is None:
            conn.commit()
            return None

        candidate_id = int(candidate["id"])

        update_result = conn.execute(
            f"""
            UPDATE trade_candidates
            SET execution_status = 'submitted',
                claimed_by = ?,
                claim_token = ?,
                claimed_at = ?
            WHERE id = ?
              AND (
                    execution_status = 'pending'
                 OR (execution_status = 'submitted' AND {_is_stale_clause()})
              )
            """,
            (normalized_worker, claim_token, claimed_at, candidate_id, stale_before),
        )

        if update_result.rowcount != 1:
            conn.commit()
            return None

        row = conn.execute(
            _SQL_SELECT_TRADE_CANDIDATE_BY_ID,
            (candidate_id,),
        ).fetchone()
        conn.commit()

    return dict(row) if row is not None else None


def heartbeat_trade_candidate_claim(
    candidate_id: int,
    *,
    worker_id: str,
    claim_token: str,
) -> Dict[str, Any]:
    normalized_worker = worker_id.strip()
    normalized_token = claim_token.strip()
    if not normalized_worker or not normalized_token:
        raise ValueError("worker_id and claim_token must not be empty")

    refreshed_at = _current_utc_iso()

    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, claimed_by, claim_token FROM trade_candidates WHERE id = ? LIMIT 1",
            (candidate_id,),
        ).fetchone()

        if row is None:
            return {"status": "not_found", "row": None}

        if row["claimed_by"] != normalized_worker or row["claim_token"] != normalized_token:
            return {"status": "lease_mismatch", "row": None}

        conn.execute(
            "UPDATE trade_candidates SET claimed_at = ? WHERE id = ?",
            (refreshed_at, candidate_id),
        )
        conn.commit()

        refreshed_row = conn.execute(
            _SQL_SELECT_TRADE_CANDIDATE_BY_ID,
            (candidate_id,),
        ).fetchone()

    return {
        "status": "ok",
        "row": dict(refreshed_row) if refreshed_row is not None else None,
    }


def release_trade_candidate_claim(
    candidate_id: int,
    *,
    worker_id: str,
    claim_token: str,
    execution_status: str = "pending",
    execution_note: Any = UNSET,
) -> Dict[str, Any]:
    allowed_statuses = {"pending", "skipped", "rejected"}
    if execution_status not in allowed_statuses:
        raise ValueError("execution_status must be pending, skipped, or rejected")

    normalized_worker = worker_id.strip()
    normalized_token = claim_token.strip()
    if not normalized_worker or not normalized_token:
        raise ValueError("worker_id and claim_token must not be empty")

    assignments = [
        _WC_EXECUTION_STATUS,
        "claimed_by = NULL",
        "claim_token = NULL",
        "claimed_at = NULL",
    ]
    params: list[Any] = [execution_status]

    if execution_status == "pending":
        assignments.append("executed_at = NULL")
    else:
        assignments.append("executed_at = ?")
        params.append(_current_utc_iso())

    if execution_note is not UNSET:
        assignments.append("execution_note = ?")
        params.append(execution_note)

    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, claimed_by, claim_token FROM trade_candidates WHERE id = ? LIMIT 1",
            (candidate_id,),
        ).fetchone()

        if row is None:
            return {"status": "not_found", "row": None}

        if row["claimed_by"] != normalized_worker or row["claim_token"] != normalized_token:
            return {"status": "lease_mismatch", "row": None}

        params.append(candidate_id)
        conn.execute(
            f"UPDATE trade_candidates SET {', '.join(assignments)} WHERE id = ?",
            tuple(params),
        )
        conn.commit()

        released_row = conn.execute(
            _SQL_SELECT_TRADE_CANDIDATE_BY_ID,
            (candidate_id,),
        ).fetchone()

    return {
        "status": "released",
        "row": dict(released_row) if released_row is not None else None,
    }


def get_trade_candidate_execution_summary() -> Dict[str, int]:
    stale_before = (datetime.now(timezone.utc) - timedelta(seconds=_get_claim_timeout_seconds())).isoformat()

    with get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT
                SUM(CASE WHEN execution_status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN execution_status = 'submitted' THEN 1 ELSE 0 END) AS submitted,
                SUM(CASE WHEN execution_status = 'filled' THEN 1 ELSE 0 END) AS filled,
                SUM(CASE WHEN execution_status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
                SUM(CASE WHEN execution_status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN execution_status = 'submitted' AND claimed_by IS NOT NULL AND claim_token IS NOT NULL THEN 1 ELSE 0 END) AS leased_submitted_count,
                SUM(CASE WHEN execution_status = 'submitted' AND {_is_stale_clause()} THEN 1 ELSE 0 END) AS stale_submitted_count,
                COUNT(*) AS total
            FROM trade_candidates
            """,
            (stale_before,),
        ).fetchone()

    if row is None:
        return {
            "pending": 0,
            "submitted": 0,
            "filled": 0,
            "rejected": 0,
            "skipped": 0,
            "leased_submitted_count": 0,
            "stale_submitted_count": 0,
            "total": 0,
        }

    return {
        "pending": int(row["pending"] or 0),
        "submitted": int(row["submitted"] or 0),
        "filled": int(row["filled"] or 0),
        "rejected": int(row["rejected"] or 0),
        "skipped": int(row["skipped"] or 0),
        "leased_submitted_count": int(row["leased_submitted_count"] or 0),
        "stale_submitted_count": int(row["stale_submitted_count"] or 0),
        "total": int(row["total"] or 0),
    }


def insert_execution_journal_entry(
    *,
    candidate_id: int,
    signal_id: Optional[str],
    worker_id: str,
    action: str,
    execution_status: str,
    execution_note: Optional[str] = None,
    confidence: Optional[float] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    entry_price: Optional[float] = None,
    metadata_json: Any = None,
    created_at: Optional[str] = None,
) -> int:
    journal_created_at = created_at or _current_utc_iso()

    if metadata_json is None:
        serialized_metadata = None
    elif isinstance(metadata_json, str):
        serialized_metadata = metadata_json
    else:
        serialized_metadata = json.dumps(metadata_json, ensure_ascii=False)

    query = """
        INSERT INTO execution_journal (
            candidate_id,
            signal_id,
            worker_id,
            action,
            execution_status,
            execution_note,
            confidence,
            symbol,
            direction,
            entry_price,
            created_at,
            metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        int(candidate_id),
        signal_id,
        worker_id,
        action,
        execution_status,
        execution_note,
        confidence,
        symbol,
        direction,
        entry_price,
        journal_created_at,
        serialized_metadata,
    )

    with get_connection() as conn:
        cursor = conn.execute(query, values)
        conn.commit()
        return _require_lastrowid(cursor)


def get_recent_execution_journal(
    limit: int,
    *,
    worker_id: Optional[str] = None,
    execution_status: Optional[str] = None,
    symbol: Optional[str] = None,
    action: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))

    where_clauses: list[str] = []
    params: list[Any] = []

    if worker_id is not None:
        where_clauses.append("worker_id = ?")
        params.append(worker_id)

    if execution_status is not None:
        where_clauses.append(_WC_EXECUTION_STATUS)
        params.append(execution_status)

    if symbol is not None:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)

    if action is not None:
        where_clauses.append("action = ?")
        params.append(action)

    query = """
        SELECT *
        FROM execution_journal
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_execution_journal_timeline(
    limit: int,
    *,
    worker_id: Optional[str] = None,
    execution_status: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    signal_id: Optional[str] = None,
    candidate_id: Optional[int] = None,
    action: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))

    where_clauses: list[str] = []
    params: list[Any] = []

    if worker_id is not None:
        where_clauses.append("ej.worker_id = ?")
        params.append(worker_id)

    if execution_status is not None:
        where_clauses.append("ej.execution_status = ?")
        params.append(execution_status)

    if symbol is not None:
        where_clauses.append("ej.symbol = ?")
        params.append(symbol)

    if direction is not None:
        where_clauses.append("ej.direction = ?")
        params.append(direction)

    if signal_id is not None:
        where_clauses.append("ej.signal_id = ?")
        params.append(signal_id)

    if candidate_id is not None:
        where_clauses.append("ej.candidate_id = ?")
        params.append(int(candidate_id))

    if action is not None:
        where_clauses.append("ej.action = ?")
        params.append(action)

    query = """
        SELECT
            ej.id AS journal_id,
            ej.candidate_id,
            ej.signal_id,
            ej.worker_id,
            ej.action,
            ej.execution_status,
            ej.execution_note,
            ej.confidence,
            ej.symbol,
            ej.direction,
            ej.entry_price,
            tc.timestamp AS candidate_timestamp,
            ej.created_at AS journal_created_at,
            CASE
                WHEN json_valid(tc.payload_json)
                THEN json_extract(tc.payload_json, '$.strategy')
                ELSE NULL
            END AS strategy,
            CASE
                WHEN json_valid(tc.payload_json)
                THEN json_extract(tc.payload_json, '$.source')
                ELSE NULL
            END AS source,
            COALESCE(
                tc.setup_family,
                CASE WHEN json_valid(tc.payload_json) THEN json_extract(tc.payload_json, '$.setup_family') ELSE NULL END,
                CASE WHEN json_valid(tc.payload_json) THEN json_extract(tc.payload_json, '$.event_type') ELSE NULL END,
                CASE WHEN json_valid(tc.payload_json) THEN json_extract(tc.payload_json, '$.strategy') ELSE NULL END
            ) AS setup_family,
            tc.claimed_by,
            tc.claim_token,
            ej.metadata_json
        FROM execution_journal ej
        LEFT JOIN trade_candidates tc ON tc.id = ej.candidate_id
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY ej.created_at DESC, ej.id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_execution_journal_summary() -> Dict[str, Any]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN execution_status = 'filled' THEN 1 ELSE 0 END) AS filled,
                SUM(CASE WHEN execution_status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN execution_status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
                SUM(CASE WHEN execution_status NOT IN ('filled', 'skipped', 'rejected') THEN 1 ELSE 0 END) AS other,
                COUNT(*) AS total,
                COUNT(DISTINCT worker_id) AS worker_count,
                MAX(created_at) AS latest_created_at
            FROM execution_journal
            """
        ).fetchone()

    if row is None:
        return {
            "filled": 0,
            "skipped": 0,
            "rejected": 0,
            "other": 0,
            "total": 0,
            "worker_count": 0,
            "latest_created_at": None,
        }

    return {
        "filled": int(row["filled"] or 0),
        "skipped": int(row["skipped"] or 0),
        "rejected": int(row["rejected"] or 0),
        "other": int(row["other"] or 0),
        "total": int(row["total"] or 0),
        "worker_count": int(row["worker_count"] or 0),
        "latest_created_at": row["latest_created_at"],
    }


def _build_execution_journal_filters(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    execution_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> tuple[list[str], list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if worker_id is not None:
        where_clauses.append("worker_id = ?")
        params.append(worker_id)

    if symbol is not None:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)

    if execution_status is not None:
        where_clauses.append(_WC_EXECUTION_STATUS)
        params.append(execution_status)

    if since is not None:
        where_clauses.append("created_at >= ?")
        params.append(since)

    if until is not None:
        where_clauses.append("created_at <= ?")
        params.append(until)

    return where_clauses, params


def _get_execution_journal_filtered_rows(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    execution_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[Dict[str, Any]]:
    where_clauses, params = _build_execution_journal_filters(
        worker_id=worker_id,
        symbol=symbol,
        execution_status=execution_status,
        since=since,
        until=until,
    )

    query = """
        SELECT *
        FROM execution_journal
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY created_at DESC, id DESC"

    if limit is not None:
        safe_limit = max(1, min(int(limit), 5000))
        query += _LIMIT
        params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def _to_optional_float(value: Any) -> Optional[float]:
    return shared_to_optional_float(value)


def _average(values: list[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _sample_standard_deviation(values: list[float]) -> Optional[float]:
    sample_count = len(values)
    if sample_count < 2:
        return None

    mean = sum(values) / sample_count
    variance = sum((value - mean) ** 2 for value in values) / (sample_count - 1)
    return math.sqrt(variance)


def _mean_confidence_interval_95(*, mean: Optional[float], stdev: Optional[float], sample_count: int) -> tuple[Optional[float], Optional[float]]:
    if sample_count < 3 or mean is None or stdev is None:
        return None, None
    margin = 1.96 * (stdev / math.sqrt(sample_count))
    return mean - margin, mean + margin


def get_execution_journal_analytics(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    execution_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    rows = _get_execution_journal_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        execution_status=execution_status,
        since=since,
        until=until,
        limit=limit,
    )

    total = len(rows)
    filled_count = sum(1 for row in rows if row.get("execution_status") == "filled")
    skipped_count = sum(1 for row in rows if row.get("execution_status") == "skipped")
    rejected_count = sum(1 for row in rows if row.get("execution_status") == "rejected")

    confidence_values: list[float] = []
    confidence_filled: list[float] = []
    confidence_skipped: list[float] = []
    confidence_rejected: list[float] = []

    by_symbol: dict[str, int] = {}
    by_worker: dict[str, int] = {}

    for row in rows:
        row_symbol = row.get("symbol")
        if row_symbol:
            by_symbol[str(row_symbol)] = by_symbol.get(str(row_symbol), 0) + 1

        row_worker = row.get("worker_id")
        if row_worker:
            by_worker[str(row_worker)] = by_worker.get(str(row_worker), 0) + 1

        confidence_value = _to_optional_float(row.get("confidence"))
        if confidence_value is None:
            continue

        confidence_values.append(confidence_value)

        status_value = row.get("execution_status")
        if status_value == "filled":
            confidence_filled.append(confidence_value)
        elif status_value == "skipped":
            confidence_skipped.append(confidence_value)
        elif status_value == "rejected":
            confidence_rejected.append(confidence_value)

    if total == 0:
        fill_rate = 0.0
        skip_rate = 0.0
        reject_rate = 0.0
    else:
        fill_rate = filled_count / total
        skip_rate = skipped_count / total
        reject_rate = rejected_count / total

    latest_created_at = rows[0].get("created_at") if rows else None

    return {
        "total_decisions": total,
        "filled_count": filled_count,
        "skipped_count": skipped_count,
        "rejected_count": rejected_count,
        "fill_rate": fill_rate,
        "skip_rate": skip_rate,
        "reject_rate": reject_rate,
        "avg_confidence": _average(confidence_values),
        "avg_confidence_filled": _average(confidence_filled),
        "avg_confidence_skipped": _average(confidence_skipped),
        "avg_confidence_rejected": _average(confidence_rejected),
        "by_symbol": dict(sorted(by_symbol.items())),
        "by_worker": dict(sorted(by_worker.items())),
        "latest_created_at": latest_created_at,
    }


def get_execution_journal_daily_rollup(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: int = 30,
) -> list[Dict[str, Any]]:
    rows = _get_execution_journal_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        since=since,
        until=until,
        limit=None,
    )

    bucket: dict[str, Dict[str, Any]] = {}

    for row in rows:
        created_at = row.get("created_at")
        if not created_at:
            continue

        day = str(created_at)[:10]
        if day not in bucket:
            bucket[day] = {
                "day": day,
                "total": 0,
                "filled": 0,
                "skipped": 0,
                "rejected": 0,
            }

        bucket_row = bucket[day]
        bucket_row["total"] += 1

        status_value = row.get("execution_status")
        if status_value == "filled":
            bucket_row["filled"] += 1
        elif status_value == "skipped":
            bucket_row["skipped"] += 1
        elif status_value == "rejected":
            bucket_row["rejected"] += 1

    ordered_rows = sorted(bucket.values(), key=lambda item: item["day"], reverse=True)
    safe_limit = max(1, min(int(limit), 500))
    return ordered_rows[:safe_limit]


@dataclass
class ExecutionOutcomeParams:
    """Bundles all fields required to persist one execution outcome row.

    Using a dataclass keeps ``insert_execution_outcome`` well below the
    cyclomatic / parameter-count thresholds enforced by static analysis
    (e.g. SonarQube python:S107) while preserving a strongly-typed,
    self-documenting contract for callers.
    """

    # ── required / identity fields ──────────────────────────────────────
    journal_id: int
    candidate_id: int
    evaluation_window_minutes: int
    outcome_status: str

    # ── signal-context fields (nullable) ────────────────────────────────
    signal_id: Optional[str] = None
    worker_id: Optional[str] = None
    symbol: Optional[str] = None
    direction: Optional[str] = None
    entry_price: Optional[float] = None
    reference_timestamp: Optional[str] = None

    # ── outcome-metric fields (nullable) ────────────────────────────────
    exit_price: Optional[float] = None
    pnl_points: Optional[float] = None
    pnl_pct: Optional[float] = None
    max_favorable_excursion: Optional[float] = None
    max_adverse_excursion: Optional[float] = None
    evaluated_at: Optional[str] = None
    metadata_json: Any = None


def insert_execution_outcome(params: ExecutionOutcomeParams) -> int:
    outcome_evaluated_at = params.evaluated_at or _current_utc_iso()

    if params.metadata_json is None:
        serialized_metadata = None
    elif isinstance(params.metadata_json, str):
        serialized_metadata = params.metadata_json
    else:
        serialized_metadata = json.dumps(params.metadata_json, ensure_ascii=False)

    query = """
        INSERT INTO execution_outcomes (
            journal_id,
            candidate_id,
            signal_id,
            worker_id,
            symbol,
            direction,
            entry_price,
            reference_timestamp,
            evaluation_window_minutes,
            outcome_status,
            exit_price,
            pnl_points,
            pnl_pct,
            max_favorable_excursion,
            max_adverse_excursion,
            evaluated_at,
            metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        int(params.journal_id),
        int(params.candidate_id),
        params.signal_id,
        params.worker_id,
        params.symbol,
        params.direction,
        params.entry_price,
        params.reference_timestamp,
        int(params.evaluation_window_minutes),
        params.outcome_status,
        params.exit_price,
        params.pnl_points,
        params.pnl_pct,
        params.max_favorable_excursion,
        params.max_adverse_excursion,
        outcome_evaluated_at,
        serialized_metadata,
    )

    with get_connection() as conn:
        cursor = conn.execute(query, values)
        conn.commit()
        return _require_lastrowid(cursor)


def get_pending_filled_journal_for_outcomes(
    *,
    symbol: Optional[str] = None,
    limit: int = 200,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 5000))

    where_clauses = ["ej.execution_status = 'filled'", "eo.id IS NULL"]
    params: list[Any] = []

    if symbol is not None:
        where_clauses.append("ej.symbol = ?")
        params.append(symbol)

    query = """
        SELECT
            ej.id AS journal_id,
            ej.candidate_id,
            ej.signal_id,
            ej.worker_id,
            ej.symbol,
            ej.direction,
            ej.entry_price,
            ej.confidence,
            ej.created_at AS journal_created_at,
            tc.timestamp AS candidate_timestamp,
            tc.payload_json AS candidate_payload_json
        FROM execution_journal ej
        LEFT JOIN trade_candidates tc ON tc.id = ej.candidate_id
        LEFT JOIN execution_outcomes eo ON eo.journal_id = ej.id
    """

    query += _where_clause(where_clauses)
    query += " ORDER BY ej.created_at ASC, ej.id ASC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def get_bar_states_in_window(
    *,
    symbol: str,
    since_timestamp: str,
    until_timestamp: str,
    limit: int = 5000,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 5000))

    query = """
        SELECT id, timestamp, symbol, timeframe, payload_json
        FROM bar_states
        WHERE symbol = ?
                    AND julianday(timestamp) >= julianday(?)
                    AND julianday(timestamp) <= julianday(?)
                ORDER BY julianday(timestamp) ASC, id ASC
        LIMIT ?
    """

    with get_connection() as conn:
        rows = conn.execute(query, (symbol, since_timestamp, until_timestamp, safe_limit)).fetchall()

    return [dict(row) for row in rows]


def get_outcome_label_thresholds() -> tuple[float, float]:
    win_threshold = _get_env_float("OUTCOME_WIN_THRESHOLD_PCT", DEFAULT_OUTCOME_WIN_THRESHOLD_PCT)
    loss_threshold = _get_env_float("OUTCOME_LOSS_THRESHOLD_PCT", DEFAULT_OUTCOME_LOSS_THRESHOLD_PCT)
    return win_threshold, loss_threshold


def _normalize_outcome_label_filter(label: Optional[str]) -> Optional[str]:
    if label is None:
        return None

    normalized = label.strip().lower()
    if not normalized:
        raise ValueError("label must not be empty")

    if normalized not in VALID_OUTCOME_LABELS:
        allowed_labels = ", ".join(sorted(VALID_OUTCOME_LABELS))
        raise ValueError(f"label must be one of: {allowed_labels}")

    return normalized


def _normalize_outcome_group_by(group_by: str) -> str:
    normalized = group_by.strip().lower()
    if not normalized:
        raise ValueError("group_by must not be empty")

    if normalized not in VALID_OUTCOME_COHORT_GROUP_BYS:
        allowed = ", ".join(sorted(VALID_OUTCOME_COHORT_GROUP_BYS))
        raise ValueError(f"group_by must be one of: {allowed}")

    return normalized


def _normalize_policy_scoring_mode(scoring_mode: str) -> str:
    normalized = scoring_mode.strip().lower()
    if not normalized:
        raise ValueError("scoring_mode must not be empty")

    if normalized not in VALID_OUTCOME_POLICY_SCORING_MODES:
        allowed = ", ".join(sorted(VALID_OUTCOME_POLICY_SCORING_MODES))
        raise ValueError(f"scoring_mode must be one of: {allowed}")

    return normalized


def _normalize_policy_matrix_groupings(groupings: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if groupings is None:
        return DEFAULT_POLICY_MATRIX_GROUP_BYS

    normalized_values: list[str] = []
    for group_by in groupings:
        normalized = _normalize_outcome_group_by(group_by)
        if normalized not in normalized_values:
            normalized_values.append(normalized)

    return tuple(normalized_values)


def _get_cohort_key_for_row(row: Dict[str, Any], *, group_by: str) -> str:
    key_map = {
        "strategy": row.get("cohort_strategy"),
        "source": row.get("cohort_source"),
        "setup_family": row.get("cohort_setup_family"),
        "worker_id": row.get("worker_id"),
        "symbol": row.get("symbol"),
        "direction": row.get("direction"),
    }

    raw_value = key_map.get(group_by)
    if raw_value is None:
        return "unknown"

    text_value = str(raw_value).strip()
    return text_value if text_value else "unknown"


def get_execution_outcome_label(
    *,
    outcome_status: Optional[str],
    pnl_pct: Any,
    win_threshold_pct: Optional[float] = None,
    loss_threshold_pct: Optional[float] = None,
) -> str:
    if outcome_status != "evaluated":
        return "unknown"

    pnl_pct_value = _to_optional_float(pnl_pct)
    if pnl_pct_value is None:
        return "unknown"

    resolved_win_threshold = win_threshold_pct
    resolved_loss_threshold = loss_threshold_pct
    if resolved_win_threshold is None or resolved_loss_threshold is None:
        env_win_threshold, env_loss_threshold = get_outcome_label_thresholds()
        if resolved_win_threshold is None:
            resolved_win_threshold = env_win_threshold
        if resolved_loss_threshold is None:
            resolved_loss_threshold = env_loss_threshold

    if pnl_pct_value > float(resolved_win_threshold):
        return "winner"

    if pnl_pct_value < float(resolved_loss_threshold):
        return "loser"

    return "scratch"


def _build_execution_outcome_filters(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    column_prefix: str = "",
) -> tuple[list[str], list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []

    def _col(name: str) -> str:
        return f"{column_prefix}{name}"

    if worker_id is not None:
        where_clauses.append(f"{_col('worker_id')} = ?")
        params.append(worker_id)

    if symbol is not None:
        where_clauses.append(f"{_col('symbol')} = ?")
        params.append(symbol)

    if direction is not None:
        where_clauses.append(f"{_col('direction')} = ?")
        params.append(direction)

    if outcome_status is not None:
        where_clauses.append(f"{_col('outcome_status')} = ?")
        params.append(outcome_status)

    if signal_id is not None:
        where_clauses.append(f"{_col('signal_id')} = ?")
        params.append(signal_id)

    if since is not None:
        where_clauses.append(f"julianday({_col('evaluated_at')}) >= julianday(?)")
        params.append(since)

    if until is not None:
        where_clauses.append(f"julianday({_col('evaluated_at')}) <= julianday(?)")
        params.append(until)

    return where_clauses, params


def _get_execution_outcome_filtered_rows(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[Dict[str, Any]]:
    where_clauses, params = _build_execution_outcome_filters(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
    )

    query = """
        SELECT *
        FROM execution_outcomes
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY evaluated_at DESC, id DESC"

    if limit is not None:
        safe_limit = max(1, min(int(limit), 5000))
        query += _LIMIT
        params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def _with_execution_outcome_labels(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    win_threshold, loss_threshold = get_outcome_label_thresholds()

    labeled_rows: list[Dict[str, Any]] = []
    for row in rows:
        labeled_row = dict(row)
        labeled_row["label"] = get_execution_outcome_label(
            outcome_status=row.get("outcome_status"),
            pnl_pct=row.get("pnl_pct"),
            win_threshold_pct=win_threshold,
            loss_threshold_pct=loss_threshold,
        )
        labeled_rows.append(labeled_row)

    return labeled_rows


def _get_enriched_execution_outcome_filtered_rows(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: Optional[int] = None,
    order_by_ascending: bool = False,
) -> list[Dict[str, Any]]:
    where_clauses, params = _build_execution_outcome_filters(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        column_prefix="eo.",
    )

    query = """
        SELECT
            eo.*,
            tc.setup_family AS cohort_setup_family,
            CASE
                WHEN json_valid(tc.payload_json)
                THEN json_extract(tc.payload_json, '$.strategy')
                ELSE NULL
            END AS cohort_strategy,
            CASE
                WHEN json_valid(tc.payload_json)
                THEN json_extract(tc.payload_json, '$.source')
                ELSE NULL
            END AS cohort_source
        FROM execution_outcomes eo
        LEFT JOIN trade_candidates tc ON tc.id = eo.candidate_id
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    if order_by_ascending:
        query += " ORDER BY julianday(eo.evaluated_at) ASC, eo.id ASC"
    else:
        query += " ORDER BY eo.evaluated_at DESC, eo.id DESC"

    if limit is not None:
        safe_limit = max(1, min(int(limit), 5000))
        query += _LIMIT
        params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def _collect_labeled_outcome_metrics(rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    evaluated_count = sum(1 for row in rows if row.get("outcome_status") == "evaluated")

    winner_count = sum(1 for row in rows if row.get("label") == "winner")
    loser_count = sum(1 for row in rows if row.get("label") == "loser")
    scratch_count = sum(1 for row in rows if row.get("label") == "scratch")
    unknown_count = sum(1 for row in rows if row.get("label") == "unknown")
    labeled_count = winner_count + loser_count + scratch_count

    if labeled_count == 0:
        win_rate = 0.0
        loss_rate = 0.0
        scratch_rate = 0.0
    else:
        win_rate = winner_count / labeled_count
        loss_rate = loser_count / labeled_count
        scratch_rate = scratch_count / labeled_count

    pnl_points_values = [
        float(row["pnl_points"])
        for row in rows
        if row.get("pnl_points") is not None
    ]
    pnl_pct_values = [
        float(row["pnl_pct"])
        for row in rows
        if row.get("pnl_pct") is not None
    ]

    expectancy_points_values = [
        float(row["pnl_points"])
        for row in rows
        if row.get("label") in {"winner", "loser", "scratch"} and row.get("pnl_points") is not None
    ]
    expectancy_pct_values = [
        float(row["pnl_pct"])
        for row in rows
        if row.get("label") in {"winner", "loser", "scratch"} and row.get("pnl_pct") is not None
    ]

    latest_evaluated_at_values = [
        str(row["evaluated_at"])
        for row in rows
        if row.get("evaluated_at") is not None
    ]

    return {
        "total": total,
        "evaluated_count": evaluated_count,
        "labeled_count": labeled_count,
        "winner_count": winner_count,
        "loser_count": loser_count,
        "scratch_count": scratch_count,
        "unknown_count": unknown_count,
        "win_rate": win_rate,
        "loss_rate": loss_rate,
        "scratch_rate": scratch_rate,
        "avg_pnl_points": _average(pnl_points_values),
        "avg_pnl_pct": _average(pnl_pct_values),
        "expectancy_points": _average(expectancy_points_values),
        "expectancy_pct": _average(expectancy_pct_values),
        "best_pnl_points": max(pnl_points_values) if pnl_points_values else None,
        "worst_pnl_points": min(pnl_points_values) if pnl_points_values else None,
        "latest_evaluated_at": max(latest_evaluated_at_values) if latest_evaluated_at_values else None,
    }


def _metric_delta(left_value: Any, right_value: Any) -> Optional[float]:
    left_num = _to_optional_float(left_value)
    right_num = _to_optional_float(right_value)
    if left_num is None or right_num is None:
        return None
    return left_num - right_num


def _safe_numeric_for_sort(value: Any, *, fallback: float = -1e18) -> float:
    numeric = _to_optional_float(value)
    return numeric if numeric is not None else fallback


def _to_utc_datetime(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_vp_reason_time_window_filters(
    *,
    since_days: Optional[int] = None,
    since_trades: Optional[int] = None,
) -> tuple[Optional[int], Optional[int]]:
    safe_since_days = None if since_days is None else max(1, int(since_days))
    safe_since_trades = None if since_trades is None else max(1, int(since_trades))
    return safe_since_days, safe_since_trades


def _apply_vp_reason_time_window_filters(
    rows: list[Dict[str, Any]],
    *,
    since_days: Optional[int] = None,
    since_trades: Optional[int] = None,
) -> list[Dict[str, Any]]:
    safe_since_days, safe_since_trades = _parse_vp_reason_time_window_filters(
        since_days=since_days,
        since_trades=since_trades,
    )
    filtered_rows = rows

    if safe_since_days is not None and filtered_rows:
        timestamps = [
            parsed_timestamp
            for parsed_timestamp in (_to_utc_datetime(row.get("evaluated_at")) for row in rows)
            if parsed_timestamp is not None
        ]
        if timestamps:
            latest_timestamp = max(timestamps)
            cutoff_timestamp = latest_timestamp - timedelta(days=safe_since_days)
            filtered_rows = [
                row
                for row in filtered_rows
                if (row_ts := _to_utc_datetime(row.get("evaluated_at"))) is not None
                and row_ts >= cutoff_timestamp
            ]
        else:
            filtered_rows = []

    if safe_since_trades is not None:
        filtered_rows = filtered_rows[:safe_since_trades]

    return filtered_rows


def _get_outcome_cohort_rows(
    *,
    group_by: str,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    min_samples: int = 1,
) -> list[Dict[str, Any]]:
    rows = _get_labeled_execution_outcome_rows_with_context(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        label=label,
        limit=None,
        max_limit=5000,
    )

    return _build_outcome_cohort_rows_from_labeled_rows(
        rows,
        group_by=group_by,
        min_samples=min_samples,
    )


def _build_outcome_cohort_rows_from_labeled_rows(
    rows: list[Dict[str, Any]],
    *,
    group_by: str,
    min_samples: int = 1,
) -> list[Dict[str, Any]]:
    normalized_group_by = _normalize_outcome_group_by(group_by)
    safe_min_samples = max(1, int(min_samples))

    buckets: dict[str, list[Dict[str, Any]]] = {}
    for row in rows:
        cohort_key = _get_cohort_key_for_row(row, group_by=normalized_group_by)
        buckets.setdefault(cohort_key, []).append(row)

    cohort_rows: list[Dict[str, Any]] = []
    for cohort_key, cohort_bucket_rows in buckets.items():
        metrics = _collect_labeled_outcome_metrics(cohort_bucket_rows)
        if metrics["total"] < safe_min_samples:
            continue

        cohort_rows.append(
            {
                "cohort_key": cohort_key,
                "total": metrics["total"],
                "evaluated_count": metrics["evaluated_count"],
                "winner_count": metrics["winner_count"],
                "loser_count": metrics["loser_count"],
                "scratch_count": metrics["scratch_count"],
                "unknown_count": metrics["unknown_count"],
                "win_rate": metrics["win_rate"],
                "loss_rate": metrics["loss_rate"],
                "avg_pnl_points": metrics["avg_pnl_points"],
                "avg_pnl_pct": metrics["avg_pnl_pct"],
                "expectancy_points": metrics["expectancy_points"],
                "expectancy_pct": metrics["expectancy_pct"],
                "best_pnl_points": metrics["best_pnl_points"],
                "worst_pnl_points": metrics["worst_pnl_points"],
                "latest_evaluated_at": metrics["latest_evaluated_at"],
            }
        )

    return cohort_rows


def _calculate_policy_ranking_score(
    cohort_row: Dict[str, Any],
    *,
    scoring_mode: str,
    min_samples: int,
) -> float:
    normalized_mode = _normalize_policy_scoring_mode(scoring_mode)
    safe_min_samples = max(1, int(min_samples))

    if normalized_mode == "expectancy_pct":
        return _safe_numeric_for_sort(cohort_row.get("expectancy_pct"))
    if normalized_mode == "expectancy_points":
        return _safe_numeric_for_sort(cohort_row.get("expectancy_points"))
    if normalized_mode == "win_rate":
        return _safe_numeric_for_sort(cohort_row.get("win_rate"))
    if normalized_mode == "avg_pnl_pct":
        return _safe_numeric_for_sort(cohort_row.get("avg_pnl_pct"))

    expectancy_pct = _safe_numeric_for_sort(cohort_row.get("expectancy_pct"), fallback=0.0)
    win_rate = _safe_numeric_for_sort(cohort_row.get("win_rate"), fallback=0.0)
    total = int(cohort_row.get("total") or 0)
    sample_weight = total / (total + safe_min_samples)
    return ((0.7 * expectancy_pct) + (0.3 * win_rate)) * sample_weight


def _rank_outcome_cohort_rows(
    cohort_rows: list[Dict[str, Any]],
    *,
    scoring_mode: str,
    min_samples: int,
) -> list[Dict[str, Any]]:
    normalized_mode = _normalize_policy_scoring_mode(scoring_mode)

    ranked_rows: list[Dict[str, Any]] = []
    for cohort_row in cohort_rows:
        row_with_score = dict(cohort_row)
        row_with_score["ranking_score"] = _calculate_policy_ranking_score(
            cohort_row,
            scoring_mode=normalized_mode,
            min_samples=min_samples,
        )
        ranked_rows.append(row_with_score)

    ranked_rows.sort(
        key=lambda item: (
            -_safe_numeric_for_sort(item.get("ranking_score")),
            -_safe_numeric_for_sort(item.get("expectancy_pct")),
            -_safe_numeric_for_sort(item.get("win_rate"), fallback=0.0),
            -int(item.get("total") or 0),
            str(item.get("cohort_key") or ""),
        )
    )

    return ranked_rows


def _get_labeled_execution_outcome_rows(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    limit: Optional[int] = None,
    max_limit: int = 5000,
) -> list[Dict[str, Any]]:
    normalized_label = _normalize_outcome_label_filter(label)
    safe_limit = None if limit is None else max(1, min(int(limit), max_limit))

    # If label filtering is requested with a limit, filter first then cap to preserve limit semantics.
    fetch_limit = None if (normalized_label is not None and safe_limit is not None) else safe_limit

    rows = _get_execution_outcome_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        limit=fetch_limit,
    )

    labeled_rows = _with_execution_outcome_labels(rows)

    if normalized_label is not None:
        labeled_rows = [row for row in labeled_rows if row.get("label") == normalized_label]

    if safe_limit is not None:
        labeled_rows = labeled_rows[:safe_limit]

    return labeled_rows


def _get_labeled_execution_outcome_rows_with_context(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    limit: Optional[int] = None,
    max_limit: int = 5000,
    order_by_ascending: bool = False,
) -> list[Dict[str, Any]]:
    normalized_label = _normalize_outcome_label_filter(label)
    safe_limit = None if limit is None else max(1, min(int(limit), max_limit))
    fetch_limit = None if (normalized_label is not None and safe_limit is not None) else safe_limit

    rows = _get_enriched_execution_outcome_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        limit=fetch_limit,
        order_by_ascending=order_by_ascending,
    )

    labeled_rows = _with_execution_outcome_labels(rows)

    if normalized_label is not None:
        labeled_rows = [row for row in labeled_rows if row.get("label") == normalized_label]

    if safe_limit is not None:
        labeled_rows = labeled_rows[:safe_limit]

    return labeled_rows


def get_recent_execution_outcomes(
    limit: int,
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
) -> list[Dict[str, Any]]:
    return _get_labeled_execution_outcome_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        label=label,
        limit=limit,
        max_limit=500,
    )


def get_execution_outcomes_scorecard(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    rows = _get_labeled_execution_outcome_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        label=label,
        limit=limit,
        max_limit=5000,
    )

    metrics = _collect_labeled_outcome_metrics(rows)

    by_symbol: dict[str, int] = {}
    by_direction: dict[str, int] = {}

    for row in rows:
        row_symbol = row.get("symbol")
        if row_symbol:
            by_symbol[str(row_symbol)] = by_symbol.get(str(row_symbol), 0) + 1

        row_direction = row.get("direction")
        if row_direction:
            by_direction[str(row_direction)] = by_direction.get(str(row_direction), 0) + 1

    win_threshold, loss_threshold = get_outcome_label_thresholds()

    return {
        "total": metrics["total"],
        "evaluated_count": metrics["evaluated_count"],
        "labeled_count": metrics["labeled_count"],
        "winner_count": metrics["winner_count"],
        "loser_count": metrics["loser_count"],
        "scratch_count": metrics["scratch_count"],
        "unknown_count": metrics["unknown_count"],
        "win_rate": metrics["win_rate"],
        "loss_rate": metrics["loss_rate"],
        "scratch_rate": metrics["scratch_rate"],
        "avg_pnl_points": metrics["avg_pnl_points"],
        "avg_pnl_pct": metrics["avg_pnl_pct"],
        "expectancy_points": metrics["expectancy_points"],
        "expectancy_pct": metrics["expectancy_pct"],
        "best_pnl_points": metrics["best_pnl_points"],
        "worst_pnl_points": metrics["worst_pnl_points"],
        "by_symbol": dict(sorted(by_symbol.items())),
        "by_direction": dict(sorted(by_direction.items())),
        "latest_evaluated_at": metrics["latest_evaluated_at"],
        "win_threshold_pct": win_threshold,
        "loss_threshold_pct": loss_threshold,
    }


def get_execution_outcomes_leaderboard(
    *,
    group_by: str,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    min_samples: int = 1,
    limit: int = 50,
) -> list[Dict[str, Any]]:
    _normalize_outcome_group_by(group_by)
    safe_limit = max(1, min(int(limit), 500))

    leaderboard_rows = _get_outcome_cohort_rows(
        group_by=group_by,
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        label=label,
        min_samples=min_samples,
    )

    leaderboard_rows.sort(
        key=lambda item: (
            -_safe_numeric_for_sort(item.get("expectancy_pct")),
            -_safe_numeric_for_sort(item.get("win_rate"), fallback=0.0),
            -_safe_numeric_for_sort(item.get("avg_pnl_pct")),
            -int(item.get("total") or 0),
            str(item.get("cohort_key") or ""),
        )
    )

    return leaderboard_rows[:safe_limit]


def get_execution_outcomes_policy_recommendation(
    *,
    group_by: str = "strategy",
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    min_samples: int = 1,
    top_n: int = 1,
    scoring_mode: str = "blended",
) -> Dict[str, Any]:
    normalized_group_by = _normalize_outcome_group_by(group_by)
    normalized_scoring_mode = _normalize_policy_scoring_mode(scoring_mode)
    safe_min_samples = max(1, int(min_samples))
    safe_top_n = max(1, min(int(top_n), 500))

    cohort_rows = _get_outcome_cohort_rows(
        group_by=normalized_group_by,
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        since=since,
        until=until,
        label=label,
        min_samples=safe_min_samples,
    )

    ranked_rows = _rank_outcome_cohort_rows(
        cohort_rows,
        scoring_mode=normalized_scoring_mode,
        min_samples=safe_min_samples,
    )
    selected_rows = ranked_rows[:safe_top_n]

    if selected_rows:
        recommendation_summary = (
            f"Selected {len(selected_rows)} {normalized_group_by} cohort(s) "
            f"using {normalized_scoring_mode}; top cohort is {selected_rows[0]['cohort_key']}."
        )
    else:
        recommendation_summary = (
            f"No eligible {normalized_group_by} cohorts matched filters and min_samples={safe_min_samples}."
        )

    applied_filters = {
        "group_by": normalized_group_by,
        "since": since,
        "until": until,
        "symbol": symbol,
        "worker_id": worker_id,
        "direction": direction,
        "outcome_status": outcome_status,
        "signal_id": signal_id,
        "label": _normalize_outcome_label_filter(label),
        "min_samples": safe_min_samples,
        "top_n": safe_top_n,
        "scoring_mode": normalized_scoring_mode,
    }

    return {
        "group_by": normalized_group_by,
        "scoring_mode": normalized_scoring_mode,
        "selected_count": len(selected_rows),
        "rows": selected_rows,
        "recommendation_summary": recommendation_summary,
        "applied_filters": applied_filters,
    }


def get_execution_outcomes_policy_matrix(
    *,
    groupings: tuple[str, ...] | list[str] | None = None,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    min_samples: int = 1,
    top_n_per_group: int = 2,
    scoring_mode: str = "blended",
) -> Dict[str, list[Dict[str, Any]]]:
    normalized_groupings = _normalize_policy_matrix_groupings(groupings)
    safe_top_n = max(1, min(int(top_n_per_group), 100))

    matrix: Dict[str, list[Dict[str, Any]]] = {}
    for group_by in normalized_groupings:
        recommendation = get_execution_outcomes_policy_recommendation(
            group_by=group_by,
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            outcome_status=outcome_status,
            since=since,
            until=until,
            label=label,
            min_samples=min_samples,
            top_n=safe_top_n,
            scoring_mode=scoring_mode,
        )
        matrix[group_by] = recommendation["rows"]

    return matrix


def _average_audit_field(rows: list[Dict[str, Any]], field_name: str) -> Optional[float]:
    values = [
        numeric
        for numeric in (_to_optional_float(row.get(field_name)) for row in rows)
        if numeric is not None
    ]
    return _average(values)


def _collect_policy_audit_summary(rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    recommendation_values = [
        numeric
        for numeric in (_to_optional_float(row.get("forward_expectancy_points")) for row in rows)
        if numeric is not None
    ]

    recommendation_hit_rate: Optional[float]
    if recommendation_values:
        hit_count = sum(1 for value in recommendation_values if value > 0)
        recommendation_hit_rate = hit_count / len(recommendation_values)
    else:
        recommendation_hit_rate = None

    return {
        "total_steps": len(rows),
        "avg_forward_pnl_points": _average_audit_field(rows, "forward_avg_pnl_points"),
        "avg_forward_pnl_pct": _average_audit_field(rows, "forward_avg_pnl_pct"),
        "avg_forward_win_rate": _average_audit_field(rows, "forward_win_rate"),
        "avg_forward_expectancy_points": _average_audit_field(rows, "forward_expectancy_points"),
        "avg_forward_expectancy_pct": _average_audit_field(rows, "forward_expectancy_pct"),
        "recommendation_hit_rate": recommendation_hit_rate,
    }


def _calculate_execution_outcomes_policy_audit(
    *,
    group_by: str = "strategy",
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    min_samples: int = 1,
    audit_step_size: int = 1,
    audit_horizon_samples: int = 10,
    top_n: int = 1,
    scoring_mode: str = "blended",
) -> Dict[str, Any]:
    normalized_group_by = _normalize_outcome_group_by(group_by)
    normalized_scoring_mode = _normalize_policy_scoring_mode(scoring_mode)
    normalized_label = _normalize_outcome_label_filter(label)

    safe_min_samples = max(1, int(min_samples))
    safe_step_size = max(1, min(int(audit_step_size), 5000))
    safe_horizon = max(1, min(int(audit_horizon_samples), 5000))
    safe_top_n = max(1, min(int(top_n), 500))

    chronological_rows = _get_labeled_execution_outcome_rows_with_context(
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        since=since,
        until=until,
        label=normalized_label,
        limit=None,
        max_limit=5000,
        order_by_ascending=True,
    )

    audit_rows: list[Dict[str, Any]] = []
    total_rows = len(chronological_rows)
    history_end = safe_min_samples

    while history_end < total_rows:
        history_rows = chronological_rows[:history_end]
        forward_slice = chronological_rows[history_end : history_end + safe_horizon]
        if not forward_slice:
            break

        historical_cohorts = _build_outcome_cohort_rows_from_labeled_rows(
            history_rows,
            group_by=normalized_group_by,
            min_samples=safe_min_samples,
        )
        ranked_historical = _rank_outcome_cohort_rows(
            historical_cohorts,
            scoring_mode=normalized_scoring_mode,
            min_samples=safe_min_samples,
        )

        selected_rows = ranked_historical[:safe_top_n]
        if not selected_rows:
            history_end += safe_step_size
            continue

        selected_row = selected_rows[0]
        selected_key = str(selected_row.get("cohort_key") or "unknown")

        forward_cohort_rows = [
            row
            for row in forward_slice
            if _get_cohort_key_for_row(row, group_by=normalized_group_by) == selected_key
        ]
        forward_metrics = _collect_labeled_outcome_metrics(forward_cohort_rows)
        forward_sample_count = len(forward_cohort_rows)

        if forward_sample_count == 0:
            forward_avg_pnl_points = None
            forward_avg_pnl_pct = None
            forward_win_rate = None
            forward_expectancy_points = None
            forward_expectancy_pct = None
        else:
            forward_avg_pnl_points = forward_metrics.get("avg_pnl_points")
            forward_avg_pnl_pct = forward_metrics.get("avg_pnl_pct")
            forward_win_rate = forward_metrics.get("win_rate")
            forward_expectancy_points = forward_metrics.get("expectancy_points")
            forward_expectancy_pct = forward_metrics.get("expectancy_pct")

        audit_rows.append(
            {
                "audit_cutoff": history_rows[-1].get("evaluated_at"),
                "recommended_cohort": selected_key,
                "ranking_score": selected_row.get("ranking_score"),
                "historical_sample_count": int(selected_row.get("total") or 0),
                "forward_sample_count": forward_sample_count,
                "forward_avg_pnl_points": forward_avg_pnl_points,
                "forward_avg_pnl_pct": forward_avg_pnl_pct,
                "forward_win_rate": forward_win_rate,
                "forward_expectancy_points": forward_expectancy_points,
                "forward_expectancy_pct": forward_expectancy_pct,
            }
        )

        history_end += safe_step_size

    summary = _collect_policy_audit_summary(audit_rows)
    applied_filters = {
        "group_by": normalized_group_by,
        "since": since,
        "until": until,
        "symbol": symbol,
        "direction": direction,
        "outcome_status": outcome_status,
        "label": normalized_label,
        "min_samples": safe_min_samples,
        "audit_step_size": safe_step_size,
        "audit_horizon_samples": safe_horizon,
        "top_n": safe_top_n,
        "scoring_mode": normalized_scoring_mode,
    }

    return {
        "group_by": normalized_group_by,
        "scoring_mode": normalized_scoring_mode,
        "audit_steps": len(audit_rows),
        "rows": audit_rows,
        "summary": summary,
        "applied_filters": applied_filters,
    }


def get_execution_outcomes_policy_audit(
    *,
    group_by: str = "strategy",
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    min_samples: int = 1,
    audit_step_size: int = 1,
    audit_horizon_samples: int = 10,
    top_n: int = 1,
    scoring_mode: str = "blended",
) -> Dict[str, Any]:
    return _calculate_execution_outcomes_policy_audit(
        group_by=group_by,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        since=since,
        until=until,
        label=label,
        min_samples=min_samples,
        audit_step_size=audit_step_size,
        audit_horizon_samples=audit_horizon_samples,
        top_n=top_n,
        scoring_mode=scoring_mode,
    )


def get_execution_outcomes_policy_audit_summary(
    *,
    group_by: str = "strategy",
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
    min_samples: int = 1,
    audit_step_size: int = 1,
    audit_horizon_samples: int = 10,
    top_n: int = 1,
    scoring_mode: str = "blended",
) -> Dict[str, Any]:
    audit_result = _calculate_execution_outcomes_policy_audit(
        group_by=group_by,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        since=since,
        until=until,
        label=label,
        min_samples=min_samples,
        audit_step_size=audit_step_size,
        audit_horizon_samples=audit_horizon_samples,
        top_n=top_n,
        scoring_mode=scoring_mode,
    )

    return {
        "group_by": audit_result["group_by"],
        "scoring_mode": audit_result["scoring_mode"],
        "audit_steps": audit_result["audit_steps"],
        "summary": audit_result["summary"],
        "applied_filters": audit_result["applied_filters"],
    }


def get_execution_outcomes_compare(
    *,
    left_group_by: str,
    left_value: str,
    right_group_by: str,
    right_value: str,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    label: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_left_group_by = _normalize_outcome_group_by(left_group_by)
    normalized_right_group_by = _normalize_outcome_group_by(right_group_by)

    left_key = left_value.strip()
    right_key = right_value.strip()
    if not left_key or not right_key:
        raise ValueError("left_value and right_value must not be empty")

    rows = _get_labeled_execution_outcome_rows_with_context(
        worker_id=None,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=None,
        since=since,
        until=until,
        label=label,
        limit=None,
        max_limit=5000,
    )

    left_rows = [
        row
        for row in rows
        if _get_cohort_key_for_row(row, group_by=normalized_left_group_by) == left_key
    ]
    right_rows = [
        row
        for row in rows
        if _get_cohort_key_for_row(row, group_by=normalized_right_group_by) == right_key
    ]

    left_metrics = _collect_labeled_outcome_metrics(left_rows)
    right_metrics = _collect_labeled_outcome_metrics(right_rows)

    left_payload = {
        "group_by": normalized_left_group_by,
        "cohort_key": left_key,
        **left_metrics,
    }
    right_payload = {
        "group_by": normalized_right_group_by,
        "cohort_key": right_key,
        **right_metrics,
    }

    deltas = {
        "delta_win_rate": _metric_delta(left_metrics.get("win_rate"), right_metrics.get("win_rate")),
        "delta_avg_pnl_points": _metric_delta(left_metrics.get("avg_pnl_points"), right_metrics.get("avg_pnl_points")),
        "delta_avg_pnl_pct": _metric_delta(left_metrics.get("avg_pnl_pct"), right_metrics.get("avg_pnl_pct")),
        "delta_expectancy_points": _metric_delta(
            left_metrics.get("expectancy_points"),
            right_metrics.get("expectancy_points"),
        ),
        "delta_expectancy_pct": _metric_delta(
            left_metrics.get("expectancy_pct"),
            right_metrics.get("expectancy_pct"),
        ),
    }

    return {
        "left": left_payload,
        "right": right_payload,
        "deltas": deltas,
    }


def get_execution_outcomes_summary(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
) -> Dict[str, Any]:
    rows = _get_execution_outcome_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        limit=None,
    )

    total = len(rows)
    evaluated_count = sum(1 for row in rows if row.get("outcome_status") == "evaluated")
    insufficient_data_count = sum(1 for row in rows if row.get("outcome_status") == "insufficient_data")

    pnl_points_values = [
        float(row["pnl_points"])
        for row in rows
        if row.get("pnl_points") is not None
    ]
    pnl_pct_values = [
        float(row["pnl_pct"])
        for row in rows
        if row.get("pnl_pct") is not None
    ]

    wins = sum(
        1
        for row in rows
        if row.get("pnl_points") is not None and float(row["pnl_points"]) > 0
    )
    win_base = sum(1 for row in rows if row.get("pnl_points") is not None)
    win_rate = (wins / win_base) if win_base > 0 else 0.0

    by_symbol: dict[str, int] = {}
    for row in rows:
        row_symbol = row.get("symbol")
        if row_symbol:
            by_symbol[str(row_symbol)] = by_symbol.get(str(row_symbol), 0) + 1

    latest_evaluated_at = rows[0].get("evaluated_at") if rows else None

    return {
        "total": total,
        "evaluated_count": evaluated_count,
        "insufficient_data_count": insufficient_data_count,
        "avg_pnl_points": _average(pnl_points_values),
        "avg_pnl_pct": _average(pnl_pct_values),
        "win_rate": win_rate,
        "by_symbol": dict(sorted(by_symbol.items())),
        "latest_evaluated_at": latest_evaluated_at,
    }


def _parse_json_object(raw_value: Any) -> Dict[str, Any]:
    try:
        parsed = json.loads(raw_value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}

    return parsed if isinstance(parsed, dict) else {}


def _is_vp_policy_candidate(value: Any) -> bool:
    try:
        return int(value or 0) == 1
    except (TypeError, ValueError):
        return False


def _normalize_vp_policy_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    return side if side in {"long", "short"} else "unknown"


def _is_vp_policy_direction_correct(*, row_direction: Any, vp_policy_side: str, pnl_points: Any) -> Optional[bool]:
    normalized_direction = str(row_direction or "").strip().lower()
    numeric_pnl = _to_optional_float(pnl_points)
    if vp_policy_side not in {"long", "short"}:
        return None
    if normalized_direction not in {"long", "short"}:
        return None
    if numeric_pnl is None:
        return None
    if normalized_direction == vp_policy_side:
        return numeric_pnl > 0
    return numeric_pnl < 0


def get_execution_outcomes_vp_policy_summary(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
) -> Dict[str, Any]:
    rows = _get_execution_outcome_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        limit=None,
    )

    candidate_rows = 0
    long_candidate_rows = 0
    short_candidate_rows = 0
    score_values: list[float] = []

    for row in rows:
        metadata = _parse_json_object(row.get("metadata_json"))

        score = _to_optional_float(metadata.get("vp_trade_bias_score"))
        if score is not None:
            score_values.append(score)

        if not _is_vp_policy_candidate(metadata.get("vp_policy_candidate")):
            continue

        candidate_rows += 1
        side = str(metadata.get("vp_policy_side") or "").strip().lower()
        if side == "long":
            long_candidate_rows += 1
        elif side == "short":
            short_candidate_rows += 1

    return {
        "total_rows": len(rows),
        "candidate_rows": candidate_rows,
        "long_candidate_rows": long_candidate_rows,
        "short_candidate_rows": short_candidate_rows,
        "avg_vp_trade_bias_score": _average(score_values),
    }


def get_execution_outcomes_vp_policy_cohorts(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
) -> list[Dict[str, Any]]:
    rows = _get_execution_outcome_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        limit=None,
    )

    buckets: dict[tuple[str, Optional[float]], list[Dict[str, Any]]] = {}
    for row in rows:
        metadata = _parse_json_object(row.get("metadata_json"))
        cohort_side = _normalize_vp_policy_side(metadata.get("vp_policy_side"))
        cohort_score = _to_optional_float(metadata.get("vp_trade_bias_score"))
        buckets.setdefault((cohort_side, cohort_score), []).append(row)

    cohort_rows: list[Dict[str, Any]] = []
    for (cohort_side, cohort_score), cohort_bucket_rows in buckets.items():
        pnl_values = [
            numeric
            for numeric in (_to_optional_float(row.get("pnl_points")) for row in cohort_bucket_rows)
            if numeric is not None
        ]
        mfe_values = [
            numeric
            for numeric in (_to_optional_float(row.get("max_favorable_excursion")) for row in cohort_bucket_rows)
            if numeric is not None
        ]
        mae_values = [
            numeric
            for numeric in (_to_optional_float(row.get("max_adverse_excursion")) for row in cohort_bucket_rows)
            if numeric is not None
        ]
        direction_correct_values = [
            numeric
            for numeric in (
                _is_vp_policy_direction_correct(
                    row_direction=row.get("direction"),
                    vp_policy_side=cohort_side,
                    pnl_points=row.get("pnl_points"),
                )
                for row in cohort_bucket_rows
            )
            if numeric is not None
        ]

        cohort_rows.append(
            {
                "vp_policy_side": cohort_side,
                "vp_trade_bias_score": cohort_score,
                "row_count": len(cohort_bucket_rows),
                "avg_pnl": _average(pnl_values),
                "avg_mfe": _average(mfe_values),
                "avg_mae": _average(mae_values),
                "direction_correct_rate": _direction_correct_rate(direction_correct_values),
            }
        )

    cohort_rows.sort(
        key=lambda row: (
            str(row.get("vp_policy_side") or ""),
            -_safe_numeric_for_sort(row.get("vp_trade_bias_score"), fallback=-1e18),
        )
    )

    return cohort_rows


def _direction_correct_rate(values: list[Any]) -> Optional[float]:
    if not values:
        return None
    return sum(1 for v in values if v) / len(values)


def get_execution_outcomes_vp_policy_reason_cohorts(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    vp_trade_bias_score: Optional[float] = None,
    vp_policy_side: Optional[str] = None,
    since_days: Optional[int] = None,
    since_trades: Optional[int] = None,
) -> list[Dict[str, Any]]:
    rows = _get_execution_outcome_filtered_rows(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        limit=None,
    )
    rows = _apply_vp_reason_time_window_filters(
        rows,
        since_days=since_days,
        since_trades=since_trades,
    )

    requested_score = _to_optional_float(vp_trade_bias_score)
    requested_side = None if vp_policy_side is None else _normalize_vp_policy_side(vp_policy_side)

    buckets: dict[str, list[Dict[str, Any]]] = {}
    for row in rows:
        metadata = _parse_json_object(row.get("metadata_json"))
        row_score = _to_optional_float(metadata.get("vp_trade_bias_score"))
        row_side = _normalize_vp_policy_side(metadata.get("vp_policy_side"))
        if requested_score is not None and row_score != requested_score:
            continue
        if requested_side is not None and row_side != requested_side:
            continue
        cohort_reason = str(metadata.get("vp_policy_reason") or "").strip() or "unknown"
        buckets.setdefault(cohort_reason, []).append(row)

    cohort_rows: list[Dict[str, Any]] = []
    for cohort_reason, cohort_bucket_rows in buckets.items():
        row_count = len(cohort_bucket_rows)
        pnl_values = [
            numeric
            for numeric in (_to_optional_float(row.get("pnl_points")) for row in cohort_bucket_rows)
            if numeric is not None
        ]
        mfe_values = [
            numeric
            for numeric in (_to_optional_float(row.get("max_favorable_excursion")) for row in cohort_bucket_rows)
            if numeric is not None
        ]
        mae_values = [
            numeric
            for numeric in (_to_optional_float(row.get("max_adverse_excursion")) for row in cohort_bucket_rows)
            if numeric is not None
        ]
        direction_correct_values = [
            numeric
            for numeric in (
                _is_vp_policy_direction_correct(
                    row_direction=row.get("direction"),
                    vp_policy_side=_normalize_vp_policy_side(
                        _parse_json_object(row.get("metadata_json")).get("vp_policy_side")
                    ),
                    pnl_points=row.get("pnl_points"),
                )
                for row in cohort_bucket_rows
            )
            if numeric is not None
        ]

        avg_pnl = _average(pnl_values)
        stdev_pnl = _sample_standard_deviation(pnl_values) if row_count >= 2 else None
        pnl_ci_low, pnl_ci_high = _mean_confidence_interval_95(
            mean=avg_pnl,
            stdev=stdev_pnl,
            sample_count=row_count,
        )

        cohort_rows.append(
            {
                "vp_policy_reason": cohort_reason,
                "row_count": row_count,
                "avg_pnl": avg_pnl,
                "stdev_pnl": stdev_pnl,
                "pnl_ci_low": pnl_ci_low,
                "pnl_ci_high": pnl_ci_high,
                "avg_mfe": _average(mfe_values),
                "avg_mae": _average(mae_values),
                "direction_correct_rate": _direction_correct_rate(direction_correct_values),
            }
        )

    cohort_rows.sort(key=lambda row: str(row.get("vp_policy_reason") or ""))

    return cohort_rows


def _get_vp_policy_reason_quality_score(cohort_row: Dict[str, Any]) -> float:
    return (
        _safe_numeric_for_sort(cohort_row.get("avg_pnl"), fallback=0.0)
        * _safe_numeric_for_sort(cohort_row.get("direction_correct_rate"), fallback=0.0)
    )


def _shape_vp_policy_reason_rank_row(cohort_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "vp_policy_reason": cohort_row.get("vp_policy_reason"),
        "row_count": cohort_row.get("row_count"),
        "avg_pnl": cohort_row.get("avg_pnl"),
        "stdev_pnl": cohort_row.get("stdev_pnl"),
        "pnl_ci_low": cohort_row.get("pnl_ci_low"),
        "pnl_ci_high": cohort_row.get("pnl_ci_high"),
        "direction_correct_rate": cohort_row.get("direction_correct_rate"),
        "quality_score": _get_vp_policy_reason_quality_score(cohort_row),
    }


def get_execution_outcomes_vp_policy_reason_leaderboard(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    vp_trade_bias_score: Optional[float] = None,
    vp_policy_side: Optional[str] = None,
    since_days: Optional[int] = None,
    since_trades: Optional[int] = None,
    min_count: int = 2,
    limit: Optional[int] = None,
    sort: Optional[str] = "pnl",
) -> list[Dict[str, Any]]:
    safe_min_count = max(1, int(min_count))
    safe_limit = None if limit is None else max(1, int(limit))
    sort_mode = sort if sort in {"accuracy", "quality"} else "pnl"
    cohort_rows = get_execution_outcomes_vp_policy_reason_cohorts(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        vp_trade_bias_score=vp_trade_bias_score,
        vp_policy_side=vp_policy_side,
        since_days=since_days,
        since_trades=since_trades,
    )

    leaderboard_rows = [row for row in cohort_rows if int(row.get("row_count") or 0) >= safe_min_count]
    if sort_mode == "accuracy":
        leaderboard_rows.sort(
            key=lambda row: (
                -_safe_numeric_for_sort(row.get("direction_correct_rate"), fallback=0.0),
                -_safe_numeric_for_sort(row.get("avg_pnl")),
                str(row.get("vp_policy_reason") or ""),
            )
        )
    elif sort_mode == "quality":
        leaderboard_rows.sort(
            key=lambda row: (
                -_get_vp_policy_reason_quality_score(row),
                -_safe_numeric_for_sort(row.get("avg_pnl")),
                -_safe_numeric_for_sort(row.get("direction_correct_rate"), fallback=0.0),
                str(row.get("vp_policy_reason") or ""),
            )
        )
    else:
        leaderboard_rows.sort(
            key=lambda row: (
                -_safe_numeric_for_sort(row.get("avg_pnl")),
                -_safe_numeric_for_sort(row.get("direction_correct_rate"), fallback=0.0),
                str(row.get("vp_policy_reason") or ""),
            )
        )
    if safe_limit is not None:
        leaderboard_rows = leaderboard_rows[:safe_limit]

    return [_shape_vp_policy_reason_rank_row(row) for row in leaderboard_rows]


def get_execution_outcomes_vp_policy_reason_laggards(
    *,
    worker_id: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    outcome_status: Optional[str] = None,
    signal_id: Optional[str] = None,
    vp_trade_bias_score: Optional[float] = None,
    vp_policy_side: Optional[str] = None,
    since_days: Optional[int] = None,
    since_trades: Optional[int] = None,
    min_count: int = 2,
    limit: Optional[int] = None,
    sort: Optional[str] = "pnl",
) -> list[Dict[str, Any]]:
    safe_min_count = max(1, int(min_count))
    safe_limit = None if limit is None else max(1, int(limit))
    sort_mode = sort if sort in {"accuracy", "quality"} else "pnl"
    cohort_rows = get_execution_outcomes_vp_policy_reason_cohorts(
        worker_id=worker_id,
        symbol=symbol,
        direction=direction,
        outcome_status=outcome_status,
        signal_id=signal_id,
        vp_trade_bias_score=vp_trade_bias_score,
        vp_policy_side=vp_policy_side,
        since_days=since_days,
        since_trades=since_trades,
    )

    laggard_rows = [row for row in cohort_rows if int(row.get("row_count") or 0) >= safe_min_count]
    if sort_mode == "accuracy":
        laggard_rows.sort(
            key=lambda row: (
                _safe_numeric_for_sort(row.get("direction_correct_rate"), fallback=0.0),
                _safe_numeric_for_sort(row.get("avg_pnl")),
                str(row.get("vp_policy_reason") or ""),
            )
        )
    elif sort_mode == "quality":
        laggard_rows.sort(
            key=lambda row: (
                _get_vp_policy_reason_quality_score(row),
                _safe_numeric_for_sort(row.get("avg_pnl")),
                _safe_numeric_for_sort(row.get("direction_correct_rate"), fallback=0.0),
                str(row.get("vp_policy_reason") or ""),
            )
        )
    else:
        laggard_rows.sort(
            key=lambda row: (
                _safe_numeric_for_sort(row.get("avg_pnl")),
                _safe_numeric_for_sort(row.get("direction_correct_rate"), fallback=0.0),
                str(row.get("vp_policy_reason") or ""),
            )
        )
    if safe_limit is not None:
        laggard_rows = laggard_rows[:safe_limit]

    return [_shape_vp_policy_reason_rank_row(row) for row in laggard_rows]


def get_recent_trade_candidates(
    limit: int,
    *,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    direction: Optional[str] = None,
    strategy: Optional[str] = None,
    source: Optional[str] = None,
    event_type: Optional[str] = None,
    signal_id: Optional[str] = None,
    derived_from_event: Optional[bool] = None,
    execution_status: Optional[str] = None,
) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))

    where_clauses: list[str] = []
    params: list[Any] = []

    if symbol is not None:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)

    if direction is not None:
        where_clauses.append("direction = ?")
        params.append(direction)

    if signal_id is not None:
        where_clauses.append("signal_id = ?")
        params.append(signal_id)

    if execution_status is not None:
        where_clauses.append(_WC_EXECUTION_STATUS)
        params.append(execution_status)

    json_filters = {
        "timeframe": timeframe,
        "strategy": strategy,
        "source": source,
        "event_type": event_type,
    }
    for key, value in json_filters.items():
        if value is not None:
            where_clauses.append(f"json_valid(payload_json) AND json_extract(payload_json, '$.{key}') = ?")
            params.append(value)

    if derived_from_event is not None:
        where_clauses.append(
            "CASE "
            "WHEN json_valid(payload_json) "
            "THEN COALESCE(json_extract(payload_json, '$.derived_from_event'), 0) "
            "ELSE 0 END = ?"
        )
        params.append(1 if derived_from_event else 0)

    query = """
        SELECT *
        FROM trade_candidates
    """

    if where_clauses:
        query += _where_clause(where_clauses)

    query += " ORDER BY timestamp DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [dict(row) for row in rows]


def insert_volume_profile_snapshot(**kwargs) -> int:
    query = """
        INSERT INTO volume_profile_snapshots (
            timestamp, symbol, timeframe, engine_version,
            poc, vah, val, profile_high, profile_low,
            shape_label, balance_state, source_bar_count,
            profile_range, value_area_width, value_area_width_pct,
            poc_relative, poc_distance_from_mid,
            close_position_in_profile, distance_to_poc, distance_to_vah, distance_to_val,
            distance_to_poc_pct, distance_to_vah_pct, distance_to_val_pct,
            inside_value_area, above_vah, below_val
        ) VALUES (
            :timestamp, :symbol, :timeframe, :engine_version,
            :poc, :vah, :val, :profile_high, :profile_low,
            :shape_label, :balance_state, :source_bar_count,
            :profile_range, :value_area_width, :value_area_width_pct,
            :poc_relative, :poc_distance_from_mid,
            :close_position_in_profile, :distance_to_poc, :distance_to_vah, :distance_to_val,
            :distance_to_poc_pct, :distance_to_vah_pct, :distance_to_val_pct,
            :inside_value_area, :above_vah, :below_val
        )
    """
    # Defensive fallbacks for optional bounds before blindly inserting
    defaults = {
        "close_position_in_profile": None,
        "distance_to_poc": None,
        "distance_to_vah": None,
        "distance_to_val": None,
        "distance_to_poc_pct": None,
        "distance_to_vah_pct": None,
        "distance_to_val_pct": None,
        "inside_value_area": None,
        "above_vah": None,
        "below_val": None,
    }
    for k, v in defaults.items():
        if k not in kwargs:
            kwargs[k] = v

    with get_connection() as conn:
        cursor = conn.execute(query, kwargs)
        conn.commit()
        return _require_lastrowid(cursor)

def get_recent_volume_profile_snapshots(
    symbol: str | None = None,
    timeframe: str | None = None,
    limit: int = 100
) -> list[dict]:
    where_clauses = []
    params = []

    if symbol:
        where_clauses.append(_WC_SYMBOL)
        params.append(symbol)
        
    if timeframe:
        where_clauses.append(_WC_TIMEFRAME)
        params.append(timeframe)

    query = "SELECT * FROM volume_profile_snapshots"
    if where_clauses:
        query += _where_clause(where_clauses)
        
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(min(limit, 500))

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
