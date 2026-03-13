CREATE TABLE IF NOT EXISTS bar_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    long_score REAL,
    short_score REAL,
    no_trade_score REAL,
    setup_family TEXT,
    pressure_index REAL,
    volatility_state TEXT,
    participation_score REAL,
    confidence_seed REAL,
    payload_json TEXT
);

CREATE TABLE IF NOT EXISTS trade_candidates (
    -- Signals generated from webhook-driven strategy logic
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id TEXT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT,
    entry_price REAL,
    stop_price REAL,
    tp1 REAL,
    tp2 REAL,
    confidence REAL,
    setup_family TEXT,
    payload_json TEXT,
    execution_status TEXT DEFAULT 'pending',
    execution_note TEXT,
    executed_at TEXT
);

CREATE TABLE IF NOT EXISTS trade_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT,
    signal_id TEXT,
    event_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    pnl REAL,
    mfe REAL,
    mae REAL,
    exit_reason TEXT,
    payload_json TEXT
);

CREATE TABLE IF NOT EXISTS regime_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    regime_id TEXT,
    regime_confidence REAL,
    transition_risk REAL
);

CREATE TABLE IF NOT EXISTS model_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    long_probability REAL,
    short_probability REAL,
    no_trade_probability REAL,
    expected_excursion REAL,
    setup_trust_score REAL
);

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

CREATE INDEX IF NOT EXISTS idx_bar_states_timestamp
ON bar_states(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_bar_states_symbol_timeframe_timestamp
ON bar_states(symbol, timeframe, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_trade_candidates_timestamp
ON trade_candidates(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_symbol_timeframe_timestamp
ON feature_snapshots(symbol, timeframe, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_feature_snapshot_values_feature_key
ON feature_snapshot_values(feature_key);

CREATE INDEX IF NOT EXISTS idx_trade_candidates_symbol_timestamp
ON trade_candidates(symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_trade_candidates_signal_id
ON trade_candidates(signal_id);

CREATE INDEX IF NOT EXISTS idx_trade_candidates_execution_status_timestamp
ON trade_candidates(execution_status, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_execution_journal_created_at
ON execution_journal(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_journal_candidate_id
ON execution_journal(candidate_id);

CREATE INDEX IF NOT EXISTS idx_execution_journal_worker_created
ON execution_journal(worker_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_journal_status_created
ON execution_journal(execution_status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_outcomes_evaluated_at
ON execution_outcomes(evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_outcomes_symbol_evaluated
ON execution_outcomes(symbol, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_outcomes_worker_evaluated
ON execution_outcomes(worker_id, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_outcomes_status_evaluated
ON execution_outcomes(outcome_status, evaluated_at DESC);

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

CREATE TABLE IF NOT EXISTS volume_profile_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    poc REAL NOT NULL,
    vah REAL NOT NULL,
    val REAL NOT NULL,
    profile_high REAL NOT NULL,
    profile_low REAL NOT NULL,
    shape_label TEXT NOT NULL,
    balance_state TEXT NOT NULL,
    source_bar_count INTEGER NOT NULL,
    profile_range REAL NOT NULL DEFAULT 0.0,
    value_area_width REAL NOT NULL DEFAULT 0.0,
    value_area_width_pct REAL NOT NULL DEFAULT 0.0,
    poc_relative REAL NOT NULL DEFAULT 0.5,
    poc_distance_from_mid REAL NOT NULL DEFAULT 0.0,
    close_position_in_profile REAL,
    distance_to_poc REAL,
    distance_to_vah REAL,
    distance_to_val REAL,
    inside_value_area INTEGER,
    above_vah INTEGER,
    below_val INTEGER,
    distance_to_poc_pct REAL,
    distance_to_vah_pct REAL,
    distance_to_val_pct REAL
);

CREATE INDEX IF NOT EXISTS idx_vp_snapshots_symbol_timeframe_timestamp
ON volume_profile_snapshots(symbol, timeframe, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_vp_snapshots_timestamp
ON volume_profile_snapshots(timestamp DESC);


-- ─────────────────────────────────────────────────────────────────────
-- Release Cohort Scoring + Feature Lifecycle
-- ─────────────────────────────────────────────────────────────────────

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

