# System Blueprint

## Status
Formal build specification for moving from webhook analytics to a full dry-run trading machine with strong auditability.

Current repository baseline:
- FastAPI monolith entrypoint in backend/api_server.py
- SQLite persistence in database/schema.sql and backend/event_writer.py
- Existing analytics surfaces, including VP policy reason monitor

Target operating posture:
- end-to-end signal to decision to simulated execution
- no live broker routing until Alpha dry-run acceptance criteria pass

## System Identity
Single-chart adaptive orderflow-style trading system for BTCUSDT.P on 1m, built around four cooperating systems:
- Signal Intake System
- Decision System
- Execution System
- Audit and Learning System

## End-to-End Future Stack
```text
TradingView Pine Script
				-> TradingView Alert JSON
				-> FastAPI Webhook Intake
				-> Signal Normalizer
				-> Strategy and Policy Engine
				-> Risk Engine
				-> Execution Engine
				-> Broker Adapter
				-> Order and Fill and Position Sync
				-> SQL Storage
				-> Monitoring and Dashboard and Analytics
```

## Architectural Principles
- Pine is a sensor, not the control plane.
- Incoming alerts are immutable facts and must be stored raw.
- All downstream services operate on canonical normalized objects.
- Strategy decides intent, risk decides allowance, execution performs action.
- Every transition is persisted with IDs, timestamps, and reason codes.
- Live trading is blocked behind explicit go-live gates.

## Canonical Domain Objects

### RawWebhookEvent
- exact inbound payload from TradingView
- includes received_at, source_ip, signature headers, and parse status

### NormalizedSignal
- canonical internal representation after cleaning and mapping
- includes event_id, canonical side, canonical symbol, and derived features

### StrategyDecision
- strategy verdict for a normalized signal
- decision in approve/reject/defer/downgrade
- includes policy_version, confidence, and reason code

### RiskDecision
- risk verdict for a strategy-approved candidate
- risk_decision in approve/deny/resize
- includes limit checks, risk profile, and sizing outputs

### ExecutionRequest
- final request object passed to execution engine
- includes order intent, quantity, stop, take-profit, and correlation IDs

### BrokerOrder
- broker-native order acknowledgement and state transitions

### FillEvent
- one fill record per broker fill callback or sync poll result

### PositionState
- symbol-level position snapshot plus realized and unrealized metrics

### ExecutionOutcome
- post-trade or post-window result used for analytics and selector feedback

## JSON Message Contracts

### A. Raw Alert Payload Contract
Source of truth: what TradingView sends.

```json
{
	"source": "tradingview",
	"symbol": "BTCUSDT",
	"timeframe": "1m",
	"side": "long",
	"signal_name": "vp_breakout_long",
	"strategy_id": "smart_algo_v1",
	"score": 4,
	"bar_time": "2026-03-11T13:05:00Z",
	"price": 84250.5,
	"atr": 185.2,
	"volume_ratio": 1.42,
	"metadata": {
		"confluence": "strong",
		"session": "london_ny_overlap"
	}
}
```

Contract rules:
- preserve exact raw payload bytes or canonical json dump in storage
- reject malformed payloads with explicit validation errors
- generate server-side event_id and received_at

### B. Normalized Signal Payload Contract
Canonical internal truth object.

```json
{
	"event_id": "tv_20260311_000001",
	"source": "tradingview",
	"symbol": "BTCUSDT",
	"broker_symbol": "BTCUSDT",
	"timeframe": "1m",
	"side": "long",
	"signal_name": "vp_breakout_long",
	"strategy_id": "smart_algo_v1",
	"score": 4,
	"received_at": "2026-03-11T13:05:02Z",
	"bar_time": "2026-03-11T13:05:00Z",
	"market_price": 84250.5,
	"features": {
		"atr": 185.2,
		"volume_ratio": 1.42,
		"confluence": "strong"
	}
}
```

Normalizer requirements:
- side aliases normalize to long or short
- symbol mapped to canonical and broker forms
- numeric values coerced safely
- null and missing fields sanitized
- unknown fields retained in extensions object when needed

### C. Execution Request Payload Contract
Produced only when strategy and risk both permit.

```json
{
	"execution_request_id": "exec_req_20260311_000077",
	"event_id": "tv_20260311_000001",
	"symbol": "BTCUSDT",
	"side": "long",
	"order_type": "market",
	"quantity": 0.25,
	"time_in_force": "ioc",
	"risk_profile": "normal",
	"stop_loss_distance": 145.0,
	"take_profit_distance": 310.0,
	"decision_trace": {
		"strategy_decision": "approve",
		"risk_decision": "approve",
		"strategy_reason": "strong_confluence_with_healthy_reason_monitor"
	}
}
```

### D. Execution Result Payload Contract
Returned by simulated executor or broker adapter.

```json
{
	"execution_request_id": "exec_req_20260311_000077",
	"execution_status": "accepted",
	"mode": "simulated",
	"broker": "paper",
	"broker_order_id": "paper_ord_4491",
	"submitted_at": "2026-03-11T13:05:03Z",
	"fills": [],
	"error": null
}
```

## API Blueprint

### Existing intake baseline
- POST /webhook/tradingview
- stores bar state and triggers feature pipeline

### Target endpoint families

#### Webhook Intake
- POST /webhook/tradingview
- POST /webhooks/tradingview as alias route after compatibility check
- GET /webhooks/events/recent
- GET /webhooks/events/{event_id}

#### Signal and Decision
- POST /signals/normalize
- GET /signals/recent
- POST /decisions/strategy/evaluate
- POST /decisions/risk/evaluate

#### Execution
- POST /execution/requests/simulate
- GET /execution/requests/recent
- GET /execution/orders/recent
- GET /execution/fills/recent
- GET /execution/positions/current

#### Monitoring and Analytics
- GET /execution_outcomes/vp_policy_reason_monitor
- GET /execution_outcomes/vp_policy_reason_best_worst
- GET /execution_outcomes/vp_policy_reason/policy_selector_simulation
- GET /health
- GET /system/health/dependencies

### API behavior requirements
- every write endpoint emits an internal idempotency key
- decision and risk responses include reason codes and machine-readable status
- simulated execution is explicit via mode field
- no endpoint silently mutates payload semantics between layers

## Persistence Blueprint

### Current tables already available
- bar_states
- trade_candidates
- trade_events
- regime_states
- model_predictions
- feature_registry
- feature_snapshots
- feature_snapshot_values
- execution_journal
- execution_outcomes
- volume_profile_snapshots

### Required additional tables for full stack maturity

#### raw_webhook_events
- event_id primary key
- source string
- received_at timestamp
- source_ip string
- headers_json json
- payload_json json
- payload_hash string
- schema_version string

#### normalized_signals
- normalized_id primary key
- event_id foreign key
- symbol
- broker_symbol
- timeframe
- side
- signal_name
- strategy_id
- score
- bar_time
- market_price
- features_json json
- created_at timestamp

#### strategy_decisions
- strategy_decision_id primary key
- normalized_id foreign key
- decision
- reason_code
- confidence
- policy_version
- decision_json json
- created_at timestamp

#### risk_events
- risk_event_id primary key
- strategy_decision_id foreign key
- risk_decision
- reason_code
- position_size
- stop_loss_distance
- take_profit_distance
- risk_json json
- created_at timestamp

#### execution_requests
- execution_request_id primary key
- strategy_decision_id foreign key
- risk_event_id foreign key
- symbol
- side
- order_type
- quantity
- request_json json
- mode
- created_at timestamp

#### broker_orders
- broker_order_pk primary key
- execution_request_id foreign key
- broker_name
- broker_order_id
- order_status
- submitted_at
- acknowledged_at
- updated_at
- broker_payload_json json

#### fills
- fill_id primary key
- broker_order_pk foreign key
- fill_time
- fill_price
- fill_qty
- fee
- side
- raw_json json

#### positions
- position_id primary key
- symbol
- side
- qty
- avg_entry
- realized_pnl
- unrealized_pnl
- last_synced_at
- status

#### system_logs
- log_id primary key
- trace_id
- level
- component
- message
- context_json json
- created_at

## Module and Package Blueprint

### Target package split
```text
backend/
	api/
		webhook_routes.py
		signal_routes.py
		monitoring_routes.py
		execution_routes.py

	models/
		webhook_models.py
		signal_models.py
		decision_models.py
		risk_models.py
		execution_models.py

	services/
		webhook_service.py
		normalization_service.py
		strategy_service.py
		risk_service.py
		execution_service.py
		broker_service.py

	adapters/
		tradingview_adapter.py
		broker_paper_adapter.py
		broker_btcc_adapter.py
		broker_kraken_adapter.py

	db/
		db.py
		repositories/
			raw_event_repo.py
			normalized_signal_repo.py
			decisions_repo.py
			execution_repo.py
			positions_repo.py

	engine/
		policy_engine.py
		selector_engine.py
		reason_monitor.py
		position_sizer.py
```

### Migration note for current codebase
- keep backend/api_server.py as orchestration shell during transition
- extract one route family at a time into backend/api/*
- preserve current endpoint behavior while moving implementation behind service classes

## Decision Pipeline Contract

### Strategy engine responsibilities
- evaluate signal quality and context
- use VP reason best and worst and monitor surfaces
- return approve/reject/defer/downgrade with reason_code

### Risk engine responsibilities
- enforce daily loss lock
- enforce max concurrent positions
- enforce symbol exposure and cooldowns
- return approve/deny/resize

### Execution engine responsibilities
- accept only strategy-approved and risk-approved requests
- route to paper adapter in Alpha
- update order and fill and position state machine

## State Machine Requirements
- Signal received
- Signal normalized
- Strategy decision emitted
- Risk decision emitted
- Execution request created
- Order accepted or rejected
- Fill partial or full
- Position opened or adjusted or closed
- Outcome evaluated

Every transition must include:
- correlation ids
- monotonic timestamps
- actor/component name
- reason code

## Monitoring Requirements
- webhook success rate and validation failures
- normalization failures by field
- strategy approve and reject counts
- risk deny counts by reason
- simulated order acceptance latency
- fill latency and slippage estimates
- open position exposure
- outcome drift and selector monitor health

## Build Phases

### Phase 1: Contracts and Intake
- define canonical pydantic models for raw alert and normalized signal
- persist raw webhook event unchanged
- implement deterministic normalization path

### Phase 2: Storage and Repositories
- add new tables for raw events, normalized signals, decisions, requests, orders, fills, positions
- add repository layer with typed io contracts

### Phase 3: Decision Pipeline
- implement strategy service and reason-code output
- wire risk service with deny and resize outcomes
- emit execution_request in simulated mode only

### Phase 4: Simulated Execution
- implement paper adapter with deterministic order ids
- implement order state transitions and fill simulation hooks

### Phase 5: Observability
- add dashboard views for intake, decisions, risk, execution, and monitor health
- add endpoint-level and component-level health checks

### Phase 6: Hardening
- duplicate signal protection
- retry policy with dead-letter queue
- circuit breakers and kill switches
- backfill and replay tooling

## Milestone Alpha Definition
Alpha is complete only when one full dry-run path is proven:
- TradingView sends webhook JSON
- backend stores raw payload
- signal normalizer produces canonical object
- strategy decision is produced
- risk decision is produced
- execution request is created in simulated mode
- simulated order result is persisted
- records are queryable through monitoring endpoints

## Alpha Acceptance Criteria
- all write-path messages have correlation ids
- replaying same event_id is idempotent
- decision and risk reason codes are populated
- no unhandled exceptions in end-to-end dry-run pytest suite
- dashboard shows intake, decision, risk, and simulated execution states
- vp_policy_reason_monitor is queryable and reflected in strategy decision traces

## Go-Live Gates
Live broker routing remains disabled until all gates pass:
- 30-day dry-run stability window
- risk controls proven under stress tests
- position reconciliation and drift checks passing
- alerting on webhook, execution, and sync failures
- manual kill switch tested and documented

## Immediate Next Implementation Tasks
- implement raw_webhook_events table and repository
- add normalized_signals write-path from current webhook route
- add strategy_decisions and risk_events persistence in simulated flow
- add execution_requests and broker_orders for paper adapter
- expose compact monitoring endpoints for each pipeline stage
