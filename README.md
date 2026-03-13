# BTC Adaptive Engine

A modular adaptive trading system for **BTCUSDT.P on the 1-minute timeframe**.
Runs a full pipeline from TradingView alert → signal normalization → strategy/risk decisions → simulated execution → SQLite persistence → analytics API.

**Current status: ✅ 446/446 tests passing. TradingView → webhook → pipeline is fully operational.**

---

## Quick answer: does the TV → webhook → pipeline still work?

**Yes.** Here is the live path end-to-end:

```
TradingView Pine Script  (tradingview/bridge_signal_sender.pine)
  ↓  alert fires on bar close
Cloudflare Worker  (https://tv-webhook.staybusyent.workers.dev/?secret=<TV_WEBHOOK_SECRET>)
  ↓  forwards JSON batch
POST /webhooks/tradingview/batch  (FastAPI backend, port 8000)
  ↓  accepts + queues batch file
Ingest Cycle Scheduler  (runs every 60 s inside the API process)
  ↓  normalizes events, writes signal journal, evaluates outcomes, updates market bias
SQLite database  (data/btc_engine.db)
  ↓  persisted for every downstream service
Feature Pipeline  (11 engines, 97 features)
Strategy Engine → Risk Engine → Execution Engine  (dry-run / simulated)
Analytics API  (76 endpoints for audit, monitoring, and cohort analysis)
```

The **legacy direct-bar endpoint** (`POST /webhook/tradingview`) also still works and
triggers the full feature pipeline synchronously on each bar.

---

## Repository layout

```
btc_adaptive_engine/
├── backend/               # All Python source
│   ├── api_server.py      # FastAPI app — 76 endpoints
│   ├── feature_engine.py  # Pipeline orchestrator
│   ├── event_writer.py    # SQLite read/write helpers
│   ├── normalization_service.py  # Alert → canonical signal
│   ├── tradingview_ingest_*.py   # Batch ingest + cycle scheduler
│   ├── *_engine.py        # Feature / strategy / risk engines
│   └── main.py            # One-shot DB init helper
├── tradingview/
│   ├── bridge_signal_sender.pine  # Pine v6 alert script
│   ├── bridge_manifest.json       # Release contract + manifest
│   └── bridge_release_notes.md
├── database/
│   └── schema.sql         # 21-table SQLite schema
├── tests/                 # 446 pytest tests
├── docs/                  # System blueprint and setup guides
├── config/                # YAML configuration
├── dashboard/             # Jinja dashboard (read-only)
├── tools/                 # build_bridge_pine.py and helpers
└── requirements.txt       # fastapi, uvicorn, pydantic, etc.
```

---

## Full pipeline in plain English

### 1 · TradingView Pine Script

`tradingview/bridge_signal_sender.pine` runs on a BTCUSDT.P 1m chart.
On every confirmed bar close it fires an `alert()` call whose body is a JSON
**batch payload** containing:
- batch metadata (release ID, strategy ID, contract version)
- an `events` array with one or more signal events, each carrying full OHLCV,
  EMA stack, RSI, ATR, candle structure, and a `research` block with scored fields

The alert is configured with:
- **trigger**: Once Per Bar Close
- **webhook URL**: `https://tv-webhook.staybusyent.workers.dev/?secret=<TV_WEBHOOK_SECRET>`

### 2 · Cloudflare Worker

The Worker validates the `secret` query parameter and forwards the full payload
to `POST /webhooks/tradingview/batch` on the backend.
It acts as a secure proxy so the backend URL is never exposed directly in TradingView.

### 3 · Batch intake (`POST /webhooks/tradingview/batch`)

The endpoint:
1. Validates the `X-SIGNAL-KEY` header against `TRADINGVIEW_INGEST_SIGNAL_KEY`.
2. Checks payload size limit.
3. Writes the raw batch as a JSON file under `data/tv_ingest/pending/`.
4. Returns `202 Accepted` immediately so TradingView does not time out.

### 4 · Ingest Cycle Scheduler

Runs inside the FastAPI process as an async background task.
Polls `data/tv_ingest/pending/` every 60 seconds (configurable via
`TRADINGVIEW_INGEST_CYCLE_INTERVAL_SECONDS`).

For each pending batch it:
1. Normalizes every event (symbol, timeframe, side aliases, feature extraction).
2. Writes a `NormalizedSignal` row.
3. Runs signal analytics (market bias preview).
4. Appends a receipt to `data/logs/tv_ingest_receipts.jsonl`.
5. Moves the batch file to `data/tv_ingest/processed/`.

### 5 · Phase 1 dry-run pipeline (`POST /webhooks/tradingview`)

For direct (non-batch) alert payloads the Phase 1 endpoint runs the full
decision chain in one synchronous call:

```
raw_webhook_event
  → normalized_signal
  → strategy_decision   (approve / reject / defer / downgrade)
  → risk_event          (position size, stop, target)
  → execution_request   (simulated order)
  → paper fill + position update
```

All objects are persisted to SQLite with linked IDs and timestamps.
The response body echoes every step so you can audit the entire chain in one call.

### 6 · Legacy bar-state webhook (`POST /webhook/tradingview`)

Accepts a `TradingViewPayload` (bar OHLCV + scores), inserts a `bar_state` row,
then immediately runs the **full feature pipeline** on the most recent N bars for
that symbol/timeframe and writes a `feature_snapshot`.

### 7 · Feature pipeline (11 engines, 97 features)

Engines run in order:

| Engine | Features |
|--------|----------|
| `CandleFeatureEngine` | OHLCV-derived candle metrics (body ratio, wick sizes, bar type) |
| `RangeExpansionEngine` | Range expansion relative to ATR baseline |
| `VolatilityEngine` | ATR, realised vol, Bollinger Band width, vol regime |
| `TrendEngine` | EMA 9/21/55, SMA 20, trend alignment score, slope |
| `IndicatorsEngine` | RSI 14, MACD, Stochastic %K/%D, momentum, ROC |
| `StructureEngine` | Swing highs/lows, higher-high/lower-low structure flags |
| `LiquidityEngine` | Wick-sweep flags, liquidity grab detection |
| `DisplacementEngine` | Impulse quality, follow-through, decay scores |
| `SessionContextEngine` | Asia / London / NY / overlap session flags |
| `OrderFlowEngine` | CLV-based buying/selling pressure, cumulative delta, exhaustion flag |
| `DivergenceEngine` | RSI and MACD histogram bullish/bearish divergences |

Plus the **Volume Profile engine** (computed as a side-effect, not in the chain):
- POC, VAH, VAL, shape label, balance state
- Migration delta and direction
- Auction regime, trade bias, policy candidate, policy side

### 8 · Regime + Model engines

After features are computed:
- `regime_engine.classify_regime()` → regime label + confidence + transition risk
- `model_engine.score_state()` → long/short/no-trade probabilities + setup trust score

Results are stored in `regime_states` and `model_predictions`.

### 9 · SQLite persistence (21 tables)

| Table | Purpose |
|-------|---------|
| `bar_states` | Raw bar data from webhook |
| `feature_snapshots` | One row per pipeline run |
| `feature_snapshot_values` | Key-value store for each feature |
| `feature_registry` | Catalogue of known feature specs |
| `volume_profile_snapshots` | Rolling VP per symbol/timeframe |
| `raw_webhook_events` | Immutable inbound payloads |
| `normalized_signals` | Canonical signal objects |
| `strategy_decisions` | Strategy verdict per signal |
| `risk_events` | Risk parameters per decision |
| `execution_requests` | Simulated order instructions |
| `broker_orders` | Order records (paper / live) |
| `fills` | Fill records |
| `positions` | Aggregated position state |
| `trade_candidates` | Candidate pool for execution workers |
| `trade_events` | Trade lifecycle events |
| `execution_journal` | Full audit trail with P&L |
| `execution_outcomes` | Labelled outcomes per execution |
| `regime_states` | Regime classifications over time |
| `model_predictions` | Model probability outputs |
| `release_cohort_scores` | Cohort scoring by release version |
| `feature_lifecycle` | Feature promotion / demotion tracking |

---

## Auth

Webhook endpoints are protected by the `X-SIGNAL-KEY` header:

```
X-SIGNAL-KEY: <value of SIGNAL_WEBHOOK_KEY env var>
```

The batch ingest path uses `TRADINGVIEW_INGEST_SIGNAL_KEY` (set in the Cloudflare Worker).

---

## Key API endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness check |
| `GET` | `/bar_states/recent` | Recent ingested bars |
| `GET` | `/feature_snapshots/recent` | Recent feature pipeline outputs |
| `GET` | `/volume_profile_snapshots/recent` | Recent VP snapshots |
| `POST` | `/webhook/tradingview` | Legacy bar + feature pipeline trigger |
| `POST` | `/webhooks/tradingview` | Phase 1 dry-run pipeline (signal → execution) |
| `POST` | `/webhooks/tradingview/batch` | Batch ingest from Cloudflare Worker |
| `GET` | `/webhooks/tradingview/batches/recent` | Recent ingest batches |
| `GET` | `/webhooks/tradingview/events/recent` | Recent normalized events |
| `GET` | `/webhooks/tradingview/signal-journal/recent` | Signal journal |
| `GET` | `/webhooks/tradingview/signal-outcomes/recent` | Signal outcome history |
| `GET` | `/webhooks/tradingview/market-bias/recent` | Market bias state |
| `GET` | `/execution_journal/recent` | Execution audit trail |
| `GET` | `/execution_outcomes/vp_policy_reason_monitor` | VP policy reason monitor |
| `GET` | `/execution_outcomes/policy_recommendation` | Policy recommendation |
| `GET` | `/cohorts/leaderboard` | Release cohort leaderboard |
| `POST` | `/trade_candidates` | Submit trade candidate |
| `GET` | `/trade_candidates/recent` | Recent trade candidates |

Full interactive API docs: `http://127.0.0.1:8000/docs`

---

## Running locally

### One-time setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Start the API (port 8000)

```powershell
$env:SIGNAL_WEBHOOK_KEY           = "change-me-now"
$env:TRADINGVIEW_INGEST_SIGNAL_KEY = "change-me-now"
python -m uvicorn backend.api_server:app --host 127.0.0.1 --port 8000 --reload
```

Or use the launcher scripts:

```powershell
# Stable launcher
.\start_api.ps1

# Auto-restarts on crash
.\start_api_watchdog.ps1
```

### Verify it's up

```powershell
curl http://127.0.0.1:8000/health
```

### Pipeline status check (one-shot)

```powershell
.\check_pipeline_status.ps1 -TvWebhookSecret "<TV_WEBHOOK_SECRET>"
```

### Run tests

```powershell
python -m pytest tests/ -q
# Expected: 446 passed
```

---

## TradingView alert setup (recap)

1. Open the Pine Editor in TradingView.
2. Load `tradingview/bridge_signal_sender.pine` (Pine v6).
3. Add it to the BTCUSDT.P 1m chart.
4. Create an alert:
   - **Condition**: `Bridge Signal Sender → Any alert() function call`
   - **Trigger**: Once Per Bar Close
   - **Webhook URL**: `https://tv-webhook.staybusyent.workers.dev/?secret=<TV_WEBHOOK_SECRET>`
   - **Message**: `bridge` (the Pine script generates the full JSON body)
5. The Worker validates the secret and forwards to `POST /webhooks/tradingview/batch`.
6. The ingest scheduler picks it up within 60 seconds.

Manifest and release contract: `tradingview/bridge_manifest.json`

To validate the Pine file and regenerate release notes:

```powershell
python tools/build_bridge_pine.py --check-pine --write-release-notes
```

---

## Canonical pipeline verification

`pipeline_config.json` at the repo root is the single source of truth for all
local/remote URLs and parameter names.  All `.ps1` pipeline scripts read from
it via `tools/pipeline_common.ps1` — no hardcoded URLs or port scanning.

```jsonc
// pipeline_config.json (key fields)
{
  "local_api_base":       "http://127.0.0.1:8000",
  "backend_base":         "https://api.dopedreamspnl.com",
  "worker_base":          "https://tv-webhook.staybusyent.workers.dev",
  "batch_path":           "/webhooks/tradingview/batch",
  "worker_secret_param":  "secret",
  "signal_key_file":      "data/tv_ingest/signal_key.txt"
}
```

### End-to-end verifier (the truth test)

Sends a real batch through the full pipeline and validates each stage:

```powershell
$env:TV_WEBHOOK_SECRET = "<your-worker-secret>"
.\verify-pipeline-e2e.ps1
```

Output shows `PASS` / `FAIL` for each stage:

| Stage | What it proves |
|---|---|
| Worker accepted | Worker URL, secret, and config are correct |
| Backend reachable | Public backend / Cloudflare tunnel is up |
| Local API reachable | FastAPI running on port 8000 |
| Batch persisted | Ingest route accepted and wrote `processing_status` JSON |
| Batch processed | Scheduler advanced the batch to `status=processed` |
| Journal advanced | Downstream analytics wrote to `signal_journal.jsonl` |

### Periodic watcher

Registers a Windows scheduled task that runs the autoverify script automatically:

```powershell
.\install-pipeline-autoverify.ps1 -LaunchNow
```

---

## Auto-start tasks (Windows)

```powershell
# Start API watchdog at logon
.\register_api_watchdog_task.ps1

# Start Cloudflare tunnel at logon
.\register_cloudflared_tunnel_task.ps1
```

Logs:
- `data/logs/api_watchdog.log`
- `data/logs/tv_ingest_receipts.jsonl`

---

## Docs

| File | Contents |
|------|----------|
| `docs/system_blueprint.md` | Architecture principles and domain object specs |
| `docs/tradingview_bridge_setup.md` | Bridge setup and release workflow |
| `docs/tradingview_bridge_refinement_runbook.md` | Runbook for refining bridge signals |
| `docs/permanent_webhook_render.md` | Render.com deployment guide |
