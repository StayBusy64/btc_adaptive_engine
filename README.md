# BTC Adaptive Engine

A modular adaptive trading system scaffold for **BTCUSDT.P on the 1-minute timeframe**.

## Architecture

- `pine/`  
  TradingView Pine Script internal market sensor

- `backend/`  
  Python adaptive engine:
  - API ingestion
  - event logging
  - outcome labeling
  - regime logic
  - decay logic
  - model logic
  - governance
  - feedback

- `database/`  
  SQLite schema and local DB

- `dashboard/`  
  FastAPI/Jinja dashboard

- `config/`  
  Centralized YAML configuration

- `docs/`  
  System blueprint and notes

## Initial Goal

Build a clean repo scaffold that VS Code + Copilot can analyze as a complete project.

## Suggested Next Steps

1. Create Python virtual environment
2. Install requirements
3. Start FastAPI backend
4. Build Pine feature engine
5. Connect TradingView webhook payloads to backend API

## Local Operations

Stable API launcher:

```powershell
Set-Location "C:\Users\Stayb\OneDrive\Desktop\btc_adaptive_engine"
.\start_api.ps1
```

Watchdog launcher (auto-restarts if API exits):

```powershell
Set-Location "C:\Users\Stayb\OneDrive\Desktop\btc_adaptive_engine"
.\start_api_watchdog.ps1
```

Watchdog log file:

`data/logs/api_watchdog.log`

Ingest receipt log file:

`data/logs/tv_ingest_receipts.jsonl`

One-shot pipeline status check:

```powershell
Set-Location "C:\Users\Stayb\OneDrive\Desktop\btc_adaptive_engine"
.\check_pipeline_status.ps1 -TvWebhookSecret "<TV_WEBHOOK_SECRET>"
```

Register watchdog auto-start task (logon):

```powershell
Set-Location "C:\Users\Stayb\OneDrive\Desktop\btc_adaptive_engine"
.\register_api_watchdog_task.ps1
```

Register cloudflared tunnel auto-start task (logon):

```powershell
Set-Location "C:\Users\Stayb\OneDrive\Desktop\btc_adaptive_engine"
.\register_cloudflared_tunnel_task.ps1
```
