# Permanent Webhook on Render

## Goal
Create a stable webhook URL for TradingView that does not expire.

Target endpoint format:
https://YOUR-RENDER-DOMAIN/webhooks/tradingview?signal_key=YOUR_SECRET

## Prerequisites
- GitHub repository connected to Render
- Render account
- FastAPI bridge file present: backend/tradingview_bridge_standalone.py

## Important repo-path note
This repository currently has git root at C:/Users/Stayb and project files under OneDrive/Desktop/btc_adaptive_engine.

When creating the Render service, set Root Directory to:
OneDrive/Desktop/btc_adaptive_engine

## Deploy steps (Render Web Service)
1. In Render, create a new Web Service from your GitHub repo.
2. Set Root Directory to OneDrive/Desktop/btc_adaptive_engine.
3. Set Build Command:
   pip install -r requirements.txt
4. Set Start Command:
   uvicorn backend.tradingview_bridge_standalone:app --host 0.0.0.0 --port $PORT
5. Set Health Check Path:
   /health
6. Add environment variable:
   TV_SIGNAL_KEY=YOUR_SECRET
7. Deploy.

## Resulting stable webhook
After deploy, Render will give a fixed domain such as:
https://btc-adaptive-engine-bridge.onrender.com

Use this in TradingView:
https://btc-adaptive-engine-bridge.onrender.com/webhooks/tradingview?signal_key=YOUR_SECRET

## Post-deploy verification
Health:
https://btc-adaptive-engine-bridge.onrender.com/health

PowerShell smoke test:
$url = "https://btc-adaptive-engine-bridge.onrender.com/webhooks/tradingview?signal_key=YOUR_SECRET"
$payload = @{
  source="tradingview"
  namespace="ghostprint"
  strategy_id="bridge_signal_sender_v1"
  ticker="BTCUSDT.P"
  tickerid="BINANCE:BTCUSDT.P"
  exchange="BINANCE"
  timeframe="1"
  side="long"
  signal_name="render_pipeline_test"
  price=70000.0
  bar_time=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
} | ConvertTo-Json -Compress
Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Body $payload

## TradingView alert settings
- Script title: Bridge Signal Sender
- Condition: Any alert() function call
- Webhook URL: your stable Render URL + /webhooks/tradingview?signal_key=YOUR_SECRET
- Message field: simple placeholder text is fine (script alert() builds JSON payload)
