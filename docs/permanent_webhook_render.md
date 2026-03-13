# Permanent Webhook on Render

## Goal
Create a stable webhook URL for TradingView that does not expire.

Target endpoint:
```
POST https://YOUR-RENDER-DOMAIN/webhooks/tradingview/batch
```

This endpoint accepts the batch payload that the Bridge Signal Sender Pine script emits
and that the Cloudflare Worker forwards.

## Prerequisites
- GitHub repository connected to Render
- Render account (free tier is fine)
- Cloudflare Worker forwarding requests to the Render URL

## Deploy steps (Render Web Service)

1. In Render, create a new **Web Service** from your GitHub repo.
2. Leave **Root Directory** blank (the repo root is the project root).
3. Set **Build Command**:
   ```
   pip install -r requirements.txt
   ```
4. Set **Start Command**:
   ```
   uvicorn backend.api_server:app --host 0.0.0.0 --port $PORT
   ```
5. Set **Health Check Path**:
   ```
   /health
   ```
6. Add **environment variables** (use `sync: false` so Render prompts for the value):

   | Variable | Purpose |
   |----------|---------|
   | `SIGNAL_WEBHOOK_KEY` | Authenticates direct `/webhooks/tradingview` calls |
   | `TRADINGVIEW_INGEST_SIGNAL_KEY` | Authenticates batch ingest from the Cloudflare Worker |

   Both env vars can be set to the same secret or different secrets. The ingest service
   falls back through `TRADINGVIEW_INGEST_SIGNAL_KEY → SIGNAL_WEBHOOK_KEY → TV_SIGNAL_KEY`
   so setting `SIGNAL_WEBHOOK_KEY` alone is sufficient if you prefer a single key.

7. Deploy.

## Resulting stable webhook
After deploy, Render will give a fixed domain such as:
```
https://btc-adaptive-engine-bridge.onrender.com
```

### Cloudflare Worker → Render
Set the Cloudflare Worker's backend target to:
```
POST https://btc-adaptive-engine-bridge.onrender.com/webhooks/tradingview/batch
```
The Worker passes the `?secret=<TV_WEBHOOK_SECRET>` (or `X-SIGNAL-KEY` header) through
to the backend. The backend validates it against `TRADINGVIEW_INGEST_SIGNAL_KEY`.

### TradingView alert URL
Keep using the Cloudflare Worker URL in TradingView:
```
https://tv-webhook.staybusyent.workers.dev/?secret=<TV_WEBHOOK_SECRET>
```

**Do NOT** point TradingView directly at the Render URL — the Render free tier spins
down after 15 minutes of inactivity. The Worker provides buffering and retries.

## Post-deploy verification

### Health check
```
GET https://btc-adaptive-engine-bridge.onrender.com/health
```
Expected response: `{"status": "ok"}`

### Smoke test (PowerShell)
```powershell
$url = "https://btc-adaptive-engine-bridge.onrender.com/webhooks/tradingview/batch"
$headers = @{ "X-SIGNAL-KEY" = "YOUR_TRADINGVIEW_INGEST_SIGNAL_KEY" }
$payload = @{
  source      = "tradingview"
  namespace   = "ghostprint"
  symbol      = "BTCUSDT.P"
  chart_tf    = "1"
  batch_id    = "smoke-test-$(Get-Date -Format 'yyyyMMddHHmmss')"
  batch_trigger_side = "long"
  batch_size  = 1
  batch_close_time   = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  confirmed   = $true
  events      = @(@{
    event_id     = "evt-smoke-001"
    event_time   = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    side         = "long"
    signal_type  = "continuation"
    signal_family= "momentum"
    signal_name  = "render_smoke_test"
    strategy_id  = "bridge_signal_sender_v2"
    price        = 70000.0
    confirmed    = $true
    micro        = @{ ticker = "BTCUSDT.P"; timeframe = "1" }
    macro        = @{}
  })
} | ConvertTo-Json -Compress -Depth 5
Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Headers $headers -Body $payload
```

Expected: `status: "accepted"` with `raw_saved: true`.

## Why the 502 errors happen

502 Bad Gateway from TradingView means the Cloudflare Worker received the alert
but the Render backend did not return a response in time.

**Causes and fixes:**

| Cause | Fix |
|-------|-----|
| Wrong `rootDir` in render.yaml (Windows local path) | Removed — rootDir defaults to repo root ✅ |
| Start command used `tradingview_bridge_standalone.py` which has no `/webhooks/tradingview/batch` endpoint | Changed to `api_server.py` ✅ |
| Wrong env var name `TV_SIGNAL_KEY` vs `SIGNAL_WEBHOOK_KEY` | Fixed — render.yaml now declares `SIGNAL_WEBHOOK_KEY` ✅ |
| Free-tier spin-down (Render sleeps after 15 min idle) | Keep using the Cloudflare Worker as a buffer; consider a free uptime monitor (e.g., UptimeRobot) to ping `/health` every 10 minutes |

## Render free-tier spin-down mitigation

Render's free tier suspends the service after 15 minutes of no requests.
The first webhook after a sleep period takes ~30 seconds to start, which
causes the Worker to time out and TradingView to see a failure.

Options to prevent this:
1. Add a free [UptimeRobot](https://uptimerobot.com) monitor pinging `/health` every 10 minutes.
2. Upgrade to Render's paid "Starter" plan (always-on).
3. Run the backend locally with a permanent tunnel (see `register_cloudflared_tunnel_task.ps1`).

