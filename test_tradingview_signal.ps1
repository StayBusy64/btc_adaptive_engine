# =====================================
# TradingView Signal Simulation Script
# =====================================
# Sends a minimal TradingView-style alert payload to the Cloudflare Worker,
# exercising the full ingestion path:
#
#   test_tradingview_signal.ps1
#          ↓
#   tv-webhook.staybusyent.workers.dev
#          ↓
#   Cloudflare Worker
#          ↓
#   api.dopedreamspnl.com/webhooks/tradingview/batch
#          ↓
#   Cloudflare Tunnel
#          ↓
#   FastAPI backend
#
# Usage (from project root):
#   .\test_tradingview_signal.ps1

$WORKER_URL = "https://tv-webhook.staybusyent.workers.dev/?secret=btc_signal_secret_7421"

$body = @{
    source             = "tradingview"
    namespace          = "tradingview"
    symbol             = "BTCUSDT"
    chart_tf           = "1"
    batch_id           = "btc-test-$(Get-Date -Format yyyyMMddHHmmss)"
    batch_trigger_side = "buy"
    batch_size         = 1
    batch_close_time   = (Get-Date).ToString("o")
} | ConvertTo-Json -Depth 3

Write-Host "Sending TradingView signal to Worker..."
Write-Host "URL: $WORKER_URL"
Write-Host ""

try {
    $response = Invoke-RestMethod `
        -Uri $WORKER_URL `
        -Method Post `
        -ContentType "application/json" `
        -Body $body

    Write-Host "Response:" ($response | ConvertTo-Json -Depth 5)
} catch {
    $statusCode = $_.Exception.Response.StatusCode.value__
    Write-Host "HTTP $statusCode — $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message
    }
}

Write-Host ""
Write-Host "Signal sent. Watch your FastAPI terminal for:"
Write-Host "  POST /webhooks/tradingview/batch 200   (accepted)"
Write-Host "  POST /webhooks/tradingview/batch 422   (schema mismatch — pipeline still reached the backend)"
