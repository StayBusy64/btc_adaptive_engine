# =====================================
# TradingView Signal Simulation Script
# =====================================
# Sends a minimal TradingView-style alert payload to the Cloudflare Worker,
# exercising the full ingestion path:
#
#   test_tradingview_signal.ps1
#          ↓
#   Cloudflare Worker  (URL from pipeline_config.json)
#          ↓
#   api.dopedreamspnl.com/webhooks/tradingview/batch
#          ↓
#   Cloudflare Tunnel
#          ↓
#   FastAPI backend
#
# Usage (from project root):
#   .\test_tradingview_signal.ps1
#
# Worker secret is read from $env:TV_WEBHOOK_SECRET.

$ErrorActionPreference = "Stop"

$_scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $_scriptRoot "tools\pipeline_common.ps1")

$workerUrl = Get-PipelineWorkerUrl

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
Write-Host "URL: $workerUrl"
Write-Host ""

try {
    $response = Invoke-RestMethod `
        -Uri $workerUrl `
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
