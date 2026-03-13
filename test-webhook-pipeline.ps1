# =====================================
# TradingView Pipeline Test Script
# =====================================
# Credentials are read from environment variables so this script works on
# any machine without modification.  Override on the command line if needed.
#
# Required env vars (or pass as params):
#   TV_WORKER_URL   – full Worker URL including the ?secret= query param
#                     e.g. https://tv-webhook.staybusyent.workers.dev/?secret=...
#   BACKEND_BASE_URL – base URL of the FastAPI backend (no trailing slash)
#                     e.g. https://api.dopedreamspnl.com
#   TRADINGVIEW_INGEST_SIGNAL_KEY (or TV_SIGNAL_KEY) – backend signal key

param(
    [string]$WorkerUrl    = "",
    [string]$BackendBaseUrl = "",
    [string]$SignalKey    = ""
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

# Resolve Worker URL from env if not passed explicitly
if ([string]::IsNullOrWhiteSpace($WorkerUrl)) {
    $WorkerUrl = $env:TV_WORKER_URL
}

# Resolve backend base URL from env if not passed explicitly
if ([string]::IsNullOrWhiteSpace($BackendBaseUrl)) {
    $BackendBaseUrl = $env:BACKEND_BASE_URL
}
if ([string]::IsNullOrWhiteSpace($BackendBaseUrl)) {
    # Default: production backend for this repo.  Override with BACKEND_BASE_URL env var
    # or the -BackendBaseUrl parameter for staging/local runs.
    $BackendBaseUrl = "https://api.dopedreamspnl.com"
}
$BackendBaseUrl = $BackendBaseUrl.TrimEnd("/")
$BACKEND_URL = "$BackendBaseUrl/webhooks/tradingview/batch"

# Resolve signal key: param > env TRADINGVIEW_INGEST_SIGNAL_KEY > env TV_SIGNAL_KEY > local file
if ([string]::IsNullOrWhiteSpace($SignalKey)) {
    $SignalKey = $env:TRADINGVIEW_INGEST_SIGNAL_KEY
}
if ([string]::IsNullOrWhiteSpace($SignalKey)) {
    $SignalKey = $env:TV_SIGNAL_KEY
}
if ([string]::IsNullOrWhiteSpace($SignalKey)) {
    $keyFile = Join-Path $root "data\tv_ingest\signal_key.txt"
    if (Test-Path $keyFile) {
        $SignalKey = (Get-Content $keyFile -TotalCount 1).Trim()
    }
}
if ([string]::IsNullOrWhiteSpace($SignalKey)) {
    throw "Signal key not found. Set TRADINGVIEW_INGEST_SIGNAL_KEY env var or run backend/tradingview_webhook_setup.py first."
}
$SIGNAL_KEY = $SignalKey

Write-Host "---- Testing Worker endpoint ----"

if ([string]::IsNullOrWhiteSpace($WorkerUrl)) {
    Write-Host "Skipped: set TV_WORKER_URL env var or pass -WorkerUrl to test the Worker."
} else {
    $runId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

    $workerPayload = @{
        source="tradingview"
        namespace="pipeline_test"
        symbol="BTCUSDT.P"
        chart_tf="1"
        batch_id="pipeline-worker-$runId"
        batch_trigger_side="buy"
        batch_size=1
        batch_close_time=$runId
        confirmed=$true
        events=@(
            @{
                event_id="pipeline-worker-event-$runId"
                event_time=$runId
                side="buy"
                signal_type="continuation"
                signal_family="momentum"
                price=70000
                confirmed=$true
                micro=@{}
                macro=@{}
            }
        )
    } | ConvertTo-Json -Depth 5

    try {
        $resp = Invoke-WebRequest `
            -Uri $WorkerUrl `
            -Method POST `
            -ContentType "application/json" `
            -Body $workerPayload `
            -TimeoutSec 15

        Write-Host "Worker response:" $resp.StatusCode
    }
    catch {
        Write-Host "Worker test failed"
        Write-Host $_
    }
}

Write-Host "`n---- Testing Backend endpoint ----"

$runId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

$backendPayload = @{
    source="tradingview"
    namespace="pipeline_test"
    symbol="BTCUSDT.P"
    chart_tf="1"
    batch_id="pipeline-backend-$runId"
    batch_trigger_side="buy"
    batch_size=1
    batch_close_time=$runId
    confirmed=$true
    events=@(
        @{
            event_id="pipeline-backend-event-$runId"
            event_time=$runId
            side="buy"
            signal_type="continuation"
            signal_family="momentum"
            price=70000
            confirmed=$true
            micro=@{}
            macro=@{}
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $resp = Invoke-WebRequest `
        -Uri "${BACKEND_URL}?signal_key=$SIGNAL_KEY" `
        -Method POST `
        -ContentType "application/json" `
        -Body $backendPayload `
        -TimeoutSec 15

    Write-Host "Backend response:" $resp.StatusCode
}
catch {
    Write-Host "Backend test failed"
    Write-Host $_
}

Write-Host "`nPipeline test complete."

