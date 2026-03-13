# =====================================
# TradingView Pipeline Test Script
# =====================================

$WORKER_URL = "https://tv-webhook.staybusyent.workers.dev/?secret=btc_signal_secret_7421"
$BACKEND_URL = "https://api.dopedreamspnl.com/webhooks/tradingview/batch"
$SIGNAL_KEY = "1e08c7d13393255c922dde02dcc28196"

Write-Host "---- Testing Worker endpoint ----"

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
        -Uri $WORKER_URL `
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

Write-Host "`n---- Testing Backend endpoint ----"

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


