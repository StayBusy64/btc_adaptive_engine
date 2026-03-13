# =====================================
# TradingView Pipeline Test Script
# =====================================
# Reads all URLs and keys from pipeline_config.json via tools/pipeline_common.ps1.
# No hardcoded endpoints, ports, or secrets in this file.
#
# Usage:
#   .\test-webhook-pipeline.ps1
#
# To override the worker secret:
#   $env:TV_WEBHOOK_SECRET = "your_secret"
#   .\test-webhook-pipeline.ps1

$ErrorActionPreference = "Stop"

$_scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $_scriptRoot "tools\pipeline_common.ps1")

$workerUrl = Get-PipelineWorkerUrl
$backendBatchUrl = Get-PipelineBackendBatchUrl

$runId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

Write-Host "---- Testing Worker endpoint ----"

$workerPayload = @{
    source             = "tradingview"
    namespace          = "pipeline_test"
    symbol             = "BTCUSDT.P"
    chart_tf           = "1"
    batch_id           = "pipeline-worker-$runId"
    batch_trigger_side = "buy"
    batch_size         = 1
    batch_close_time   = $runId
    confirmed          = $true
    events             = @(
        @{
            event_id      = "pipeline-worker-event-$runId"
            event_time    = $runId
            side          = "buy"
            signal_type   = "continuation"
            signal_family = "momentum"
            price         = 70000
            confirmed     = $true
            micro         = @{}
            macro         = @{}
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $resp = Invoke-WebRequest `
        -Uri $workerUrl `
        -Method POST `
        -ContentType "application/json" `
        -Body $workerPayload `
        -TimeoutSec 15

    Write-Host "Worker response:" $resp.StatusCode
} catch {
    Write-Host "Worker test failed"
    Write-Host $_
}

Write-Host ""
Write-Host "---- Testing Backend endpoint ----"

$backendPayload = @{
    source             = "tradingview"
    namespace          = "pipeline_test"
    symbol             = "BTCUSDT.P"
    chart_tf           = "1"
    batch_id           = "pipeline-backend-$runId"
    batch_trigger_side = "buy"
    batch_size         = 1
    batch_close_time   = $runId
    confirmed          = $true
    events             = @(
        @{
            event_id      = "pipeline-backend-event-$runId"
            event_time    = $runId
            side          = "buy"
            signal_type   = "continuation"
            signal_family = "momentum"
            price         = 70000
            confirmed     = $true
            micro         = @{}
            macro         = @{}
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $resp = Invoke-WebRequest `
        -Uri $backendBatchUrl `
        -Method POST `
        -ContentType "application/json" `
        -Body $backendPayload `
        -TimeoutSec 15

    Write-Host "Backend response:" $resp.StatusCode
} catch {
    Write-Host "Backend test failed"
    Write-Host $_
}

Write-Host ""
Write-Host "Pipeline test complete."
