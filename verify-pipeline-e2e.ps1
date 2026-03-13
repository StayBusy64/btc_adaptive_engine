# verify-pipeline-e2e.ps1
#
# Canonical end-to-end pipeline verifier.
#
# Sends one TradingView-style batch through the canonical Worker URL from
# pipeline_config.json and proves the full pipeline progression:
#
#   Worker accepted
#   Backend route reachable
#   Local API reachable
#   Batch persisted (processing_status JSON)
#   Batch processed (normalized event JSON + status == processed)
#   Journal advanced (secondary)
#
# All URLs come from pipeline_config.json via tools/pipeline_common.ps1.
# No alternate endpoint discovery.  No port fallbacks.
#
# Usage:
#   .\verify-pipeline-e2e.ps1
#
# Required:  $env:TV_WEBHOOK_SECRET must be set.
# Optional:  pass -WorkerSecret directly to override the env var.

param(
    [string]$WorkerSecret = "",
    [int]$PollAttempts    = 10,
    [int]$PollDelaySeconds = 3
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $repoRoot "tools\pipeline_common.ps1")

$cfg = Get-PipelineConfig

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

$checks = [ordered]@{
    "Worker accepted"      = $null
    "Backend reachable"    = $null
    "Local API reachable"  = $null
    "Batch persisted"      = $null
    "Batch processed"      = $null
    "Journal advanced"     = $null
}

function Set-Check {
    param([string]$Name, [bool]$Value, [string]$Detail = "")
    $script:checks[$Name] = [pscustomobject]@{ Pass = $Value; Detail = $Detail }
}

function Write-Section { param([string]$Message); Write-Host ""; Write-Host $Message -ForegroundColor Cyan }

# ---------------------------------------------------------------------------
# Build canonical Worker URL from config
# ---------------------------------------------------------------------------

$workerUrl = Get-PipelineWorkerUrl -WorkerSecret $WorkerSecret

# ---------------------------------------------------------------------------
# Build payload
# ---------------------------------------------------------------------------

$epochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$batchId = "e2e-BTCUSDT.P-$epochMs-buy"
$eventId = "$batchId-evt-1"

$payload = [pscustomobject]@{
    source             = "tradingview"
    namespace          = "e2e_verify"
    symbol             = "BTCUSDT.P"
    chart_tf           = "1"
    batch_id           = $batchId
    batch_trigger_side = "buy"
    batch_size         = 1
    batch_close_time   = $epochMs
    confirmed          = $true
    events             = @(
        [pscustomobject]@{
            event_id      = $eventId
            event_time    = $epochMs
            side          = "buy"
            signal_type   = "continuation"
            signal_family = "momentum"
            price         = 71150.0
            confirmed     = $true
            micro         = [ordered]@{
                ticker              = "BTCUSDT.P"
                tickerid            = "BTCC:BTCUSDT.P"
                exchange            = "BTCC"
                base_currency       = "BTC"
                quote_currency      = "USDT"
                timeframe           = "1"
                strategy_id         = "bridge_signal_sender_v2"
                signal_name         = "e2e_long_entry"
                open                = 71000.0
                high                = 71200.0
                low                 = 70900.0
                close               = 71150.0
                volume              = 100.0
                bar_time            = $epochMs
                bar_open_time       = $epochMs - 60000
                bar_close_time      = $epochMs
                bar_index           = 999999
                fast_ema            = 71120.0
                slow_ema            = 71090.0
                trend_ema           = 70980.0
                rsi                 = 61.0
                atr                 = 102.0
                atr_pct             = 0.0014
                vol_sma             = 80.0
                rel_volume          = 1.25
                ema_spread          = 30.0
                ema_spread_abs      = 30.0
                ema_spread_bps      = 4.22
                fast_slope          = 18.0
                slow_slope          = 11.0
                dist_fast           = 30.0
                dist_slow           = 60.0
                dist_fast_bps       = 4.22
                dist_slow_bps       = 8.44
                candle_range        = 300.0
                candle_body         = 150.0
                upper_wick          = 50.0
                lower_wick          = 100.0
                body_to_range       = 0.5
                upper_wick_to_range = 0.1667
                lower_wick_to_range = 0.3333
                is_bull_body        = $true
                is_bear_body        = $false
                is_expansion_bar    = $false
                is_compression_bar  = $false
            }
            macro         = [ordered]@{
                trend_direction     = "bull_trend"
                regime_tag          = "balanced"
                price_vs_trend      = "above_trend_ema"
                momentum_regime     = "bullish_momentum"
                volume_regime       = "normal_relative_volume"
                candle_bias         = "bullish_candle"
                wick_bias           = "lower_wick_dominant"
                ema_bull_stack      = $true
                ema_bear_stack      = $false
                use_rsi_filter      = $true
                rsi_long_threshold  = 52.0
                rsi_short_threshold = 48.0
                fast_len            = 9
                slow_len            = 21
                trend_len           = 50
                rsi_len             = 14
                atr_len             = 14
                vol_sma_len         = 20
                slope_lookback      = 3
                confirmed_bars_only = $true
            }
        }
    )
}

# ---------------------------------------------------------------------------
# Step 1: Send to Worker
# ---------------------------------------------------------------------------

Write-Section "Step 1 — Send batch to Worker"
Write-Host "Worker URL:  $workerUrl"
Write-Host "Batch ID:    $batchId"
Write-Host "Event ID:    $eventId"

try {
    $workerResp = Invoke-PipelineJsonRequest -Method POST -Uri $workerUrl -Body $payload -TimeoutSec 20
    $workerOk = ($workerResp.StatusCode -ge 200 -and $workerResp.StatusCode -lt 300)
    Set-Check -Name "Worker accepted" -Value $workerOk -Detail "HTTP $($workerResp.StatusCode)"
    Write-Host ("Worker:      HTTP {0}" -f $workerResp.StatusCode)
} catch {
    Set-Check -Name "Worker accepted" -Value $false -Detail $_.Exception.Message
    Write-Host "Worker:      FAILED — $($_.Exception.Message)" -ForegroundColor Red
}

# ---------------------------------------------------------------------------
# Step 2: Backend batch route reachable (GET probe → 405)
# ---------------------------------------------------------------------------

Write-Section "Step 2 — Backend batch route reachable"
$backendBatchProbe = "$($cfg.backend_base)$($cfg.batch_path)"
try {
    $backendStatus = Get-PipelineRouteStatusCode -Uri $backendBatchProbe -TimeoutSec 8
    $backendOk = ($backendStatus -in @(200, 405, 422))
    Set-Check -Name "Backend reachable" -Value $backendOk -Detail "HTTP $backendStatus"
    Write-Host ("Backend:     HTTP {0}" -f $backendStatus)
} catch {
    Set-Check -Name "Backend reachable" -Value $false -Detail $_.Exception.Message
    Write-Host "Backend:     FAILED — $($_.Exception.Message)" -ForegroundColor Red
}

# ---------------------------------------------------------------------------
# Step 3: Local API reachable
# ---------------------------------------------------------------------------

Write-Section "Step 3 — Local API health"
try {
    $localResp = Invoke-WebRequest -Uri $cfg.local_health -TimeoutSec 8 -UseBasicParsing
    $localOk = ($localResp.StatusCode -eq 200)
    Set-Check -Name "Local API reachable" -Value $localOk -Detail "HTTP $($localResp.StatusCode)"
    Write-Host ("Local API:   HTTP {0}" -f $localResp.StatusCode)
} catch {
    Set-Check -Name "Local API reachable" -Value $false -Detail "Down"
    Write-Host "Local API:   DOWN" -ForegroundColor Red
}

# ---------------------------------------------------------------------------
# Step 4: Trigger ingest cycle then poll for batch persisted
# ---------------------------------------------------------------------------

Write-Section "Step 4 — Trigger ingest cycle and poll for persistence"

$localApiBase = $cfg.local_api_base

try {
    $cycleResp = Invoke-PipelineJsonRequest -Method POST -Uri "$localApiBase/webhooks/tradingview/cycle/run" -TimeoutSec 20
    Write-Host ("Cycle trigger: HTTP {0}" -f $cycleResp.StatusCode)
} catch {
    Write-Host "Cycle trigger: FAILED (local API may be down) — $($_.Exception.Message)" -ForegroundColor Yellow
}

$statusDir   = Join-Path $repoRoot "data\state\normalized\processing_status"
$safeId      = $batchId -replace "[^a-zA-Z0-9\-_]", "_"
$statusFile  = Join-Path $statusDir "$safeId.json"

$batchPersisted = $false
for ($i = 1; $i -le $PollAttempts; $i++) {
    Start-Sleep -Seconds $PollDelaySeconds
    if (Test-Path $statusFile) {
        $batchPersisted = $true
        Write-Host "processing_status found: $statusFile"
        break
    }
    Write-Host "  poll $i/$PollAttempts — waiting for $statusFile"
}

$persistedDetail = if ($batchPersisted) { $statusFile } else { "not found after $($PollAttempts * $PollDelaySeconds)s" }
Set-Check -Name "Batch persisted" -Value $batchPersisted -Detail $persistedDetail

# ---------------------------------------------------------------------------
# Step 5: Poll for batch processed (status == processed + event JSON)
# ---------------------------------------------------------------------------

Write-Section "Step 5 — Poll for batch processed"

$signalEventsDir = Join-Path $repoRoot "data\state\normalized\signal_events"
$safeEventId     = $eventId -replace "[^a-zA-Z0-9\-_]", "_"
$eventFile       = Join-Path $signalEventsDir "$safeEventId.json"

$batchProcessed = $false
for ($i = 1; $i -le $PollAttempts; $i++) {
    Start-Sleep -Seconds $PollDelaySeconds

    $statusOk = $false
    $eventOk  = $false

    if (Test-Path $statusFile) {
        try {
            $statusData = Get-Content $statusFile -Raw | ConvertFrom-Json -Depth 10
            $statusOk   = ($statusData.status -eq "processed")
        } catch {}
    }

    if (Test-Path $eventFile) { $eventOk = $true }

    # Also check via the local events API as an alternative signal.
    if (-not $statusOk -or -not $eventOk) {
        try {
            $eventsResp = Invoke-PipelineJsonRequest -Method GET -Uri "$localApiBase/webhooks/tradingview/events/recent?limit=25&symbol=BTCUSDT.P" -TimeoutSec 10
            if ($eventsResp.Content -and $eventsResp.Content.Contains($eventId)) {
                $eventOk = $true
            }
        } catch {}
    }

    if ($statusOk -and $eventOk) {
        $batchProcessed = $true
        Write-Host "Batch processed: status=processed  event_json=$eventOk"
        break
    }

    Write-Host ("  poll {0}/{1} — status_ok={2}  event_file={3}" -f $i, $PollAttempts, $statusOk, $eventOk)
}

$processedDetail = if ($batchProcessed) { "status=processed + event found" } else { "not processed after $($PollAttempts * $PollDelaySeconds)s" }
Set-Check -Name "Batch processed" -Value $batchProcessed -Detail $processedDetail

# ---------------------------------------------------------------------------
# Step 6: Journal advanced (secondary)
# ---------------------------------------------------------------------------

Write-Section "Step 6 — Journal advanced (secondary check)"

$journalFile    = Join-Path $repoRoot "data\state\outcomes\signal_journal.jsonl"
$journalAdvanced = $false

for ($i = 1; $i -le $PollAttempts; $i++) {
    Start-Sleep -Seconds $PollDelaySeconds

    if ((Test-Path $journalFile) -and (Select-String -Path $journalFile -Pattern ([regex]::Escape($eventId)) -Quiet)) {
        $journalAdvanced = $true
        Write-Host "Journal entry found for $eventId"
        break
    }

    Write-Host "  poll $i/$PollAttempts — journal entry not yet present"
}

$journalDetail = if ($journalAdvanced) { "found in signal_journal.jsonl" } else { "not in journal after polling (secondary — non-blocking)" }
Set-Check -Name "Journal advanced" -Value $journalAdvanced -Detail $journalDetail

# ---------------------------------------------------------------------------
# Final report
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "============================================="
Write-Host "END-TO-END VERIFICATION REPORT"
Write-Host "============================================="
Write-Host ("Batch ID:  $batchId")
Write-Host ("Event ID:  $eventId")
Write-Host ("Time:      $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
Write-Host ""

$allPassed = $true

foreach ($key in $checks.Keys) {
    $entry = $checks[$key]
    if ($null -eq $entry) {
        Write-Host ("{0,-28} SKIPPED" -f $key) -ForegroundColor Yellow
        continue
    }

    $symbol = if ($entry.Pass) { "PASS" } else { "FAIL" }
    $color  = if ($entry.Pass) { [ConsoleColor]::Green } else { [ConsoleColor]::Red }

    # Journal failure is non-blocking (secondary signal only).
    if ($key -eq "Journal advanced" -and -not $entry.Pass) { $color = [ConsoleColor]::Yellow }

    $detail = if ($entry.Detail) { "  ($($entry.Detail))" } else { "" }
    Write-Host ("{0,-28} {1}{2}" -f $key, $symbol, $detail) -ForegroundColor $color

    # Journal is secondary — do not count as blocking failure.
    if (-not $entry.Pass -and $key -ne "Journal advanced") {
        $allPassed = $false
    }
}

Write-Host ""
if ($allPassed) {
    Write-Host "Overall: PASS — Worker → backend → local persistence → processed event" -ForegroundColor Green
} else {
    Write-Host "Overall: FAIL — one or more required checks did not pass" -ForegroundColor Red
}

Write-Host "============================================="
