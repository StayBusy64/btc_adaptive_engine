param(
    [string]$BackendBaseUrl,
    [string]$WorkerUrl,
    [switch]$AllowPartialSurface,
    [int]$PollAttempts = 5,
    [int]$PollDelaySeconds = 1
)

$ErrorActionPreference = "Stop"

# Resolve repo root from this script's location and load shared helpers.
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $root "tools\pipeline_common.ps1")

function Write-Section {
    param([string]$Message)

    Write-Host ""
    Write-Host $Message -ForegroundColor Cyan
}

function Test-BackendSurface {
    param([string]$BaseUrl)

    $requiredRoutes = @(
        @{ Route = "/webhooks/tradingview/batch"; Probe = "/webhooks/tradingview/batch"; ExpectedStatus = @(405) },
        @{ Route = "/webhooks/tradingview/cycle/run"; Probe = "/webhooks/tradingview/cycle/run"; ExpectedStatus = @(405) },
        @{ Route = "/webhooks/tradingview/events/recent"; Probe = "/webhooks/tradingview/events/recent?limit=1"; ExpectedStatus = @(200) },
        @{ Route = "/webhooks/tradingview/signal-journal/recent"; Probe = "/webhooks/tradingview/signal-journal/recent?limit=1"; ExpectedStatus = @(200) },
        @{ Route = "/webhooks/tradingview/signal-outcomes/run"; Probe = "/webhooks/tradingview/signal-outcomes/run"; ExpectedStatus = @(405) }
    )

    $health = Invoke-PipelineJsonRequest -Method GET -Uri ($BaseUrl + "/health") -TimeoutSec 5
    $routeStatuses = [ordered]@{}
    $missingRoutes = New-Object System.Collections.Generic.List[string]

    foreach ($routeCheck in $requiredRoutes) {
        $probeStatus = Get-PipelineRouteStatusCode -Uri ($BaseUrl + $routeCheck.Probe)
        $routeStatuses[$routeCheck.Route] = $probeStatus

        if ($probeStatus -notin $routeCheck.ExpectedStatus) {
            $missingRoutes.Add("$($routeCheck.Route) status=$probeStatus") | Out-Null
        }
    }

    return [pscustomobject]@{
        BaseUrl        = $BaseUrl
        Health         = $health
        RouteStatuses  = $routeStatuses
        MissingRoutes  = @($missingRoutes)
        HasFullSurface = ($missingRoutes.Count -eq 0)
    }
}

function Resolve-CanonicalBackendUrl {
    param([string]$PreferredBaseUrl)

    $cfg = Get-PipelineConfig

    $baseUrl = if (-not [string]::IsNullOrWhiteSpace($PreferredBaseUrl)) {
        $PreferredBaseUrl.TrimEnd("/")
    } else {
        $cfg.local_api_base
    }

    $surface = Test-BackendSurface -BaseUrl $baseUrl

    if (-not $AllowPartialSurface.IsPresent -and -not $surface.HasFullSurface) {
        $missingText = ($surface.MissingRoutes -join ", ")
        throw "Backend $baseUrl is reachable but missing required TradingView analytics routes: $missingText"
    }

    return [pscustomobject]@{
        BaseUrl = $baseUrl
        Health  = $surface.Health
        Surface = $surface
    }
}

function New-DiagnosticPayload {
    param([int64]$EpochMs)

    $batchId = "probe-BTCUSDT.P-$EpochMs-buy"
    $eventId = "$batchId-evt-1"

    return [pscustomobject]@{
        source             = "tradingview"
        namespace          = "diagnostic"
        symbol             = "BTCUSDT.P"
        chart_tf           = "1"
        batch_id           = $batchId
        batch_trigger_side = "buy"
        batch_size         = 1
        batch_close_time   = $EpochMs
        confirmed          = $true
        events             = @(
            [pscustomobject]@{
                event_id      = $eventId
                event_time    = $EpochMs
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
                    signal_name         = "diagnostic_long_entry"
                    open                = 71000.0
                    high                = 71200.0
                    low                 = 70900.0
                    close               = 71150.0
                    volume              = 100.0
                    bar_time            = $EpochMs
                    bar_open_time       = $EpochMs - 60000
                    bar_close_time      = $EpochMs
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
                    trend_direction      = "bull_trend"
                    regime_tag           = "balanced"
                    price_vs_trend       = "above_trend_ema"
                    momentum_regime      = "bullish_momentum"
                    volume_regime        = "normal_relative_volume"
                    candle_bias          = "bullish_candle"
                    wick_bias            = "lower_wick_dominant"
                    ema_bull_stack       = $true
                    ema_bear_stack       = $false
                    use_rsi_filter       = $true
                    rsi_long_threshold   = 52.0
                    rsi_short_threshold  = 48.0
                    fast_len             = 9
                    slow_len             = 21
                    trend_len            = 50
                    rsi_len              = 14
                    atr_len              = 14
                    vol_sma_len          = 20
                    slope_lookback       = 3
                    confirmed_bars_only  = $true
                }
            }
        )
    }
}

function Find-MatchingRow {
    param(
        [object[]]$Rows,
        [string]$Key,
        [string]$ExpectedValue
    )

    if ($null -eq $Rows) { return $null }

    foreach ($row in $Rows) {
        if ($null -ne $row -and $row.$Key -eq $ExpectedValue) {
            return $row
        }
    }

    return $null
}

function Test-ResponseContainsText {
    param([string]$Content, [string]$ExpectedText)

    if ([string]::IsNullOrWhiteSpace($Content) -or [string]::IsNullOrWhiteSpace($ExpectedText)) {
        return $false
    }

    return $Content.Contains($ExpectedText)
}

function Find-JournalRowInFile {
    param([string]$JournalPath, [string]$EventId)

    if (-not (Test-Path $JournalPath)) { return $null }

    foreach ($line in Get-Content $JournalPath) {
        if (-not [string]::IsNullOrWhiteSpace($line) -and $line.Contains($EventId)) {
            return $line
        }
    }

    return $null
}

Write-Host ""
Write-Host "============================================="
Write-Host "TRADINGVIEW PIPELINE TEST"
Write-Host "============================================="

$backend = Resolve-CanonicalBackendUrl -PreferredBaseUrl $BackendBaseUrl
$backendBase = $backend.BaseUrl

Write-Section "Backend health"
Write-Host "Backend:" $backendBase
Write-Host "Health Status:" $backend.Health.StatusCode
Write-Host "Full Analytics Surface:" $backend.Surface.HasFullSurface

$epochMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$payload = New-DiagnosticPayload -EpochMs $epochMs
$batchId = $payload.batch_id
$eventId = $payload.events[0].event_id

$ingressResponse = $null
$ingressMode = "direct-backend"

Write-Section "Sending diagnostic payload"

if (-not [string]::IsNullOrWhiteSpace($WorkerUrl)) {
    $ingressMode = "worker"
    Write-Host "Ingress:" $ingressMode
    Write-Host "Worker URL:" $WorkerUrl
    $ingressResponse = Invoke-PipelineJsonRequest -Method POST -Uri $WorkerUrl -Body $payload -TimeoutSec 20
} else {
    $signalKey = Get-PipelineSignalKey
    $directUrl = "$backendBase/webhooks/tradingview/batch?signal_key=$signalKey"
    Write-Host "Ingress:" $ingressMode
    Write-Host "Batch URL:" $directUrl
    $ingressResponse = Invoke-PipelineJsonRequest -Method POST -Uri $directUrl -Body $payload -TimeoutSec 20
}

Write-Host "Ingress Status:" $ingressResponse.StatusCode
if ($ingressResponse.Json) {
    Write-Host ($ingressResponse.Json | ConvertTo-Json -Depth 10)
} else {
    Write-Host $ingressResponse.Content
}

Write-Section "Running ingest cycle"
$cycle = Invoke-PipelineJsonRequest -Method POST -Uri "$backendBase/webhooks/tradingview/cycle/run" -TimeoutSec 20
Write-Host "Cycle Status:" $cycle.StatusCode
Write-Host ($cycle.Json | ConvertTo-Json -Depth 10)

$normalizedMatch = $null
$normalizedOutput = $null
$journalOutput = $null
$journalPath = Join-Path $root "data\state\outcomes\signal_journal.jsonl"
$backfillUnavailable = $false

for ($attempt = 1; $attempt -le $PollAttempts; $attempt++) {
    Start-Sleep -Seconds $PollDelaySeconds

    $events = Invoke-PipelineJsonRequest -Method GET -Uri "$backendBase/webhooks/tradingview/events/recent?limit=25&symbol=BTCUSDT.P" -TimeoutSec 15
    $normalizedMatch = Find-MatchingRow -Rows $events.Json.rows -Key "event_id" -ExpectedValue $eventId
    if ($null -ne $normalizedMatch) {
        $normalizedOutput = $normalizedMatch
        break
    }

    if (Test-ResponseContainsText -Content $events.Content -ExpectedText $eventId) {
        $normalizedOutput = $events.Content
        break
    }
}

Write-Section "Normalized event check"
if ($null -eq $normalizedOutput) {
    throw "Normalized event $eventId was not found after $PollAttempts attempts."
}
if ($null -ne $normalizedMatch) {
    Write-Host ($normalizedMatch | ConvertTo-Json -Depth 10)
} else {
    Write-Host $normalizedOutput
}

for ($attempt = 1; $attempt -le $PollAttempts; $attempt++) {
    Start-Sleep -Seconds $PollDelaySeconds

    $journalOutput = Find-JournalRowInFile -JournalPath $journalPath -EventId $eventId
    if ($null -ne $journalOutput) { break }
}

$usedBackfill = $false
if ($null -eq $journalOutput) {
    Write-Host "Journal row not found after cycle. Running signal-outcomes backfill check..." -ForegroundColor Yellow
    try {
        $backfill = Invoke-PipelineJsonRequest -Method POST -Uri "$backendBase/webhooks/tradingview/signal-outcomes/run" -TimeoutSec 20
        $usedBackfill = $true
        if ($backfill.Json) {
            Write-Host ($backfill.Json | ConvertTo-Json -Depth 10)
        } else {
            Write-Host $backfill.Content
        }

        for ($attempt = 1; $attempt -le $PollAttempts; $attempt++) {
            Start-Sleep -Seconds $PollDelaySeconds

            $journalOutput = Find-JournalRowInFile -JournalPath $journalPath -EventId $eventId
            if ($null -ne $journalOutput) { break }
        }
    } catch {
        $backfillUnavailable = $true
        Write-Host "Backfill route unavailable on $backendBase. This runtime is missing the full TradingView analytics surface." -ForegroundColor Yellow
    }
}

Write-Section "Signal journal check"
if ($null -eq $journalOutput) {
    if ($backfillUnavailable) {
        throw "Signal journal row $eventId was not found, and $backendBase does not expose /webhooks/tradingview/signal-outcomes/run. Ensure the backend has the full TradingView analytics surface deployed."
    }
    throw "Signal journal row $eventId was not found. Normalization succeeded, but analytics storage did not update. Check that the ingest cycle ran and signal-outcomes processing completed."
}
try {
    $parsedJournalRow = $journalOutput | ConvertFrom-Json -Depth 20
    Write-Host ($parsedJournalRow | ConvertTo-Json -Depth 10)
} catch {
    Write-Host $journalOutput
}

Write-Section "Result"
Write-Host "Batch ID:" $batchId
Write-Host "Event ID:" $eventId
Write-Host "Backend:" $backendBase
Write-Host "Ingress Mode:" $ingressMode
Write-Host "Normalized Event:" "PASS"
Write-Host "Signal Journal:" "PASS"
Write-Host "Journal Backfill Used:" $usedBackfill

Write-Host ""
Write-Host "============================================="
Write-Host "PIPELINE TEST COMPLETE"
Write-Host "============================================="
