param(
    [string]$WorkerBaseUrl = "",
    [string]$PublicApiHealthUrl = "",
    [string]$LocalApiHealthUrl = "",
    [string]$TvWebhookSecret = "",
    [string]$Symbol = "BTCUSDT.P"
)

$ErrorActionPreference = "Stop"

# Load shared helpers and canonical config.
$_scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $_scriptRoot "tools\pipeline_common.ps1")

$_cfg = Get-PipelineConfig

if ([string]::IsNullOrWhiteSpace($WorkerBaseUrl))     { $WorkerBaseUrl     = $_cfg.worker_base }
if ([string]::IsNullOrWhiteSpace($PublicApiHealthUrl)) { $PublicApiHealthUrl = $_cfg.backend_health }
if ([string]::IsNullOrWhiteSpace($LocalApiHealthUrl))  { $LocalApiHealthUrl  = $_cfg.local_health }

function Write-Check {
    param(
        [string]$Name,
        [string]$Result,
        [ConsoleColor]$Color = [ConsoleColor]::Gray
    )

    Write-Host ("{0,-32} {1}" -f $Name, $Result) -ForegroundColor $Color
}

Write-Host "=== Pipeline Status Check ==="
Write-Host "Timestamp: $(Get-Date -Format s)"
Write-Host ""

# 1) Scheduled tasks
$taskNames = @(
    "BTC Adaptive Engine API Watchdog",
    "BTC Cloudflared Tunnel"
)

foreach ($taskName in $taskNames) {
    try {
        $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
        Write-Check -Name "Task $taskName" -Result $task.State -Color Green
    } catch {
        Write-Check -Name "Task $taskName" -Result "Missing" -Color Yellow
    }
}

# 2) Local API health
try {
    $localResp = Invoke-WebRequest -Uri $LocalApiHealthUrl -TimeoutSec 8
    Write-Check -Name "Local API" -Result ("HTTP {0}" -f $localResp.StatusCode) -Color Green
} catch {
    Write-Check -Name "Local API" -Result "Down" -Color Red
}

# 3) Public API health
try {
    $publicResp = Invoke-WebRequest -Uri $PublicApiHealthUrl -TimeoutSec 12
    Write-Check -Name "Public API" -Result ("HTTP {0}" -f $publicResp.StatusCode) -Color Green
} catch {
    if ($_.Exception.Response) {
        Write-Check -Name "Public API" -Result ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode) -Color Red
    } else {
        Write-Check -Name "Public API" -Result "Down" -Color Red
    }
}

# 4) Tunnel connector state (optional)
$cloudflaredCmd = Get-Command cloudflared -ErrorAction SilentlyContinue
if ($cloudflaredCmd) {
    try {
        $info = & $cloudflaredCmd.Source tunnel info btc-adaptive-engine 2>$null | Out-String
        if ($info -match "does not have any active connection") {
            Write-Check -Name "Tunnel connector" -Result "No active connection" -Color Yellow
        } elseif ($info -match "CONNECTOR ID") {
            Write-Check -Name "Tunnel connector" -Result "Active" -Color Green
        } else {
            Write-Check -Name "Tunnel connector" -Result "Unknown" -Color Yellow
        }
    } catch {
        Write-Check -Name "Tunnel connector" -Result "Check failed" -Color Yellow
    }
} else {
    Write-Check -Name "Tunnel connector" -Result "cloudflared not on PATH" -Color Yellow
}

# 5) Worker forward probe (optional secret)
if ([string]::IsNullOrWhiteSpace($TvWebhookSecret)) {
    $TvWebhookSecret = $env:TV_WEBHOOK_SECRET
}

if ([string]::IsNullOrWhiteSpace($TvWebhookSecret)) {
    Write-Check -Name "Worker probe" -Result "Skipped (set -TvWebhookSecret or TV_WEBHOOK_SECRET)" -Color Yellow
} else {
    $runId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $secretParam = $_cfg.worker_secret_param
    $workerProbeUrl = "$WorkerBaseUrl/?$secretParam=$TvWebhookSecret"

    $payloadObj = @{
        source = "tradingview"
        namespace = "status_check"
        symbol = $Symbol
        chart_tf = "1"
        batch_id = "status-check-$runId"
        batch_trigger_side = "buy"
        batch_size = 1
        batch_close_time = $runId
        confirmed = $true
        events = @(
            @{
                event_id = "status-check-event-$runId"
            event_time = $runId
                side = "buy"
                signal_type = "continuation"
                signal_family = "momentum"
                price = 70000
                confirmed = $true
                micro = @{}
                macro = @{}
            }
        )
    }

    $probeBody = $payloadObj | ConvertTo-Json -Depth 6

    try {
        $workerResp = Invoke-WebRequest -Uri $workerProbeUrl -Method POST -ContentType "application/json" -Body $probeBody -TimeoutSec 15
        Write-Check -Name "Worker probe" -Result ("HTTP {0}" -f $workerResp.StatusCode) -Color Green
    } catch {
        if ($_.Exception.Response) {
            Write-Check -Name "Worker probe" -Result ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode) -Color Red
        } else {
            Write-Check -Name "Worker probe" -Result "Failed" -Color Red
        }
    }
}

Write-Host ""
Write-Host "Done."
