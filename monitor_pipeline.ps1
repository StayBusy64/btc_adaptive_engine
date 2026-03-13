param(
    [string]$WorkerName = "tv-webhook",
    [double]$SamplingRate = 0.99,
    [string]$WorkerProjectPath = "",
    [string]$ReceiptLogPath = "",
    [switch]$IncludeHealthMonitor,
    [int]$HealthIntervalSeconds = 20,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if ($SamplingRate -le 0 -or $SamplingRate -ge 1) {
    throw "SamplingRate must be greater than 0 and less than 1. Example: 0.99"
}

if ($HealthIntervalSeconds -lt 5) {
    throw "HealthIntervalSeconds must be at least 5"
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$desktopRoot = Split-Path -Parent $scriptRoot

if ([string]::IsNullOrWhiteSpace($WorkerProjectPath)) {
    $WorkerProjectPath = Join-Path $desktopRoot "tv-webhook-worker"
}

if ([string]::IsNullOrWhiteSpace($ReceiptLogPath)) {
    $ReceiptLogPath = Join-Path $scriptRoot "data\logs\tv_ingest_receipts.jsonl"
}

if (-not (Test-Path $WorkerProjectPath)) {
    throw "Worker project path not found: $WorkerProjectPath"
}

$receiptDir = Split-Path -Parent $ReceiptLogPath
New-Item -ItemType Directory -Force -Path $receiptDir | Out-Null
if (-not (Test-Path $ReceiptLogPath)) {
    New-Item -ItemType File -Force -Path $ReceiptLogPath | Out-Null
}

function Quote-Single {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Start-MonitorWindow {
    param(
        [string]$Title,
        [string]$Command
    )

    $titleLiteral = Quote-Single -Value $Title
    $fullCommand = '$host.UI.RawUI.WindowTitle = ' + $titleLiteral + '; ' + $Command

    if ($DryRun) {
        Write-Host "[DryRun] $fullCommand"
        return $null
    }

    return Start-Process -FilePath "powershell.exe" -ArgumentList @(
        "-NoLogo",
        "-NoExit",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        $fullCommand
    ) -PassThru
}

$workerPathLiteral = Quote-Single -Value $WorkerProjectPath
$receiptPathLiteral = Quote-Single -Value $ReceiptLogPath

$workerCommand = "& { Set-Location $workerPathLiteral; npx wrangler tail $WorkerName --format pretty --sampling-rate $SamplingRate }"
$receiptCommand = "& { Get-Content -Path $receiptPathLiteral -Tail 0 -Wait }"

Write-Host "=== Pipeline Monitor Launcher ==="
Write-Host "Worker project path: $WorkerProjectPath"
Write-Host "Receipt log path: $ReceiptLogPath"
Write-Host "Worker sampling rate: $SamplingRate"
Write-Host ""

$workerWindow = Start-MonitorWindow -Title "Pipeline Monitor - Worker" -Command $workerCommand
$receiptWindow = Start-MonitorWindow -Title "Pipeline Monitor - Receipts" -Command $receiptCommand

if (-not $DryRun) {
    if ($workerWindow) {
        Write-Host "Worker monitor started (PID $($workerWindow.Id))"
    }
    if ($receiptWindow) {
        Write-Host "Receipt monitor started (PID $($receiptWindow.Id))"
    }
}

Write-Host ""
Write-Host "Fire your TradingView alert now."

if (-not $IncludeHealthMonitor) {
    Write-Host "Tip: Add -IncludeHealthMonitor to keep API health visible in this terminal."
    return
}

Write-Host ""
Write-Host "=== Health Monitor (Ctrl+C to stop) ==="

while ($true) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

    $local = "DOWN"
    $public = "DOWN"

    try {
        $localResp = Invoke-WebRequest -Uri "http://127.0.0.1:8000/health" -TimeoutSec 5
        $local = "HTTP $($localResp.StatusCode)"
    } catch {
    }

    try {
        $publicResp = Invoke-WebRequest -Uri "https://api.dopedreamspnl.com/health" -TimeoutSec 8
        $public = "HTTP $($publicResp.StatusCode)"
    } catch {
    }

    Write-Host "[$timestamp] Local API: $local | Public API: $public"
    Start-Sleep -Seconds $HealthIntervalSeconds
}
