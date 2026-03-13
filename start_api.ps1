param(
    [switch]$Reload
)

$ErrorActionPreference = "Stop"

# Resolve repo root from the script's own path so this works regardless of
# the working directory of the calling process.
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($root)) {
    $root = $PSScriptRoot
}
if ([string]::IsNullOrWhiteSpace($root)) {
    $root = (Get-Location).Path
}
Set-Location $root

# ---- Log capture --------------------------------------------------------
$logDir = Join-Path $root "logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}
$logPath = Join-Path $logDir "api.log"
Start-Transcript -Path $logPath -Force | Out-Null

# ---- Signal key ---------------------------------------------------------
$signalKeyPath = Join-Path $root "data\tv_ingest\signal_key.txt"
if (Test-Path $signalKeyPath) {
    $signalKey = (Get-Content $signalKeyPath -TotalCount 1).Trim()
    if (-not [string]::IsNullOrWhiteSpace($signalKey)) {
        $env:TRADINGVIEW_INGEST_SIGNAL_KEY = $signalKey
        $env:TV_SIGNAL_KEY = $signalKey
        $env:SIGNAL_WEBHOOK_KEY = $signalKey
    }
}

# ---- Python executable --------------------------------------------------
$venvPython = "C:\Users\Stayb\OneDrive\Desktop\.venv\Scripts\python.exe"
$pythonExe = $null

if (Test-Path $venvPython) {
    $pythonExe = $venvPython
} else {
    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCmd) {
        $pythonExe = $pythonCmd.Source
    }
}

if (-not $pythonExe) {
    throw "Python executable not found. Activate your environment or install Python."
}

Write-Host "Starting BTC Adaptive Engine API..."
Write-Host "Working directory : $root"
Write-Host "Python            : $pythonExe"
Write-Host "Log file          : $logPath"
Write-Host "TRADINGVIEW_INGEST_SIGNAL_KEY loaded:" ([bool]$env:TRADINGVIEW_INGEST_SIGNAL_KEY)

$env:PYTHONPATH = $root

# ---- ASGI target: backend.api_server:app  host 127.0.0.1  port 8000 ---
$uvicornArgs = @(
    "backend.api_server:app"
    "--host"
    "127.0.0.1"
    "--port"
    "8000"
)

if ($Reload.IsPresent) {
    $uvicornArgs += "--reload"
}

& $pythonExe -m uvicorn @uvicornArgs
