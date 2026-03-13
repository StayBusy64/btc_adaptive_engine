param(
    [switch]$Reload
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$signalKeyPath = Join-Path $root "data\tv_ingest\signal_key.txt"
if (Test-Path $signalKeyPath) {
    $signalKey = (Get-Content $signalKeyPath -TotalCount 1).Trim()
    if (-not [string]::IsNullOrWhiteSpace($signalKey)) {
        $env:TRADINGVIEW_INGEST_SIGNAL_KEY = $signalKey
        $env:TV_SIGNAL_KEY = $signalKey
        $env:SIGNAL_WEBHOOK_KEY = $signalKey
    }
}

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
Write-Host "Working directory: $root"
Write-Host "Python: $pythonExe"
Write-Host "TRADINGVIEW_INGEST_SIGNAL_KEY loaded:" ([bool]$env:TRADINGVIEW_INGEST_SIGNAL_KEY)

$env:PYTHONPATH = $root

$uvicornArgs = @(
    "backend.api_server:app"
    "--app-dir"
    $root
    "--host"
    "127.0.0.1"
    "--port"
    "8000"
)

if ($Reload.IsPresent) {
    $uvicornArgs += "--reload"
}

& $pythonExe -m uvicorn @uvicornArgs
