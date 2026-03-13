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

$venvPython = $null
$pythonExe = $null

# 1) Activated virtual environment
if (-not [string]::IsNullOrWhiteSpace($env:VIRTUAL_ENV)) {
    $venvPython = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
}

# 2) Repo-local .venv
if (-not $venvPython -or -not (Test-Path $venvPython)) {
    $localVenv = Join-Path $root ".venv\Scripts\python.exe"
    if (Test-Path $localVenv) {
        $venvPython = $localVenv
    }
}

if ($venvPython -and (Test-Path $venvPython)) {
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
