# start_backend_and_tunnel.ps1
# Single launcher that boots the FastAPI backend and Cloudflare Tunnel together.
# Stops both when you press Ctrl+C.
#
# Usage:
#   .\start_backend_and_tunnel.ps1
#   .\start_backend_and_tunnel.ps1 -Reload              # uvicorn --reload for dev
#   .\start_backend_and_tunnel.ps1 -Port 9000            # custom port

param(
    [string]$BindAddress = "127.0.0.1",
    [int]$Port           = 8000,
    [string]$TunnelName  = "btc-adaptive-engine",
    [string]$TunnelConfig = "",
    [switch]$Reload
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# ===== LOAD SIGNAL KEY =====
$signalKeyPath = Join-Path $root "data\tv_ingest\signal_key.txt"
if (Test-Path $signalKeyPath) {
    $signalKey = (Get-Content $signalKeyPath -TotalCount 1).Trim()
    if (-not [string]::IsNullOrWhiteSpace($signalKey)) {
        $env:TRADINGVIEW_INGEST_SIGNAL_KEY = $signalKey
        $env:TV_SIGNAL_KEY = $signalKey
        $env:SIGNAL_WEBHOOK_KEY = $signalKey
    }
}

# ===== RESOLVE PYTHON =====
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

# ===== RESOLVE CLOUDFLARED =====
$cloudflaredExe = $null
$cloudflaredCmd = Get-Command cloudflared -ErrorAction SilentlyContinue
if ($cloudflaredCmd) {
    $cloudflaredExe = $cloudflaredCmd.Source
}

if (-not $cloudflaredExe) {
    $searchPaths = @(
        "C:\Program Files (x86)\cloudflared\cloudflared.exe",
        "C:\Program Files\cloudflared\cloudflared.exe",
        "$env:LOCALAPPDATA\Microsoft\WinGet\Links\cloudflared.exe",
        "C:\Cloudflared\bin\cloudflared.exe"
    )
    foreach ($p in $searchPaths) {
        if (Test-Path $p) {
            $cloudflaredExe = $p
            break
        }
    }
}

if (-not $cloudflaredExe) {
    throw "cloudflared.exe not found. Install cloudflared or run setup-cloudflare-tunnel.ps1 first."
}

# ===== RESOLVE TUNNEL CONFIG =====
if ([string]::IsNullOrWhiteSpace($TunnelConfig)) {
    $userCfDir = Join-Path $env:USERPROFILE ".cloudflared"
    $candidate = Join-Path $userCfDir "config.yml"
    if (Test-Path $candidate) {
        $TunnelConfig = $candidate
    }
}

if ([string]::IsNullOrWhiteSpace($TunnelConfig) -or -not (Test-Path $TunnelConfig)) {
    throw "Tunnel config not found. Run setup-cloudflare-tunnel.ps1 first, or pass -TunnelConfig <path>."
}

# ===== LOG DIRECTORY =====
$logDir = Join-Path $root "data\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# ===== BANNER =====
Write-Host "==========================================="
Write-Host "  BTC Adaptive Engine — Combined Launcher"
Write-Host "==========================================="
Write-Host "Working directory : $root"
Write-Host "Python            : $pythonExe"
Write-Host "cloudflared       : $cloudflaredExe"
Write-Host "Tunnel config     : $TunnelConfig"
Write-Host "Bind              : ${BindAddress}:${Port}"
Write-Host "Signal key loaded : $([bool]$env:TRADINGVIEW_INGEST_SIGNAL_KEY)"
Write-Host ""

$env:PYTHONPATH = $root

# ===== START BACKEND =====
$uvicornArgs = @(
    "-m", "uvicorn",
    "backend.api_server:app",
    "--host", $BindAddress,
    "--port", "$Port"
)

if ($Reload) {
    $uvicornArgs += "--reload"
}

Write-Host "Starting FastAPI backend on ${BindAddress}:${Port} ..."
$backendProcess = Start-Process -FilePath $pythonExe `
    -ArgumentList $uvicornArgs `
    -WorkingDirectory $root `
    -PassThru

Write-Host "Backend PID: $($backendProcess.Id)"

# ===== START TUNNEL =====
$tunnelArgs = @("--config", $TunnelConfig, "tunnel", "run")

Write-Host "Starting Cloudflare Tunnel ($TunnelName) ..."
$tunnelProcess = Start-Process -FilePath $cloudflaredExe `
    -ArgumentList $tunnelArgs `
    -WorkingDirectory $root `
    -PassThru

Write-Host "Tunnel  PID: $($tunnelProcess.Id)"

Write-Host ""
Write-Host "Both services running. Press Ctrl+C to stop."
Write-Host ""

# ===== WAIT / CLEANUP =====
function Stop-Both {
    foreach ($proc in @($backendProcess, $tunnelProcess)) {
        if ($proc -and -not $proc.HasExited) {
            try {
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                Write-Host "Stopped PID $($proc.Id)"
            } catch {}
        }
    }
}

try {
    while ($true) {
        $backendAlive = $backendProcess -and -not $backendProcess.HasExited
        $tunnelAlive  = $tunnelProcess -and -not $tunnelProcess.HasExited

        if (-not $backendAlive -and -not $tunnelAlive) {
            Write-Host "Both processes have exited."
            break
        }

        if (-not $backendAlive) {
            Write-Host "Backend process exited unexpectedly. Stopping tunnel."
            Stop-Both
            break
        }

        if (-not $tunnelAlive) {
            Write-Host "Tunnel process exited unexpectedly. Stopping backend."
            Stop-Both
            break
        }

        Start-Sleep -Seconds 2
    }
} finally {
    Stop-Both
}

Write-Host "Combined launcher terminated."
