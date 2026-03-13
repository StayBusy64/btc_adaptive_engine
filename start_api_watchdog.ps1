param(
    [int]$RestartDelaySeconds = 5,
    [int]$HealthProbeAttempts = 20,
    [int]$HealthProbeIntervalSeconds = 1,
    [int]$HealthyHeartbeatLogIntervalSeconds = 60,
    [string]$BindAddress = "127.0.0.1",
    [int]$Port = 8000,
    [string]$HealthUrl = "",
    [switch]$Reload,
    [switch]$StopOnCleanExit
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

$logDir = Join-Path $root "data\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath = Join-Path $logDir "api_watchdog.log"

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

if ([string]::IsNullOrWhiteSpace($HealthUrl)) {
    $HealthUrl = "http://127.0.0.1:$Port/health"
}

function Write-WatchdogLog {
    param([string]$Message)

    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $line = "[$timestamp] $Message"
    Write-Host $line
    Add-Content -Path $logPath -Value $line
}

function Test-ApiHealthy {
    param([string]$Url)

    try {
        $response = Invoke-WebRequest -Uri $Url -TimeoutSec 3
        return ($response.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Test-ProcessRunning {
    param([System.Diagnostics.Process]$Process)

    if (-not $Process) {
        return $false
    }

    try {
        return -not $Process.HasExited
    } catch {
        return $false
    }
}

function Get-ProcessExitCodeSafe {
    param([System.Diagnostics.Process]$Process)

    if (-not $Process) {
        return $null
    }

    try {
        if ($Process.HasExited) {
            return $Process.ExitCode
        }
    } catch {
    }

    return $null
}

$mutexName = "Global\BTCAdaptiveEngineApiWatchdog"
$createdNew = $false
$script:WatchdogMutex = $null

try {
    $script:WatchdogMutex = New-Object System.Threading.Mutex($true, $mutexName, [ref]$createdNew)
} catch {
    throw "Unable to acquire watchdog lock '$mutexName': $($_.Exception.Message)"
}

if (-not $createdNew) {
    Write-WatchdogLog "Another watchdog instance is already running; exiting this instance"
    if ($script:WatchdogMutex) {
        $script:WatchdogMutex.Dispose()
        $script:WatchdogMutex = $null
    }
    return
}

Write-WatchdogLog "Starting API watchdog"
Write-WatchdogLog "Root: $root"
Write-WatchdogLog "Python: $pythonExe"
Write-WatchdogLog "Health URL: $HealthUrl"
Write-WatchdogLog "TRADINGVIEW_INGEST_SIGNAL_KEY loaded: $([bool]$env:TRADINGVIEW_INGEST_SIGNAL_KEY)"
Write-WatchdogLog "Healthy heartbeat log interval: $HealthyHeartbeatLogIntervalSeconds seconds"

$arguments = @(
    "-m",
    "uvicorn",
    "backend.api_server:app",
    "--host",
    $BindAddress,
    "--port",
    "$Port"
)

if ($Reload) {
    $arguments += "--reload"
}

$attempt = 0
$script:CurrentApiProcess = $null
$lastHealthyHeartbeatLogAt = $null

try {
    while ($true) {
        if (Test-ApiHealthy -Url $HealthUrl) {
            $shouldLogHealthyHeartbeat = $false
            if (-not $lastHealthyHeartbeatLogAt) {
                $shouldLogHealthyHeartbeat = $true
            } elseif (((Get-Date) - $lastHealthyHeartbeatLogAt).TotalSeconds -ge $HealthyHeartbeatLogIntervalSeconds) {
                $shouldLogHealthyHeartbeat = $true
            }

            if ($shouldLogHealthyHeartbeat) {
                Write-WatchdogLog "Health endpoint already responding; monitoring in-place"
                $lastHealthyHeartbeatLogAt = Get-Date
            }

            Start-Sleep -Seconds $RestartDelaySeconds
            continue
        }

        $lastHealthyHeartbeatLogAt = $null

        $attempt += 1
        $apiProcess = $null

        try {
            Write-WatchdogLog "Launch attempt $attempt"
            $apiProcess = Start-Process -FilePath $pythonExe -ArgumentList $arguments -WorkingDirectory $root -PassThru
            $script:CurrentApiProcess = $apiProcess
            Write-WatchdogLog "Started PID $($apiProcess.Id)"

            $healthy = $false
            for ($i = 1; $i -le $HealthProbeAttempts; $i++) {
                Start-Sleep -Seconds $HealthProbeIntervalSeconds

                if (-not (Test-ProcessRunning -Process $apiProcess)) {
                    Write-WatchdogLog "Process exited before health probe succeeded"
                    break
                }

                try {
                    if (Test-ApiHealthy -Url $HealthUrl) {
                        Write-WatchdogLog "Health probe succeeded on check $i"
                        $healthy = $true
                        break
                    }
                } catch {
                    # Ignore transient startup probe failures.
                }
            }

            if (-not $healthy -and (Test-ProcessRunning -Process $apiProcess)) {
                Write-WatchdogLog "Health probe did not reach 200 before timeout window"
            }

            while (Test-ProcessRunning -Process $apiProcess) {
                Start-Sleep -Milliseconds 300
            }

            $exitCode = Get-ProcessExitCodeSafe -Process $apiProcess
            if ($null -eq $exitCode) {
                Write-WatchdogLog "Process exited before exit code could be read"
                $exitCode = 1
            }
            Write-WatchdogLog "Process exited with code $exitCode"
            $script:CurrentApiProcess = $null

            if ($StopOnCleanExit -and $exitCode -eq 0) {
                Write-WatchdogLog "StopOnCleanExit enabled and process exited cleanly; watchdog stopping"
                break
            }
        } catch {
            Write-WatchdogLog "Watchdog loop error: $($_.Exception.Message)"
        } finally {
            if (Test-ProcessRunning -Process $apiProcess) {
                try {
                    Stop-Process -Id $apiProcess.Id -Force -ErrorAction SilentlyContinue
                    Write-WatchdogLog "Stopped lingering process PID $($apiProcess.Id)"
                } catch {
                }
                $script:CurrentApiProcess = $null
            }
        }

        Write-WatchdogLog "Restarting in $RestartDelaySeconds seconds"
        Start-Sleep -Seconds $RestartDelaySeconds
    }
} finally {
    if (Test-ProcessRunning -Process $script:CurrentApiProcess) {
        try {
            Stop-Process -Id $script:CurrentApiProcess.Id -Force -ErrorAction SilentlyContinue
            Write-WatchdogLog "Stopped active process PID $($script:CurrentApiProcess.Id) during watchdog shutdown"
        } catch {
        }
        $script:CurrentApiProcess = $null
    }

    if ($script:WatchdogMutex) {
        try {
            $script:WatchdogMutex.ReleaseMutex() | Out-Null
        } catch {
        }
        $script:WatchdogMutex.Dispose()
        $script:WatchdogMutex = $null
    }
}

Write-WatchdogLog "API watchdog terminated"
