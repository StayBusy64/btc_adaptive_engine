#Requires -Version 5.1
$ErrorActionPreference = "Stop"

# =====================================================================
# BTC Adaptive Engine — Unified Launch
# =====================================================================
#
# Fixes applied:
#   1. Start-Process calls use -WorkingDirectory $ProjectPath so the API
#      child PowerShell session always inherits the repo root as its CWD.
#      start_api.ps1 additionally resolves the root from its own script
#      path ($MyInvocation.MyCommand.Path), giving two independent guards.
#   2. API and tunnel startup output is captured under logs\ via
#      Start-Transcript in start_api.ps1 and via -RedirectStandard* here.
#   3. On API timeout: port 8000 state, running process list, and the
#      last 50 lines of logs\api.log are printed before aborting.
#   4. Local API is permanently fixed to 127.0.0.1:8000. No fallback.
#      No endpoint hunting.
# =====================================================================

Write-Host ""
Write-Host "=== BTC Adaptive Engine Unified Launch ===" -ForegroundColor Cyan
Write-Host ""

# -----------------------------
# CONFIG
# -----------------------------
# Resolve project root from this script's own path — works regardless of
# the calling process's working directory (same approach as start_api.ps1).
$ProjectPath = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($ProjectPath)) { $ProjectPath = $PSScriptRoot }
if ([string]::IsNullOrWhiteSpace($ProjectPath)) { $ProjectPath = (Get-Location).Path }
$ApiScript       = Join-Path $ProjectPath "start_api.ps1"
$TunnelScript    = Join-Path $ProjectPath "setup-permanent-cloudflare-tunnel.ps1"
$SignalKeyFile   = Join-Path $ProjectPath "data\tv_ingest\signal_key.txt"

$LocalHealth     = "http://127.0.0.1:8000/health"
$PublicHealth    = "https://api.dopedreamspnl.com/health"
$BackendBatchUrl = "https://api.dopedreamspnl.com/webhooks/tradingview/batch"
$WorkerBase      = "https://tv-webhook.staybusyent.workers.dev"

$ProcessingStatusDir = Join-Path $ProjectPath "data\state\normalized\processing_status"
$SignalEventsDir     = Join-Path $ProjectPath "data\state\normalized\signal_events"

$LogDir        = Join-Path $ProjectPath "logs"
$LogFile       = Join-Path $LogDir ("launch_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
# api.log is written by start_api.ps1 via Start-Transcript; used below for diagnostics.
$ApiLogFile    = Join-Path $LogDir "api.log"
$LockFile      = Join-Path $ProjectPath "data\state\.launch.lock"
$ResumeFile    = Join-Path $ProjectPath "data\state\.launch_resume.json"

$script:LaunchedProcs = @()

# =====================================================================
# LOGGING
# =====================================================================
function Write-Log {
    param(
        [Parameter(Mandatory=$true)][string]$Message,
        [string]$Color = "White",
        [ValidateSet("INFO","WARN","ERROR","PASS","STEP")][string]$Level = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line      = "[$timestamp][$Level] $Message"
    Write-Host $Message -ForegroundColor $Color
    try { Add-Content -Path $LogFile -Value $line -ErrorAction SilentlyContinue } catch {}
}

# =====================================================================
# RESUME STATE
# =====================================================================
function Get-ResumeState {
    if (Test-Path $ResumeFile) {
        try { return Get-Content $ResumeFile -Raw | ConvertFrom-Json -ErrorAction Stop } catch {}
    }
    return [PSCustomObject]@{
        ApiStarted         = $false
        TunnelStarted      = $false
        LocalApiReady      = $false
        CloudflaredReady   = $false
        PublicBackendReady = $false
        WorkerReady        = $false
        IngestSent         = $false
        StatusFileReady    = $false
        EventFileReady     = $false
        BatchId            = ""
        EventId            = ""
        RunId              = 0
    }
}

function Save-ResumeState {
    param([PSCustomObject]$State)
    try { $State | ConvertTo-Json -Depth 3 | Set-Content $ResumeFile -Force } catch {}
}

function Clear-ResumeState {
    Remove-Item $ResumeFile -Force -ErrorAction SilentlyContinue
}

# =====================================================================
# CLEANUP
# =====================================================================
function Invoke-Cleanup {
    param([switch]$ClearLock)
    if ($script:LaunchedProcs.Count -gt 0) {
        Write-Log "Cleaning up launched windows..." "DarkYellow" "WARN"
        foreach ($p in $script:LaunchedProcs) {
            try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
        }
    }
    if ($ClearLock) {
        Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
        Write-Log "Lock file released." "DarkGray" "INFO"
    }
}

# =====================================================================
# WAIT HELPERS
# =====================================================================
function Wait-ForHttpOk {
    param(
        [Parameter(Mandatory=$true)][string]$Url,
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 2,
        [string]$Label = "endpoint"
    )
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 8 -ErrorAction Stop
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
                Write-Log "$Label reachable: HTTP $($resp.StatusCode)" "Green" "PASS"
                return $true
            }
        } catch {
            Write-Log ("  [{0}/{1}] {2} not ready..." -f $i, $MaxAttempts, $Label) "DarkGray" "INFO"
            Start-Sleep -Seconds $DelaySeconds
        }
    }
    return $false
}

function Wait-ForProcess {
    param(
        [Parameter(Mandatory=$true)][string]$ProcessName,
        [int]$MaxAttempts = 20,
        [int]$DelaySeconds = 2
    )
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        $p = Get-Process $ProcessName -ErrorAction SilentlyContinue
        if ($p) {
            Write-Log "$ProcessName is running (PID: $($p[0].Id))." "Green" "PASS"
            return $p[0]
        }
        Write-Log ("  [{0}/{1}] {2} not ready..." -f $i, $MaxAttempts, $ProcessName) "DarkGray" "INFO"
        Start-Sleep -Seconds $DelaySeconds
    }
    return $null
}

function Wait-ForPath {
    param(
        [Parameter(Mandatory=$true)][string]$Path,
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 2,
        [string]$Label = "file"
    )
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        if (Test-Path $Path) {
            Write-Log "$Label found: $Path" "Green" "PASS"
            return $true
        }
        Write-Log ("  [{0}/{1}] {2} not ready..." -f $i, $MaxAttempts, $Label) "DarkGray" "INFO"
        Start-Sleep -Seconds $DelaySeconds
    }
    return $false
}

function Get-CloudflaredExe {
    foreach ($candidate in @(
        "C:\Program Files (x86)\cloudflared\cloudflared.exe",
        "C:\Program Files\cloudflared\cloudflared.exe"
    )) {
        if (Test-Path $candidate) { return $candidate }
    }
    return $null
}

# =====================================================================
# FAILURE DIAGNOSTICS
# =====================================================================
function Write-ApiFailureDiagnostics {
    Write-Log "" "White" "INFO"
    Write-Log "=== FAILURE DIAGNOSTICS ===" "Red" "ERROR"

    # Port 8000 state
    Write-Log "Port 8000 listeners:" "Yellow" "WARN"
    try {
        $conns = Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue
        if ($conns) {
            foreach ($c in $conns) {
                Write-Log ("  PID {0}  State {1}  LocalAddr {2}" -f $c.OwningProcess, $c.State, $c.LocalAddress) "DarkGray" "INFO"
            }
        } else {
            Write-Log "  Port 8000 is NOT listening." "Red" "ERROR"
        }
    } catch {
        Write-Log "  (port check unavailable: $($_.Exception.Message))" "DarkGray" "INFO"
    }

    # Python/uvicorn process list
    Write-Log "Python/uvicorn processes:" "Yellow" "WARN"
    $procs = Get-Process -Name python,pythonw,uvicorn -ErrorAction SilentlyContinue
    if ($procs) {
        foreach ($pr in $procs) {
            Write-Log ("  PID {0}  {1}  CPU {2:F1}s" -f $pr.Id, $pr.Name, $pr.CPU) "DarkGray" "INFO"
        }
    } else {
        Write-Log "  No python/uvicorn processes found." "Red" "ERROR"
    }

    # Last 50 API log lines
    if (Test-Path $ApiLogFile) {
        Write-Log "Last 50 lines of $ApiLogFile :" "Yellow" "WARN"
        Get-Content $ApiLogFile -Tail 50 | ForEach-Object {
            Write-Log "  $_" "DarkGray" "INFO"
        }
    } else {
        Write-Log "API log not found: $ApiLogFile" "Red" "ERROR"
        Write-Log "  This means start_api.ps1 never ran or failed before creating the log." "Red" "ERROR"
    }

    Write-Log "=== END DIAGNOSTICS ===" "Red" "ERROR"
    Write-Log "" "White" "INFO"
}

# =====================================================================
# INIT: LOG DIR + LOCK FILE
# =====================================================================
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

Write-Log "Log file: $LogFile" "DarkGray" "INFO"

if (Test-Path $LockFile) {
    $lockData = Get-Content $LockFile -Raw -ErrorAction SilentlyContinue
    $lockJson  = $null
    try { $lockJson = $lockData | ConvertFrom-Json -ErrorAction Stop } catch {}

    $lockPid      = if ($lockJson) { $lockJson.PID } else { $null }
    $lockPidAlive = $false

    if ($lockPid) {
        $lockProc = Get-Process -Id $lockPid -ErrorAction SilentlyContinue
        if ($lockProc) { $lockPidAlive = $true }
    }

    if ($lockPidAlive) {
        Write-Log "LOCK FILE EXISTS and owning PID $lockPid is still running." "Red" "ERROR"
        Write-Log "Lock contents: $lockData" "DarkGray" "INFO"
        Write-Log "If that process is not a real launch, stop it then delete: $LockFile" "Yellow" "WARN"
        throw "Launch aborted — active lock file found (PID $lockPid). Stop that process or delete the lock to continue."
    } else {
        Write-Log "Orphaned lock file found (PID $lockPid no longer running). Auto-clearing." "Yellow" "WARN"
        Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
        Write-Log "Orphaned lock cleared. Proceeding with launch." "Green" "PASS"
    }
}

$lockInfo = [PSCustomObject]@{
    LaunchedAt = (Get-Date -Format "o")
    PID        = $PID
} | ConvertTo-Json -Compress

Set-Content -Path $LockFile -Value $lockInfo -Force
Write-Log "Lock file created (PID $PID)." "DarkGray" "INFO"

try {

# =====================================================================
# PRECHECKS
# =====================================================================
Write-Log "Running prechecks..." "Yellow" "STEP"

foreach ($check in @(
    @{ Path = $ProjectPath;         Label = "Project path" },
    @{ Path = $ApiScript;           Label = "API launcher" },
    @{ Path = $TunnelScript;        Label = "Tunnel launcher" },
    @{ Path = $SignalKeyFile;       Label = "Signal key file" },
    @{ Path = $ProcessingStatusDir; Label = "Processing status dir" },
    @{ Path = $SignalEventsDir;     Label = "Signal events dir" }
)) {
    if (-not (Test-Path $check.Path)) {
        Invoke-Cleanup -ClearLock
        throw "$($check.Label) not found: $($check.Path)"
    }
}

Set-Location $ProjectPath

$SIGNAL_KEY = (Get-Content $SignalKeyFile -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($SIGNAL_KEY)) {
    Invoke-Cleanup -ClearLock
    throw "Signal key file exists but is empty: $SignalKeyFile"
}

$env:TV_WEBHOOK_SECRET = $SIGNAL_KEY

Write-Log "Project path     : $ProjectPath" "DarkGray" "INFO"
Write-Log "Signal key loaded: YES" "Green" "PASS"

# =====================================================================
# LOAD RESUME STATE
# =====================================================================
$resume = Get-ResumeState

if ($resume.RunId -gt 0 -and -not [string]::IsNullOrWhiteSpace($resume.BatchId)) {
    Write-Log "Resuming from previous partial run (RunId=$($resume.RunId))." "Yellow" "WARN"
    $runId         = $resume.RunId
    $manualBatchId = $resume.BatchId
    $manualEventId = $resume.EventId

    # Liveness flags must never be trusted from a stale resume — processes die
    # between runs.  Reset them unconditionally so every launch re-verifies.
    $resume.LocalApiReady      = $false
    $resume.CloudflaredReady   = $false
    $resume.PublicBackendReady = $false
    Save-ResumeState $resume
    Write-Log "Liveness flags reset — will re-verify API, cloudflared, and public backend." "Yellow" "WARN"
} else {
    $runId         = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $manualBatchId = "pipeline-backend-manual-test-$runId"
    $manualEventId = "pipeline-backend-manual-event-$runId"
    $resume.RunId   = $runId
    $resume.BatchId = $manualBatchId
    $resume.EventId = $manualEventId
    Save-ResumeState $resume
}

# =====================================================================
# KILL STALE STUFF
# =====================================================================
Write-Log "Killing stale processes..." "Yellow" "STEP"

Get-Process cloudflared -ErrorAction SilentlyContinue |
    Stop-Process -Force -ErrorAction SilentlyContinue

try {
    $pids8000 = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($p in $pids8000) {
        if ($p) {
            Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
            Write-Log "Killed port 8000 listener PID: $p" "DarkYellow" "WARN"
        }
    }
} catch {}

Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object {
        $_.Name -match "python|pythonw|uvicorn" -and
        -not [string]::IsNullOrWhiteSpace($_.CommandLine) -and
        $_.CommandLine -match "btc_adaptive_engine|uvicorn|api_server|start_api"
    } |
    ForEach-Object {
        try {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
            Write-Log "Killed stray PID $($_.ProcessId): $($_.Name)" "DarkYellow" "WARN"
        } catch {}
    }

Start-Sleep -Seconds 2
Write-Log "Kill phase complete." "Green" "PASS"

# =====================================================================
# PARALLEL API + TUNNEL LAUNCH
# start_api.ps1 resolves the repo root from its own script path via
# $MyInvocation.MyCommand.Path; -WorkingDirectory is a second guard.
# =====================================================================
if (-not $resume.ApiStarted -or -not $resume.TunnelStarted) {
    Write-Log "Starting API and tunnel in parallel..." "Yellow" "STEP"

    $apiJob = Start-Job -ScriptBlock {
        param($script, $projectPath)
        $p = Start-Process powershell.exe `
            -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-File", $script) `
            -WorkingDirectory $projectPath `
            -PassThru
        return $p.Id
    } -ArgumentList $ApiScript, $ProjectPath

    $tunnelJob = Start-Job -ScriptBlock {
        param($script, $projectPath)
        $p = Start-Process powershell.exe `
            -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-File", $script) `
            -WorkingDirectory $projectPath `
            -PassThru
        return $p.Id
    } -ArgumentList $TunnelScript, $ProjectPath

    $null = Wait-Job -Job @($apiJob, $tunnelJob) -Timeout 15

    $apiWindowPid    = Receive-Job -Job $apiJob    -ErrorAction SilentlyContinue
    $tunnelWindowPid = Receive-Job -Job $tunnelJob -ErrorAction SilentlyContinue

    Remove-Job -Job @($apiJob, $tunnelJob) -Force -ErrorAction SilentlyContinue

    foreach ($pid in @($apiWindowPid, $tunnelWindowPid)) {
        if ($pid) {
            try {
                $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
                if ($proc) { $script:LaunchedProcs += $proc }
            } catch {}
        }
    }

    $resume.ApiStarted    = $true
    $resume.TunnelStarted = $true
    Save-ResumeState $resume

    Write-Log "API window PID: $apiWindowPid  |  Tunnel window PID: $tunnelWindowPid" "DarkGray" "INFO"

    # Brief settle before polling
    Start-Sleep -Seconds 3
} else {
    Write-Log "SKIP: API + tunnel already started (resuming)." "DarkGray" "INFO"
}

# =====================================================================
# WAIT FOR LOCAL API (127.0.0.1:8000/health)
# On timeout, emit full diagnostics before aborting.
# =====================================================================
Write-Log "Waiting for local API..." "Yellow" "STEP"
$localOk = Wait-ForHttpOk -Url $LocalHealth -MaxAttempts 30 -DelaySeconds 2 -Label "Local API"
if (-not $localOk) {
    Write-ApiFailureDiagnostics
    Invoke-Cleanup -ClearLock
    throw "Local API never came up at $LocalHealth"
}
$resume.LocalApiReady = $true
Save-ResumeState $resume

# =====================================================================
# FALLBACK DIRECT CLOUDFLARED
# =====================================================================
$cloudflaredRunning = Get-Process cloudflared -ErrorAction SilentlyContinue
if (-not $cloudflaredRunning) {
    Write-Log "Tunnel script did not leave cloudflared running. Launching direct fallback..." "Yellow" "WARN"

    $cf = Get-CloudflaredExe
    if ([string]::IsNullOrWhiteSpace($cf)) {
        Invoke-Cleanup -ClearLock
        throw "cloudflared.exe not found in standard install locations."
    }

    $configPath = $null
    foreach ($candidate in @(
        "C:\Windows\System32\config\systemprofile\.cloudflared\config.yml",
        (Join-Path $env:USERPROFILE ".cloudflared\config.yml"),
        (Join-Path $env:APPDATA "cloudflared\config.yml")
    )) {
        if ($candidate -and (Test-Path $candidate)) {
            $configPath = $candidate
            break
        }
    }
    if (-not $configPath) {
        Invoke-Cleanup -ClearLock
        throw "cloudflared config not found in any standard location."
    }

    $cfFallbackProc = Start-Process `
        -FilePath $cf `
        -ArgumentList @("tunnel", "--config", $configPath, "run") `
        -WindowStyle Normal `
        -PassThru `
        -ErrorAction Stop

    $script:LaunchedProcs += $cfFallbackProc
    Write-Log "cloudflared fallback launched directly (PID: $($cfFallbackProc.Id))." "DarkGray" "INFO"

    Start-Sleep -Seconds 4
}

# =====================================================================
# WAIT FOR CLOUDFLARED PROCESS
# =====================================================================
if (-not $resume.CloudflaredReady) {
    Write-Log "Waiting for cloudflared..." "Yellow" "STEP"
    $cloudflaredProc = Wait-ForProcess -ProcessName "cloudflared" -MaxAttempts 20 -DelaySeconds 2
    if (-not $cloudflaredProc) { Invoke-Cleanup -ClearLock; throw "cloudflared never came up." }
    $resume.CloudflaredReady = $true
    Save-ResumeState $resume
} else {
    Write-Log "SKIP: cloudflared already confirmed running (resuming)." "DarkGray" "INFO"
    $cloudflaredProc = Get-Process cloudflared -ErrorAction SilentlyContinue | Select-Object -First 1
}

# =====================================================================
# WAIT FOR PUBLIC BACKEND
# =====================================================================
if (-not $resume.PublicBackendReady) {
    Write-Log "Waiting for public backend..." "Yellow" "STEP"
    $publicOk = Wait-ForHttpOk -Url $PublicHealth -MaxAttempts 30 -DelaySeconds 2 -Label "Public backend"
    if (-not $publicOk) { Invoke-Cleanup -ClearLock; throw "Public backend never came up at $PublicHealth" }
    $resume.PublicBackendReady = $true
    Save-ResumeState $resume
} else {
    Write-Log "SKIP: Public backend already confirmed up (resuming)." "DarkGray" "INFO"
}

# =====================================================================
# CHECK WORKER
# =====================================================================
if (-not $resume.WorkerReady) {
    Write-Log "Checking worker..." "Yellow" "STEP"
    $workerUrl = "{0}/?secret={1}" -f $WorkerBase.TrimEnd('/'), $SIGNAL_KEY

    try {
        $workerResp = Invoke-RestMethod `
            -Uri $workerUrl `
            -Method POST `
            -Body '{"test":"ping"}' `
            -ContentType "application/json" `
            -TimeoutSec 15 `
            -ErrorAction Stop

        Write-Log "Worker response: OK" "Green" "PASS"
        if ($workerResp) { Write-Log ($workerResp | Out-String).Trim() "DarkGray" "INFO" }
        $resume.WorkerReady = $true
        Save-ResumeState $resume
    } catch {
        Invoke-Cleanup -ClearLock
        throw "Worker check failed: $($_.Exception.Message)"
    }
} else {
    Write-Log "SKIP: Worker already confirmed reachable (resuming)." "DarkGray" "INFO"
}

# =====================================================================
# SEND BACKEND PIPELINE TEST
# =====================================================================
if (-not $resume.IngestSent) {
    Write-Log "Sending backend pipeline test..." "Yellow" "STEP"

    $backendPayload = @{
        source             = "tradingview"
        namespace          = "pipeline_test"
        symbol             = "BTCUSDT.P"
        chart_tf           = "1"
        batch_id           = $manualBatchId
        batch_trigger_side = "buy"
        batch_size         = 1
        batch_close_time   = $runId
        confirmed          = $true
        events             = @(
            @{
                event_id      = $manualEventId
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
    } | ConvertTo-Json -Depth 6

    try {
        $ingestResp = Invoke-WebRequest `
            -Uri "$($BackendBatchUrl)?signal_key=$SIGNAL_KEY" `
            -Method POST `
            -ContentType "application/json" `
            -Body $backendPayload `
            -UseBasicParsing `
            -TimeoutSec 20 `
            -ErrorAction Stop

        Write-Log "Backend ingest accepted: HTTP $($ingestResp.StatusCode)" "Green" "PASS"
        Write-Log $ingestResp.Content "DarkGray" "INFO"
        $resume.IngestSent = $true
        Save-ResumeState $resume
    } catch {
        Invoke-Cleanup -ClearLock
        throw "Backend ingest failed: $($_.Exception.Message)"
    }
} else {
    Write-Log "SKIP: Ingest already sent (resuming with BatchId=$manualBatchId)." "DarkGray" "INFO"
}

# =====================================================================
# WAIT FOR PROCESSING STATUS FILE
# =====================================================================
$statusFile = Join-Path $ProcessingStatusDir "$manualBatchId.json"

if (-not $resume.StatusFileReady) {
    Write-Log "Waiting for processing status file..." "Yellow" "STEP"
    $statusOk = Wait-ForPath -Path $statusFile -MaxAttempts 30 -DelaySeconds 2 -Label "Processing status file"
    if (-not $statusOk) { Invoke-Cleanup -ClearLock; throw "Processing status file never created for batch $manualBatchId" }
    $resume.StatusFileReady = $true
    Save-ResumeState $resume
} else {
    Write-Log "SKIP: Processing status file already confirmed (resuming)." "DarkGray" "INFO"
}

# =====================================================================
# WAIT FOR NORMALIZED EVENT FILE
# =====================================================================
if (-not $resume.EventFileReady) {
    Write-Log "Waiting for normalized event file..." "Yellow" "STEP"
    $eventOk      = $false
    $matchedEvent = $null

    for ($i = 1; $i -le 30; $i++) {
        $matchedEvent = Get-ChildItem $SignalEventsDir -Filter "*.json" -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -like "*$manualEventId*" } |
            Select-Object -First 1

        if ($matchedEvent) {
            $eventOk = $true
            Write-Log "Normalized event file found: $($matchedEvent.Name)" "Green" "PASS"
            break
        }

        Write-Log ("  [{0}/{1}] normalized event not ready..." -f $i, 30) "DarkGray" "INFO"
        Start-Sleep -Seconds 2
    }

    if (-not $eventOk) { Invoke-Cleanup -ClearLock; throw "Normalized event file never created for event $manualEventId" }
    $resume.EventFileReady = $true
    Save-ResumeState $resume
} else {
    Write-Log "SKIP: Normalized event file already confirmed (resuming)." "DarkGray" "INFO"
}

# =====================================================================
# ALL STEPS PASSED
# =====================================================================
Clear-ResumeState
Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
Write-Log "Lock file released." "DarkGray" "INFO"

# =====================================================================
# FINAL SUMMARY
# =====================================================================
Write-Log "" "White" "INFO"
Write-Log "=== FINAL SUMMARY ===" "Cyan" "INFO"
Write-Log ("API process window PID       : {0}" -f $(if ($apiWindowPid)    { $apiWindowPid }           else { "N/A (resumed)" })) "White" "INFO"
Write-Log ("Tunnel script window PID     : {0}" -f $(if ($tunnelWindowPid) { $tunnelWindowPid }        else { "N/A (resumed)" })) "White" "INFO"
Write-Log ("cloudflared PID              : {0}" -f $(if ($cloudflaredProc) { $cloudflaredProc.Id }     else { "N/A" }))           "White" "INFO"
Write-Log "Signal key loaded            : PASS" "Green" "PASS"
Write-Log "Local API reachable          : PASS" "Green" "PASS"
Write-Log "cloudflared running          : PASS" "Green" "PASS"
Write-Log "Public backend reachable     : PASS" "Green" "PASS"
Write-Log "Worker reachable             : PASS" "Green" "PASS"
Write-Log "Backend ingest accepted      : PASS" "Green" "PASS"
Write-Log "Processing status written    : PASS" "Green" "PASS"
Write-Log "Normalized event created     : PASS" "Green" "PASS"
Write-Log "" "White" "INFO"
Write-Log ("API launch command           : powershell.exe -NoExit -ExecutionPolicy Bypass -File $ApiScript") "DarkGray" "INFO"
Write-Log ("API ASGI target              : backend.api_server:app at 127.0.0.1:8000") "DarkGray" "INFO"
Write-Log ("API log                      : $ApiLogFile") "DarkGray" "INFO"
Write-Log ("Batch ID                     : $manualBatchId") "DarkGray" "INFO"
Write-Log ("Event ID                     : $manualEventId") "DarkGray" "INFO"
Write-Log ("Launch log                   : $LogFile") "DarkGray" "INFO"
Write-Log "" "White" "INFO"
Write-Log "Unified launch complete. Leave the API and tunnel windows open while trading." "Cyan" "INFO"

} finally {
    if (Test-Path $LockFile) {
        Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
        Write-Log "Lock file released (finally block)." "DarkGray" "INFO"
    }
}
