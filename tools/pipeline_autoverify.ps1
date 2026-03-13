# tools/pipeline_autoverify.ps1
#
# Autoverify watcher for the TradingView ingest pipeline.
#
# Truth model (in order of priority):
#   1. PERSISTED   = data/state/normalized/processing_status/<batch_id>.json  (status: queued)
#   2. PROCESSED   = data/state/normalized/signal_events/<event_id>.json exists
#                    AND processing_status status == "processed"
#   3. JOURNAL     = data/state/outcomes/signal_journal.jsonl contains event_id  (secondary)
#
# This script is designed to be run periodically via a scheduled task created
# by install-pipeline-autoverify.ps1.  It may also be run manually at any time.

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "pipeline_common.ps1")

$cfg      = Get-PipelineConfig
$repoRoot = Get-PipelineRepoRoot

# ---------------------------------------------------------------------------
# Ingest data directories  (mirrors tradingview_ingest_storage.py paths)
# ---------------------------------------------------------------------------
$dataRoot          = Join-Path $repoRoot "data"
$statusDir         = Join-Path $dataRoot "state\normalized\processing_status"
$signalEventsDir   = Join-Path $dataRoot "state\normalized\signal_events"
$journalFile       = Join-Path $dataRoot "state\outcomes\signal_journal.jsonl"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

function Get-StatusFiles {
    if (-not (Test-Path $statusDir)) { return @() }
    return Get-ChildItem -Path $statusDir -Filter "*.json" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 50
}

function Read-StatusFile {
    param([string]$Path)
    try {
        return Get-Content $Path -Raw | ConvertFrom-Json -Depth 10
    } catch {
        return $null
    }
}

function Test-SignalEventExists {
    param([string]$EventId)
    if (-not (Test-Path $signalEventsDir)) { return $false }
    $safe = $EventId -replace "[^a-zA-Z0-9\-_]", "_"
    return Test-Path (Join-Path $signalEventsDir "$safe.json")
}

function Test-JournalContains {
    param([string]$EventId)
    if (-not (Test-Path $journalFile)) { return $false }
    return (Select-String -Path $journalFile -Pattern ([regex]::Escape($EventId)) -Quiet)
}

# ---------------------------------------------------------------------------
# Main verification loop
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "============================================="
Write-Host "PIPELINE AUTOVERIFY  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "============================================="

# Check local API health first.
$localApiUp = $false
try {
    $healthResp = Invoke-WebRequest -Uri $cfg.local_health -TimeoutSec 5 -UseBasicParsing
    $localApiUp = ($healthResp.StatusCode -eq 200)
    Write-Host ("Local API health:   HTTP {0}" -f $healthResp.StatusCode)
} catch {
    Write-Host "Local API health:   DOWN" -ForegroundColor Red
}

$statusFiles = Get-StatusFiles

if ($statusFiles.Count -eq 0) {
    Write-Host "No processing_status files found in $statusDir"
    Write-Host ""
    Write-Host "Nothing to verify."
    exit 0
}

Write-Host ("Scanning {0} recent processing_status files..." -f $statusFiles.Count)
Write-Host ""

$persisted  = 0
$processed  = 0
$journalHit = 0
$stale      = 0
$problems   = @()

foreach ($file in $statusFiles) {
    $status = Read-StatusFile -Path $file.FullName
    if ($null -eq $status) { continue }

    $batchId  = $status.batch_id
    $batchStatus = $status.status

    # ── 1. Persisted? ─────────────────────────────────────────────────────
    if ($batchStatus -in @("queued", "processing", "processed")) {
        $persisted++
    } else {
        $stale++
        $problems += "$batchId  status=$batchStatus (unexpected)"
        continue
    }

    # ── 2. Processed? ─────────────────────────────────────────────────────
    $isProcessed = ($batchStatus -eq "processed")
    $eventFound  = $false

    if ($status.PSObject.Properties["events"]) {
        foreach ($ev in $status.events) {
            $eid = $ev.event_id
            if (-not [string]::IsNullOrWhiteSpace($eid) -and (Test-SignalEventExists -EventId $eid)) {
                $eventFound = $true
                break
            }
        }
    }

    if ($isProcessed -and $eventFound) {
        $processed++
    } elseif ($isProcessed -and -not $eventFound) {
        $problems += "$batchId  status=processed but no signal_events file found"
    }

    # ── 3. Journal (secondary) ─────────────────────────────────────────────
    if ($status.PSObject.Properties["events"]) {
        foreach ($ev in $status.events) {
            $eid = $ev.event_id
            if (-not [string]::IsNullOrWhiteSpace($eid) -and (Test-JournalContains -EventId $eid)) {
                $journalHit++
                break
            }
        }
    }
}

Write-Host ("  Persisted (queued/processing/processed) : {0}" -f $persisted)
Write-Host ("  Processed (status=processed + event JSON): {0}" -f $processed)
Write-Host ("  Journal advanced (secondary)             : {0}" -f $journalHit)
if ($stale -gt 0) {
    Write-Host ("  Unexpected status files                  : {0}" -f $stale) -ForegroundColor Yellow
}

if ($problems.Count -gt 0) {
    Write-Host ""
    Write-Host "Issues detected:" -ForegroundColor Yellow
    foreach ($p in $problems) {
        Write-Host "  - $p" -ForegroundColor Yellow
    }
}

Write-Host ""
if ($problems.Count -eq 0 -and $persisted -gt 0) {
    Write-Host "Autoverify: PASS" -ForegroundColor Green
} elseif ($persisted -eq 0) {
    Write-Host "Autoverify: NO DATA  (no batches in processing_status directory)" -ForegroundColor Yellow
} else {
    Write-Host "Autoverify: ISSUES DETECTED  (see above)" -ForegroundColor Red
}

Write-Host "============================================="
