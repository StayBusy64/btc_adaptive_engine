param(
    [switch]$LaunchNow,
    [string]$TaskName = "BTC Pipeline Autoverify",
    [int]$IntervalMinutes = 15
)

$ErrorActionPreference = "Stop"

# Resolve repo root and load shared config/helpers.
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $repoRoot "tools\pipeline_common.ps1")

$autoverifyScript = Join-Path $repoRoot "tools\pipeline_autoverify.ps1"

if (-not (Test-Path $autoverifyScript)) {
    throw "Autoverify script not found: $autoverifyScript"
}

Write-Host "=== Installing Pipeline Autoverify Scheduled Task ==="
Write-Host "Task name:      $TaskName"
Write-Host "Script:         $autoverifyScript"
Write-Host "Interval:       every $IntervalMinutes minutes"
Write-Host ""

# Build the PowerShell command to run in the task.
$psExe = if (Get-Command powershell.exe -ErrorAction SilentlyContinue) {
    (Get-Command powershell.exe -ErrorAction SilentlyContinue).Source
} else {
    "powershell.exe"
}

$taskArgs = "-NoLogo -NonInteractive -ExecutionPolicy Bypass -File `"$autoverifyScript`""

$action  = New-ScheduledTaskAction -Execute $psExe -Argument $taskArgs -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) -Once -At (Get-Date)
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable

# Remove any stale registration before re-registering.
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed existing task: $TaskName"
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "Task registered: $TaskName"
Write-Host ""

if ($LaunchNow) {
    Write-Host "Starting task immediately..."
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "Task started."
} else {
    Write-Host "Task installed.  Use -LaunchNow to start it immediately, or wait for the next scheduled run."
}

Write-Host ""
Write-Host "Done."
