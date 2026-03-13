param(
    [string]$TaskName = "BTC Adaptive Engine API Watchdog",
    [string]$RepoRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}

$scriptPath = Join-Path $RepoRoot "start_api_watchdog.ps1"
if (-not (Test-Path $scriptPath)) {
    throw "Watchdog script not found: $scriptPath"
}

$actionArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $actionArgs
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

$registered = $false
try {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Auto-starts BTC API watchdog on user logon" -Force -ErrorAction Stop | Out-Null
    Write-Host "Scheduled task registered with highest privileges: $TaskName"
    $registered = $true
} catch {
    Write-Warning "Could not register highest-privilege task in current shell: $($_.Exception.Message)"
}

if (-not $registered) {
    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Auto-starts BTC API watchdog on user logon" -Force -ErrorAction Stop | Out-Null
    Write-Host "Scheduled task registered with user privileges: $TaskName"
}

Write-Host "Script: $scriptPath"
