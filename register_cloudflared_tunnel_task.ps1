param(
    [string]$TaskName = "BTC Cloudflared Tunnel",
    [string]$TunnelName = "btc-adaptive-engine",
    [string]$ConfigPath = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = Join-Path $env:USERPROFILE ".cloudflared\config.yml"
}

$cloudflaredCmd = Get-Command cloudflared -ErrorAction SilentlyContinue
if (-not $cloudflaredCmd) {
    throw "cloudflared executable not found on PATH. Install or repair cloudflared first."
}

$cloudflaredExe = $cloudflaredCmd.Source
if (-not (Test-Path $ConfigPath)) {
    throw "Tunnel config not found: $ConfigPath"
}

$actionArgs = "tunnel --config `"$ConfigPath`" run $TunnelName"
$action = New-ScheduledTaskAction -Execute $cloudflaredExe -Argument $actionArgs
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Auto-starts cloudflared tunnel on user logon" -Force -ErrorAction Stop | Out-Null

Write-Host "Scheduled task registered: $TaskName"
Write-Host "Executable: $cloudflaredExe"
Write-Host "Config: $ConfigPath"
