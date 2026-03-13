# setup-cloudflare-tunnel.ps1
# Streamlined Cloudflare Tunnel setup for the BTC Adaptive Engine.
# Run in Administrator PowerShell.
#
# Usage:
#   .\setup-cloudflare-tunnel.ps1                  # create tunnel, route DNS
#   .\setup-cloudflare-tunnel.ps1 -InstallService  # also register as Windows service

param(
    [switch]$InstallService,
    [string]$TunnelName  = "btc-adaptive-engine",
    [string]$Domain      = "dopedreamspnl.com",
    [string]$LocalPort   = "8000"
)

$ErrorActionPreference = "Stop"

$Hostname     = "api.$Domain"
$LocalService = "http://localhost:$LocalPort"

# ===== PATHS =====
$CloudflaredDir = "C:\Cloudflared"
$CloudflaredExe = $null
$UserCfDir      = Join-Path $env:USERPROFILE ".cloudflared"
$SysCfDir       = "C:\Windows\System32\config\systemprofile\.cloudflared"
$SysConfigPath  = Join-Path $SysCfDir "config.yml"

# ===== FUNCTIONS =====

function Require-Admin {
    $currentUser = New-Object Security.Principal.WindowsPrincipal(
        [Security.Principal.WindowsIdentity]::GetCurrent()
    )
    if (-not $currentUser.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run this script in Administrator PowerShell."
    }
}

function Resolve-Cloudflared {
    Write-Host "Resolving cloudflared executable..."

    $cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
    if ($cmd) {
        $script:CloudflaredExe = $cmd.Source
    }

    if (-not $script:CloudflaredExe) {
        $searchPaths = @(
            "C:\Program Files (x86)\cloudflared\cloudflared.exe",
            "C:\Program Files\cloudflared\cloudflared.exe",
            "$env:LOCALAPPDATA\Microsoft\WinGet\Links\cloudflared.exe",
            "C:\Cloudflared\bin\cloudflared.exe"
        )

        foreach ($p in $searchPaths) {
            if (Test-Path $p) {
                $script:CloudflaredExe = $p
                break
            }
        }
    }

    if (-not $script:CloudflaredExe) {
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            Write-Host "Installing cloudflared with winget..."
            winget install --id Cloudflare.cloudflared -e `
                --accept-package-agreements --accept-source-agreements

            $cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
            if ($cmd) {
                $script:CloudflaredExe = $cmd.Source
            }
        }
    }

    if (-not $script:CloudflaredExe) {
        throw "cloudflared.exe was not found. Install it manually and rerun."
    }

    Write-Host "Using cloudflared: $script:CloudflaredExe"
    & $script:CloudflaredExe --version
}

function Ensure-Login {
    $userCert = Join-Path $UserCfDir "cert.pem"
    if (-not (Test-Path $userCert)) {
        Write-Host ""
        Write-Host "Browser login required. Approve the domain in Cloudflare when prompted."
        Write-Host ""
        & $CloudflaredExe tunnel login
    }

    if (-not (Test-Path $userCert)) {
        throw "cloudflared login did not create cert.pem in $UserCfDir"
    }
}

function Ensure-Tunnel {
    $existing = (& $CloudflaredExe tunnel list 2>$null | Out-String)
    if ($existing -match [regex]::Escape($TunnelName)) {
        Write-Host "Tunnel '$TunnelName' already exists."
    } else {
        Write-Host "Creating tunnel '$TunnelName'..."
        & $CloudflaredExe tunnel create $TunnelName
    }

    $jsonFile = Get-ChildItem $UserCfDir -Filter "*.json" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if (-not $jsonFile) {
        throw "Tunnel credentials JSON not found in $UserCfDir"
    }

    return @{
        TunnelId = [System.IO.Path]::GetFileNameWithoutExtension($jsonFile.Name)
        JsonName = $jsonFile.Name
        JsonPath = $jsonFile.FullName
    }
}

function Route-Dns {
    param([string]$TunnelId)

    Write-Host "Routing DNS: $Hostname -> tunnel $TunnelName ..."
    # NOTE: Some setups require the tunnel ID instead of the name here.
    # If this fails, swap $TunnelName for $TunnelId and rerun.
    & $CloudflaredExe tunnel route dns $TunnelName $Hostname
}

function Write-UserConfig {
    param(
        [string]$TunnelId,
        [string]$JsonPath
    )

    $configPath = Join-Path $UserCfDir "config.yml"

    $yaml = @"
tunnel: $TunnelId
credentials-file: $JsonPath

ingress:
  - hostname: $Hostname
    service: $LocalService
  - service: http_status:404
"@

    Set-Content -Path $configPath -Value $yaml -Encoding UTF8
    Write-Host "Wrote user config: $configPath"

    & $CloudflaredExe --config $configPath tunnel ingress validate
    return $configPath
}

function Install-TunnelService {
    param(
        [string]$TunnelId,
        [string]$JsonName
    )

    New-Item -ItemType Directory -Force -Path $CloudflaredDir | Out-Null
    New-Item -ItemType Directory -Force -Path $SysCfDir | Out-Null

    $userCert = Join-Path $UserCfDir "cert.pem"
    Copy-Item $userCert (Join-Path $SysCfDir "cert.pem") -Force

    $jsonSrc = Join-Path $UserCfDir $JsonName
    Copy-Item $jsonSrc (Join-Path $SysCfDir $JsonName) -Force

    $sysJson = Join-Path $SysCfDir $JsonName
    $yaml = @"
tunnel: $TunnelId
credentials-file: $sysJson

ingress:
  - hostname: $Hostname
    service: $LocalService
  - service: http_status:404

logfile: C:\Cloudflared\cloudflared.log
"@

    Set-Content -Path $SysConfigPath -Value $yaml -Encoding UTF8
    Write-Host "Wrote service config: $SysConfigPath"

    & $CloudflaredExe --config $SysConfigPath tunnel ingress validate

    Write-Host "Installing cloudflared Windows service..."
    & $CloudflaredExe service install

    $desired = "`"$CloudflaredExe`" --config=`"$SysConfigPath`" tunnel run"
    Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Services\Cloudflared' `
        -Name ImagePath -Value $desired

    Write-Host "Restarting cloudflared service..."
    sc.exe stop cloudflared | Out-Null
    Start-Sleep -Seconds 2
    sc.exe start cloudflared | Out-Null
    Start-Sleep -Seconds 3
    Get-Service cloudflared
}

# ===== MAIN =====
Require-Admin
Resolve-Cloudflared
Ensure-Login

$tunnelInfo = Ensure-Tunnel
Route-Dns -TunnelId $tunnelInfo.TunnelId

$configPath = Write-UserConfig `
    -TunnelId $tunnelInfo.TunnelId `
    -JsonPath $tunnelInfo.JsonPath

if ($InstallService) {
    Install-TunnelService `
        -TunnelId $tunnelInfo.TunnelId `
        -JsonName $tunnelInfo.JsonName
}

# ===== SUMMARY =====
Write-Host ""
Write-Host "============================================"
Write-Host "  Cloudflare Tunnel Setup Complete"
Write-Host "============================================"
Write-Host ""
Write-Host "BACKEND_URL = https://$Hostname/webhooks/tradingview/batch"
Write-Host ""
Write-Host "Set this as BACKEND_URL in your Cloudflare Worker."
Write-Host ""
Write-Host "Your secret should already be configured as:"
Write-Host "  TRADINGVIEW_INGEST_SIGNAL_KEY  (env var or data\tv_ingest\signal_key.txt)"
Write-Host ""

if (-not $InstallService) {
    Write-Host "To run the tunnel manually:"
    Write-Host "  cloudflared --config `"$configPath`" tunnel run"
    Write-Host ""
    Write-Host "To install as a Windows service later, rerun with -InstallService:"
    Write-Host "  .\setup-cloudflare-tunnel.ps1 -InstallService"
}

Write-Host ""
Write-Host "Architecture:"
Write-Host "  TradingView -> Cloudflare Worker -> $Hostname -> Tunnel -> localhost:$LocalPort -> FastAPI"
Write-Host ""
Write-Host "Quick smoke test (expect 405 Method Not Allowed):"
Write-Host "  https://$Hostname/webhooks/tradingview/batch"
Write-Host ""
