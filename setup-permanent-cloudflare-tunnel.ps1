# setup-permanent-cloudflare-tunnel.ps1
# Run in Administrator PowerShell from VS Code

$ErrorActionPreference = "Stop"

# ===== EDIT THESE IF YOU WANT =====
$TunnelName   = "btc-adaptive-engine"
$Domain       = "dopedreamspnl.com"
$Hostname     = "api.$Domain"
$LocalService = "http://localhost:8000"

# Optional local files to patch after tunnel creation
$WorkerConfigPath = "C:\Users\Stayb\OneDrive\Desktop\tv-webhook-worker\wrangler.jsonc"
$PipelineTestPath = "C:\Users\Stayb\OneDrive\Desktop\btc_adaptive_engine\test-webhook-pipeline.ps1"

# ===== PATHS =====
$CloudflaredDir = "C:\Cloudflared"
$CloudflaredExe = $null
$UserCfDir      = Join-Path $env:USERPROFILE ".cloudflared"
$SysCfDir       = "C:\Windows\System32\config\systemprofile\.cloudflared"
$SysConfigPath  = Join-Path $SysCfDir "config.yml"

function Require-Admin {
    $currentUser = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    if (-not $currentUser.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run this script in Administrator PowerShell."
    }
}

function Ensure-Cloudflared {
    Write-Host "Resolving cloudflared executable..."

    $CloudflaredCmd = Get-Command cloudflared -ErrorAction SilentlyContinue
    if ($CloudflaredCmd) {
        $script:CloudflaredExe = $CloudflaredCmd.Source
    }

    if (-not $script:CloudflaredExe) {
        $commonPaths = @(
            "C:\Program Files (x86)\cloudflared\cloudflared.exe",
            "C:\Program Files\cloudflared\cloudflared.exe",
            "$env:LOCALAPPDATA\Microsoft\WinGet\Links\cloudflared.exe",
            "C:\Cloudflared\bin\cloudflared.exe"
        )

        foreach ($p in $commonPaths) {
            if (Test-Path $p) {
                $script:CloudflaredExe = $p
                break
            }
        }
    }

    if (-not $script:CloudflaredExe) {
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            Write-Host "Installing cloudflared with winget..."
            winget install --id Cloudflare.cloudflared -e --accept-package-agreements --accept-source-agreements

            $CloudflaredCmd = Get-Command cloudflared -ErrorAction SilentlyContinue
            if ($CloudflaredCmd) {
                $script:CloudflaredExe = $CloudflaredCmd.Source
            }

            if (-not $script:CloudflaredExe) {
                $commonPaths = @(
                    "C:\Program Files (x86)\cloudflared\cloudflared.exe",
                    "C:\Program Files\cloudflared\cloudflared.exe",
                    "$env:LOCALAPPDATA\Microsoft\WinGet\Links\cloudflared.exe",
                    "C:\Cloudflared\bin\cloudflared.exe"
                )

                foreach ($p in $commonPaths) {
                    if (Test-Path $p) {
                        $script:CloudflaredExe = $p
                        break
                    }
                }
            }
        } else {
            throw "winget not found. Install cloudflared manually, then rerun this script."
        }
    }

    if (-not $script:CloudflaredExe) {
        throw "cloudflared.exe was not found. Install exists or PATH may be stale."
    }

    Write-Host "Using cloudflared: $script:CloudflaredExe"
    & $script:CloudflaredExe --version
}

function Ensure-ServiceBase {
    New-Item -ItemType Directory -Force -Path $CloudflaredDir | Out-Null
    New-Item -ItemType Directory -Force -Path $SysCfDir | Out-Null

    Write-Host "Installing cloudflared Windows service..."
    & $CloudflaredExe service install
}

function Ensure-Login {
    $UserCert = Join-Path $UserCfDir "cert.pem"
    if (-not (Test-Path $UserCert)) {
        Write-Host ""
        Write-Host "Browser login is required once. Approve the domain in Cloudflare when the browser opens."
        Write-Host ""
        & $CloudflaredExe tunnel login
    }

    if (-not (Test-Path $UserCert)) {
        throw "cloudflared login did not create cert.pem in $UserCfDir"
    }

    Copy-Item $UserCert (Join-Path $SysCfDir "cert.pem") -Force
}

function Ensure-Tunnel {
    $existing = (& $CloudflaredExe tunnel list 2>$null | Out-String)
    if ($existing -match [regex]::Escape($TunnelName)) {
        Write-Host "Tunnel $TunnelName already exists."
    } else {
        Write-Host "Creating tunnel $TunnelName ..."
        & $CloudflaredExe tunnel create $TunnelName
    }

    $jsonFile = Get-ChildItem $UserCfDir -Filter "*.json" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if (-not $jsonFile) {
        throw "Tunnel credentials JSON was not found in $UserCfDir"
    }

    Copy-Item $jsonFile.FullName (Join-Path $SysCfDir $jsonFile.Name) -Force

    $TunnelId = [System.IO.Path]::GetFileNameWithoutExtension($jsonFile.Name)
    return @{
        TunnelId = $TunnelId
        JsonName = $jsonFile.Name
    }
}

function Ensure-DnsRoute {
    param(
        [string]$HostNameToCreate,
        [string]$TunnelId
    )

    Write-Host "Routing DNS hostname $HostNameToCreate to tunnel..."
    & $CloudflaredExe tunnel route dns $TunnelId $HostNameToCreate
}

function Write-TunnelConfig {
    param(
        [string]$TunnelId,
        [string]$JsonName
    )

    $yaml = @"
tunnel: $TunnelId
credentials-file: C:\Windows\System32\config\systemprofile\.cloudflared\$JsonName

ingress:
  - hostname: $Hostname
    service: $LocalService
  - service: http_status:404

logfile: C:\Cloudflared\cloudflared.log
"@

    Set-Content -Path $SysConfigPath -Value $yaml -Encoding UTF8
    Write-Host "Wrote config: $SysConfigPath"

    Push-Location $CloudflaredDir
    try {
        & $CloudflaredExe --config $SysConfigPath tunnel ingress validate
    } finally {
        Pop-Location
    }
}

function Set-ServiceImagePath {
    $desired = "`"$CloudflaredExe`" --config=`"$SysConfigPath`" tunnel run"
    Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Services\Cloudflared' -Name ImagePath -Value $desired
    Write-Host "Updated Cloudflared service ImagePath."
}

function Restart-TunnelService {
    Write-Host "Restarting cloudflared service..."
    sc.exe stop cloudflared | Out-Null
    Start-Sleep -Seconds 2
    sc.exe start cloudflared | Out-Null
    Start-Sleep -Seconds 3
    Get-Service cloudflared
}

function Patch-Files {
    $PermanentBackend = "https://$Hostname/webhooks/tradingview/batch"

    if (Test-Path $WorkerConfigPath) {
        $raw = Get-Content $WorkerConfigPath -Raw
        if ($raw -match '"FORWARD_URL"\s*:\s*"[^"]*"') {
            $raw = [regex]::Replace($raw, '"FORWARD_URL"\s*:\s*"[^"]*"', "`"FORWARD_URL`": `"$PermanentBackend`"")
        } else {
            $raw = $raw -replace '("observability"\s*:\s*\{\s*"enabled"\s*:\s*true\s*\}\s*)', "`$1,`r`n  `"vars`": {`r`n    `"FORWARD_URL`": `"$PermanentBackend`"`r`n  }"
        }
        Set-Content $WorkerConfigPath $raw -Encoding UTF8
        Write-Host "Patched wrangler.jsonc FORWARD_URL -> $PermanentBackend"
    }

    if (Test-Path $PipelineTestPath) {
        $raw = Get-Content $PipelineTestPath -Raw
        $raw = [regex]::Replace($raw, '\$BACKEND_URL\s*=\s*"[^"]*"', "`$BACKEND_URL = `"$PermanentBackend`"")
        Set-Content $PipelineTestPath $raw -Encoding UTF8
        Write-Host "Patched test-webhook-pipeline.ps1 BACKEND_URL -> $PermanentBackend"
    }
}

# ===== RUN =====
Require-Admin
Ensure-Cloudflared
Ensure-ServiceBase
Ensure-Login
$tunnelInfo = Ensure-Tunnel
Ensure-DnsRoute -HostNameToCreate $Hostname -TunnelId $tunnelInfo.TunnelId
Write-TunnelConfig -TunnelId $tunnelInfo.TunnelId -JsonName $tunnelInfo.JsonName
Set-ServiceImagePath
Restart-TunnelService
Patch-Files

Write-Host ""
Write-Host "Done."
Write-Host "Permanent backend URL:"
Write-Host "  https://$Hostname/webhooks/tradingview/batch"
Write-Host ""
Write-Host "Next step:"
Write-Host "  Redeploy your Worker so FORWARD_URL uses the permanent hostname."
