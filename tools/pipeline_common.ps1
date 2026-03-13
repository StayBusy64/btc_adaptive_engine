# tools/pipeline_common.ps1
#
# Canonical shared helpers for all BTC Adaptive Engine pipeline scripts.
# Dot-source this file before using any of the functions below:
#
#   . (Join-Path $PSScriptRoot "..\tools\pipeline_common.ps1")   # from a root-level script
#   . (Join-Path $PSScriptRoot "pipeline_common.ps1")             # from within tools/
#
# All URL/path/param resolution must go through these helpers.
# Do NOT hardcode URLs, ports, or secret param names in individual scripts.

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Repo-root resolution
# ---------------------------------------------------------------------------

function Get-PipelineRepoRoot {
    <#
    .SYNOPSIS
        Returns the absolute path to the repository root.
    .DESCRIPTION
        Resolves repo root as the parent of the tools/ directory where this
        file lives, so it is correct regardless of the caller's working directory.
    #>
    return (Split-Path -Parent $PSScriptRoot)
}

# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

function Get-PipelineConfig {
    <#
    .SYNOPSIS
        Reads pipeline_config.json from the repo root and returns a PSCustomObject.
    #>
    $repoRoot = Get-PipelineRepoRoot
    $configPath = Join-Path $repoRoot "pipeline_config.json"

    if (-not (Test-Path $configPath)) {
        throw "pipeline_config.json not found at $configPath"
    }

    return (Get-Content $configPath -Raw | ConvertFrom-Json)
}

# ---------------------------------------------------------------------------
# Signal key
# ---------------------------------------------------------------------------

function Get-PipelineSignalKey {
    <#
    .SYNOPSIS
        Returns the backend signal key.
    .DESCRIPTION
        Reads from the TRADINGVIEW_INGEST_SIGNAL_KEY env var first,
        then falls back to the signal_key_file path declared in pipeline_config.json.
    #>
    $envKey = $env:TRADINGVIEW_INGEST_SIGNAL_KEY
    if (-not [string]::IsNullOrWhiteSpace($envKey)) {
        return $envKey.Trim()
    }

    $config = Get-PipelineConfig
    $repoRoot = Get-PipelineRepoRoot
    $keyFile = Join-Path $repoRoot ($config.signal_key_file -replace "/", "\")

    if (-not (Test-Path $keyFile)) {
        throw "Signal key file not found at $keyFile (set TRADINGVIEW_INGEST_SIGNAL_KEY to override)"
    }

    $key = (Get-Content $keyFile -TotalCount 1).Trim()
    if ([string]::IsNullOrWhiteSpace($key)) {
        throw "Signal key file is empty: $keyFile"
    }

    return $key
}

# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

function Get-PipelineWorkerUrl {
    <#
    .SYNOPSIS
        Returns the full Cloudflare Worker URL with the webhook secret appended.
    .DESCRIPTION
        Secret is read from the TV_WEBHOOK_SECRET env var or the -WorkerSecret parameter.
        The secret param name comes from pipeline_config.json (worker_secret_param).
    .PARAMETER WorkerSecret
        Override: pass the secret directly.  Falls back to $env:TV_WEBHOOK_SECRET.
    #>
    param([string]$WorkerSecret = "")

    $config = Get-PipelineConfig

    if ([string]::IsNullOrWhiteSpace($WorkerSecret)) {
        $WorkerSecret = $env:TV_WEBHOOK_SECRET
    }

    if ([string]::IsNullOrWhiteSpace($WorkerSecret)) {
        throw "Worker secret not provided.  Set the TV_WEBHOOK_SECRET environment variable or pass -WorkerSecret."
    }

    $param = $config.worker_secret_param
    return "$($config.worker_base)/?$param=$WorkerSecret"
}

function Get-PipelineBackendBatchUrl {
    <#
    .SYNOPSIS
        Returns the public backend batch URL with signal_key appended.
    .PARAMETER SignalKey
        Override signal key.  Falls back to Get-PipelineSignalKey.
    #>
    param([string]$SignalKey = "")

    $config = Get-PipelineConfig

    if ([string]::IsNullOrWhiteSpace($SignalKey)) {
        $SignalKey = Get-PipelineSignalKey
    }

    $param = $config.signal_key_query_param
    return "$($config.backend_base)$($config.batch_path)?$param=$SignalKey"
}

function Get-PipelineLocalBatchUrl {
    <#
    .SYNOPSIS
        Returns the local API batch URL with signal_key appended.
    .PARAMETER SignalKey
        Override signal key.  Falls back to Get-PipelineSignalKey.
    #>
    param([string]$SignalKey = "")

    $config = Get-PipelineConfig

    if ([string]::IsNullOrWhiteSpace($SignalKey)) {
        $SignalKey = Get-PipelineSignalKey
    }

    $param = $config.signal_key_query_param
    return "$($config.local_api_base)$($config.batch_path)?$param=$SignalKey"
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

function Invoke-PipelineJsonRequest {
    <#
    .SYNOPSIS
        Sends an HTTP request and returns status code, raw content, and parsed JSON.
    #>
    param(
        [Parameter(Mandatory = $true)][ValidateSet("GET", "POST")][string]$Method,
        [Parameter(Mandatory = $true)][string]$Uri,
        [object]$Body,
        [hashtable]$Headers,
        [int]$TimeoutSec = 15
    )

    $requestArgs = @{
        Method          = $Method
        Uri             = $Uri
        UseBasicParsing = $true
        TimeoutSec      = $TimeoutSec
    }

    if ($null -ne $Headers -and $Headers.Count -gt 0) {
        $requestArgs.Headers = $Headers
    }

    if ($PSBoundParameters.ContainsKey("Body")) {
        if ($Body -is [string]) {
            $jsonBody = $Body
        } else {
            $jsonBody = $Body | ConvertTo-Json -Depth 10 -Compress
        }

        $requestArgs.Body    = $jsonBody
        $requestArgs.ContentType = "application/json"
    }

    $response = Invoke-WebRequest @requestArgs
    $parsed   = $null

    if (-not [string]::IsNullOrWhiteSpace($response.Content)) {
        try {
            $parsed = $response.Content | ConvertFrom-Json -Depth 20
            for ($i = 0; $i -lt 3 -and $parsed -is [string]; $i++) {
                try { $parsed = $parsed.Trim() | ConvertFrom-Json -Depth 20 } catch { break }
            }
        } catch {
            $parsed = $response.Content
        }
    }

    return [pscustomobject]@{
        StatusCode = [int]$response.StatusCode
        Content    = $response.Content
        Json       = $parsed
    }
}

function Get-PipelineRouteStatusCode {
    <#
    .SYNOPSIS
        Returns the HTTP status code for a GET probe, or throws on network error.
    #>
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [int]$TimeoutSec = 5
    )

    try {
        $response = Invoke-WebRequest -Method GET -UseBasicParsing -Uri $Uri -TimeoutSec $TimeoutSec
        return [int]$response.StatusCode
    } catch {
        if ($_.Exception.Response) {
            return [int]$_.Exception.Response.StatusCode
        }
        throw
    }
}
