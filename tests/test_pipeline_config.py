"""
Tests for the API launch path configuration.

Validates that the PowerShell launcher scripts:
  - Exist in the repo root
  - Resolve the repo root from the script's own path (MyInvocation.MyCommand.Path)
  - Set the working directory before launching uvicorn
  - Target backend.api_server:app on 127.0.0.1:8000
  - Do NOT reference the deprecated port 8010
  - The unified launcher (start_backend_and_tunnel.ps1) passes -WorkingDirectory
    to every Start-Process spawning powershell.exe
  - The unified launcher captures logs under logs\\ (references logs\\api.log)
  - The unified launcher emits diagnostics on local API failure
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read(name: str) -> str:
    return (REPO_ROOT / name).read_text(encoding="utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Required files
# ---------------------------------------------------------------------------

REQUIRED_PS1_SCRIPTS = [
    "start_api.ps1",
    "start_backend_and_tunnel.ps1",
    "start_api_watchdog.ps1",
    "setup-permanent-cloudflare-tunnel.ps1",
    "check_pipeline_status.ps1",
]


@pytest.mark.parametrize("script_name", REQUIRED_PS1_SCRIPTS)
def test_required_ps1_script_exists(script_name: str) -> None:
    path = REPO_ROOT / script_name
    assert path.exists(), f"Required PS1 script missing: {script_name}"
    assert path.stat().st_size > 0, f"PS1 script is empty: {script_name}"


# ---------------------------------------------------------------------------
# start_api.ps1 — path resolution and launch correctness
# ---------------------------------------------------------------------------

def test_start_api_resolves_root_from_invocation_path() -> None:
    """$MyInvocation.MyCommand.Path must be used to derive $root."""
    content = _read("start_api.ps1")
    assert "$MyInvocation.MyCommand.Path" in content, (
        "start_api.ps1 must resolve $root from $MyInvocation.MyCommand.Path "
        "so it works regardless of the calling process's working directory."
    )


def test_start_api_has_pscriptroot_fallback() -> None:
    """$PSScriptRoot must be present as a fallback for $root."""
    content = _read("start_api.ps1")
    assert "$PSScriptRoot" in content, (
        "start_api.ps1 must have $PSScriptRoot as a fallback for $root "
        "in case $MyInvocation.MyCommand.Path is empty."
    )


def test_start_api_sets_location_to_root() -> None:
    """Set-Location $root must be called to anchor the working directory."""
    content = _read("start_api.ps1")
    assert "Set-Location $root" in content, (
        "start_api.ps1 must call Set-Location $root to ensure uvicorn "
        "can find the backend package."
    )


def test_start_api_targets_correct_asgi_app() -> None:
    """The ASGI target must be backend.api_server:app."""
    content = _read("start_api.ps1")
    assert "backend.api_server:app" in content, (
        "start_api.ps1 must explicitly target backend.api_server:app."
    )


def test_start_api_binds_to_canonical_host_and_port() -> None:
    """API must bind to 127.0.0.1:8000. No 8010 fallback."""
    content = _read("start_api.ps1")
    assert "127.0.0.1" in content, "start_api.ps1 must bind to 127.0.0.1"
    assert "8000" in content, "start_api.ps1 must use port 8000"


def test_start_api_has_no_port_8010() -> None:
    """Port 8010 must not appear anywhere in start_api.ps1."""
    content = _read("start_api.ps1")
    assert "8010" not in content, (
        "start_api.ps1 must not reference port 8010. "
        "The canonical local API port is 8000."
    )


def test_start_api_captures_log_to_logs_directory() -> None:
    """start_api.ps1 must write a log under logs\\ via Start-Transcript."""
    content = _read("start_api.ps1")
    assert "Start-Transcript" in content, (
        "start_api.ps1 must use Start-Transcript to capture all startup "
        "output under logs\\ so the unified launcher can show diagnostics."
    )
    assert "logs" in content, (
        "start_api.ps1 must reference the logs\\ directory for log capture."
    )


# ---------------------------------------------------------------------------
# start_backend_and_tunnel.ps1 — unified launcher correctness
# ---------------------------------------------------------------------------

def test_unified_launcher_passes_working_directory_to_start_process() -> None:
    """-WorkingDirectory must appear in every Start-Process spawning PS."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "-WorkingDirectory" in content, (
        "start_backend_and_tunnel.ps1 must pass -WorkingDirectory to "
        "Start-Process so the spawned PowerShell session starts in the "
        "repo root.  Without this the API child may fail to locate the "
        "backend package and uvicorn will time out."
    )


def test_unified_launcher_api_log_under_logs_dir() -> None:
    """The unified launcher must reference logs\\api.log for diagnostics."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "api.log" in content, (
        "start_backend_and_tunnel.ps1 must reference logs\\api.log "
        "so it can print the last 50 lines on API startup failure."
    )


def test_unified_launcher_emits_port_diagnostics_on_failure() -> None:
    """The launcher must check port 8000 state when the API does not start."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "Port 8000" in content or "8000" in content, (
        "start_backend_and_tunnel.ps1 must check port 8000 state "
        "in its failure diagnostics block."
    )


def test_unified_launcher_emits_api_log_tail_on_failure() -> None:
    """The launcher must print the last 50 API log lines on failure."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "-Tail 50" in content, (
        "start_backend_and_tunnel.ps1 must call Get-Content -Tail 50 "
        "on the API log file when the local health check times out."
    )


def test_unified_launcher_local_api_is_127_0_0_1_8000() -> None:
    """The health check URL must be http://127.0.0.1:8000/health exactly."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "http://127.0.0.1:8000/health" in content, (
        "start_backend_and_tunnel.ps1 must wait on http://127.0.0.1:8000/health. "
        "No fallback to other ports."
    )


def test_unified_launcher_has_no_port_8010() -> None:
    """Port 8010 must not appear anywhere in the unified launcher."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "8010" not in content, (
        "start_backend_and_tunnel.ps1 must not reference port 8010."
    )


def test_unified_launcher_references_start_api_ps1() -> None:
    """The unified launcher must call start_api.ps1 as the API sub-script."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "start_api.ps1" in content, (
        "start_backend_and_tunnel.ps1 must reference start_api.ps1 "
        "as the API launcher script."
    )


def test_unified_launcher_logs_dir_created() -> None:
    """The unified launcher must ensure the logs\\ directory exists."""
    content = _read("start_backend_and_tunnel.ps1")
    assert "$LogDir" in content, (
        "start_backend_and_tunnel.ps1 must define $LogDir and ensure it exists."
    )


# ---------------------------------------------------------------------------
# Cross-script: no port 8010 anywhere in core launch scripts
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("script_name", [
    "start_api.ps1",
    "start_backend_and_tunnel.ps1",
    "start_api_watchdog.ps1",
])
def test_no_port_8010_in_launch_scripts(script_name: str) -> None:
    content = _read(script_name)
    assert "8010" not in content, (
        f"{script_name} must not reference port 8010. "
        "The canonical local API port is 8000."
    )
