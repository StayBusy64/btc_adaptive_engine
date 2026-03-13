"""
Pipeline configuration integrity tests.

These tests prove that the TradingView pipeline cleanup is complete and correct:
- pipeline_config.json exists and carries all required canonical fields
- The canonical local API port is 8000 (not the stale 8010)
- The canonical batch endpoint path is /webhooks/tradingview/batch
- All required pipeline scripts exist (PS1 helpers, verifier, autoverify)
- No stale port 8010 references remain in scripts, docs, or config files
"""

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Required fields and their expected values in pipeline_config.json
_REQUIRED_CONFIG_FIELDS = {
    "local_api_base": "http://127.0.0.1:8000",
    "local_health": "http://127.0.0.1:8000/health",
    "backend_base": "https://api.dopedreamspnl.com",
    "backend_health": "https://api.dopedreamspnl.com/health",
    "batch_path": "/webhooks/tradingview/batch",
    "worker_base": "https://tv-webhook.staybusyent.workers.dev",
    "worker_secret_param": "secret",
    "signal_key_query_param": "signal_key",
    "signal_key_file": "data/tv_ingest/signal_key.txt",
}

# Pipeline scripts that must exist for the e2e verifier and autoverify watcher
_REQUIRED_PIPELINE_FILES = [
    "pipeline_config.json",
    "tools/pipeline_common.ps1",
    "verify-pipeline-e2e.ps1",
    "install-pipeline-autoverify.ps1",
    "tools/pipeline_autoverify.ps1",
]

# File extensions to scan for stale port references
_SCAN_EXTENSIONS = (
    "*.ps1",
    "*.py",
    "*.md",
    "*.json",
    "*.txt",
    "*.yaml",
    "*.yml",
    "*.pine",
)


# ---------------------------------------------------------------------------
# pipeline_config.json
# ---------------------------------------------------------------------------


def test_pipeline_config_json_exists():
    config_path = REPO_ROOT / "pipeline_config.json"
    assert config_path.exists(), "pipeline_config.json must exist at the repo root"


def test_pipeline_config_json_is_valid_json():
    config_path = REPO_ROOT / "pipeline_config.json"
    text = config_path.read_text(encoding="utf-8")
    cfg = json.loads(text)
    assert isinstance(cfg, dict), "pipeline_config.json must be a JSON object"


@pytest.mark.parametrize("field,expected_value", list(_REQUIRED_CONFIG_FIELDS.items()))
def test_pipeline_config_field(field, expected_value):
    config_path = REPO_ROOT / "pipeline_config.json"
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    assert field in cfg, f"pipeline_config.json is missing required field: {field!r}"
    assert cfg[field] == expected_value, (
        f"pipeline_config.json field {field!r} = {cfg[field]!r}, expected {expected_value!r}"
    )


def test_pipeline_config_local_api_uses_port_8000():
    config_path = REPO_ROOT / "pipeline_config.json"
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    assert ":8000" in cfg.get("local_api_base", ""), (
        "local_api_base must use port 8000"
    )
    assert ":8010" not in cfg.get("local_api_base", ""), (
        "local_api_base must not reference stale port 8010"
    )


def test_pipeline_config_batch_path_uses_plural_webhooks():
    config_path = REPO_ROOT / "pipeline_config.json"
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    batch_path = cfg.get("batch_path", "")
    assert batch_path == "/webhooks/tradingview/batch", (
        f"batch_path must be /webhooks/tradingview/batch, got {batch_path!r}"
    )


# ---------------------------------------------------------------------------
# Required pipeline script files
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rel_path", _REQUIRED_PIPELINE_FILES)
def test_required_pipeline_file_exists(rel_path):
    full_path = REPO_ROOT / rel_path
    assert full_path.exists(), (
        f"Required pipeline file is missing: {rel_path}"
    )


def test_pipeline_common_ps1_is_nonempty():
    path = REPO_ROOT / "tools" / "pipeline_common.ps1"
    content = path.read_text(encoding="utf-8")
    assert len(content.strip()) > 100, "tools/pipeline_common.ps1 appears to be empty or trivial"


def test_verify_pipeline_e2e_ps1_is_nonempty():
    path = REPO_ROOT / "verify-pipeline-e2e.ps1"
    content = path.read_text(encoding="utf-8")
    assert len(content.strip()) > 100, "verify-pipeline-e2e.ps1 appears to be empty or trivial"


# ---------------------------------------------------------------------------
# No stale port 8010 references
# ---------------------------------------------------------------------------


def _collect_files_with_8010():
    """Return list of (relative_path, line_number, line) for any line containing '8010'.

    The test file itself is excluded because it necessarily references "8010"
    as a literal string for the purpose of checking against it.
    """
    this_file = Path(__file__).resolve()
    hits = []
    for ext in _SCAN_EXTENSIONS:
        for p in REPO_ROOT.rglob(ext):
            # Skip hidden directories (e.g. .git)
            if any(part.startswith(".") for part in p.parts):
                continue
            # Skip this test file — it legitimately contains "8010" strings
            if p.resolve() == this_file:
                continue
            try:
                for lineno, line in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
                    if "8010" in line:
                        hits.append((str(p.relative_to(REPO_ROOT)), lineno, line.strip()))
            except OSError:
                pass
    return hits


def test_no_stale_port_8010_references():
    hits = _collect_files_with_8010()
    if hits:
        details = "\n".join(f"  {path}:{lineno}: {line}" for path, lineno, line in hits)
        pytest.fail(f"Stale port 8010 references found:\n{details}")


# ---------------------------------------------------------------------------
# pipeline_common.ps1 exposes required helper functions
# ---------------------------------------------------------------------------


def test_pipeline_common_defines_get_pipeline_config():
    content = (REPO_ROOT / "tools" / "pipeline_common.ps1").read_text(encoding="utf-8")
    assert "function Get-PipelineConfig" in content, (
        "tools/pipeline_common.ps1 must define Get-PipelineConfig"
    )


def test_pipeline_common_defines_get_pipeline_worker_url():
    content = (REPO_ROOT / "tools" / "pipeline_common.ps1").read_text(encoding="utf-8")
    assert "Get-PipelineWorkerUrl" in content, (
        "tools/pipeline_common.ps1 must define/reference Get-PipelineWorkerUrl"
    )


# ---------------------------------------------------------------------------
# verify-pipeline-e2e.ps1 covers all required stages
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stage", [
    "Worker accepted",
    "Backend reachable",
    "Local API reachable",
    "Batch persisted",
    "Batch processed",
    "Journal advanced",
])
def test_verify_pipeline_e2e_covers_stage(stage):
    content = (REPO_ROOT / "verify-pipeline-e2e.ps1").read_text(encoding="utf-8")
    assert stage in content, (
        f"verify-pipeline-e2e.ps1 must reference stage: {stage!r}"
    )


# ---------------------------------------------------------------------------
# install-pipeline-autoverify.ps1 and tools/pipeline_autoverify.ps1
# ---------------------------------------------------------------------------


def test_install_pipeline_autoverify_references_autoverify_script():
    content = (REPO_ROOT / "install-pipeline-autoverify.ps1").read_text(encoding="utf-8")
    assert "pipeline_autoverify" in content, (
        "install-pipeline-autoverify.ps1 must reference tools/pipeline_autoverify.ps1"
    )


def test_pipeline_autoverify_references_pipeline_common():
    content = (REPO_ROOT / "tools" / "pipeline_autoverify.ps1").read_text(encoding="utf-8")
    assert "pipeline_common" in content, (
        "tools/pipeline_autoverify.ps1 must dot-source tools/pipeline_common.ps1"
    )
