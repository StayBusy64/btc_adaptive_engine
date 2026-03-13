"""
path_config.py – Single source of truth for repo-wide path and endpoint constants.

All path-sensitive modules should import from here rather than re-deriving
these values independently.  This module has zero intra-package dependencies
(only stdlib) so it is safe to import early and from scripts as well as
application code.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Repo root – derived from this file's own location (backend/path_config.py).
# `parents[1]` navigates one level up from `backend/` to the repository root,
# resolving correctly regardless of the current working directory.
# ---------------------------------------------------------------------------
REPO_ROOT: Path = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Canonical webhook endpoint paths
# ---------------------------------------------------------------------------

# Production batch-ingest endpoint – used by Cloudflare Worker → FastAPI.
# This is the authoritative path; all scripts, docs, and tests must agree.
WEBHOOK_BATCH_PATH: str = "/webhooks/tradingview/batch"

# Legacy Phase-1 single-event endpoint (still present in api_server.py but
# NOT the target for the Worker).  Kept here as a named constant so any
# accidental references are obvious.
WEBHOOK_PHASE1_PATH: str = "/webhooks/tradingview"

# ---------------------------------------------------------------------------
# Local data directories (relative to REPO_ROOT)
# ---------------------------------------------------------------------------
DATA_DIR: Path = REPO_ROOT / "data"
TV_INGEST_DIR: Path = DATA_DIR / "tv_ingest"
TV_INGEST_PENDING_DIR: Path = TV_INGEST_DIR / "pending"
TV_INGEST_PROCESSED_DIR: Path = TV_INGEST_DIR / "processed"
STATE_DIR: Path = DATA_DIR / "state"
LOGS_DIR: Path = DATA_DIR / "logs"

# ---------------------------------------------------------------------------
# Environment-variable name constants
# Use these instead of bare string literals to avoid typo-driven mismatches.
# ---------------------------------------------------------------------------

# Primary auth key for the batch ingest endpoint (set in Cloudflare Worker).
ENV_INGEST_SIGNAL_KEY: str = "TRADINGVIEW_INGEST_SIGNAL_KEY"

# Auth key for the direct Phase-1 webhook endpoint.
ENV_SIGNAL_WEBHOOK_KEY: str = "SIGNAL_WEBHOOK_KEY"

# Legacy alias accepted alongside ENV_INGEST_SIGNAL_KEY.
ENV_TV_SIGNAL_KEY: str = "TV_SIGNAL_KEY"

# Public base URL used by tradingview_webhook_setup.py to print the
# permanent webhook URL.
ENV_PUBLIC_BASE_URL: str = "TRADINGVIEW_PUBLIC_BASE_URL"

# Host / port for local dev server.
ENV_WEBHOOK_HOST: str = "TRADINGVIEW_WEBHOOK_HOST"
ENV_WEBHOOK_PORT: str = "TRADINGVIEW_WEBHOOK_PORT"
