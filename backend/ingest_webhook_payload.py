"""
Webhook payload archival module.

Accepts a validated webhook JSON dict and persists it to
``data/webhook_ingest/`` inside the repository.  Every file is named:

    {strategy_id}__{release_version}__{utc_timestamp}.json

so the complete history of ingest files is queryable by strategy, release,
and time without loading any database.

This module has **no side effects beyond writing files** – it does not patch
Pine, does not trigger downstream pipelines, and does not modify any
existing ingest tables.  Its sole job is durable archival.
"""

from __future__ import annotations

import datetime
import json
import os
import re
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_INGEST_DIR = _PROJECT_ROOT / "data" / "webhook_ingest"


def _resolve_ingest_dir() -> Path:
    """Return the directory where archived payloads are written.

    Overridable via the ``WEBHOOK_INGEST_DIR`` environment variable so that
    tests can redirect writes to a tmp directory without monkeypatching
    module globals.
    """
    configured = os.getenv("WEBHOOK_INGEST_DIR")
    return Path(configured) if configured else _DEFAULT_INGEST_DIR


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SAFE_CHARS_RE = re.compile(r"[^\w.\-]")


def _safe_component(value: str) -> str:
    """Sanitise a payload field value so it is safe to embed in a filename."""
    cleaned = _SAFE_CHARS_RE.sub("_", str(value or "").strip())
    return cleaned or "unknown"


def _utc_timestamp() -> str:
    """Return a filesystem-safe UTC timestamp string: ``YYYY-MM-DDTHH-MM-SS``."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


def _build_filename(payload: dict[str, Any]) -> str:
    strategy = _safe_component(payload.get("strategy_id") or "unknown_strategy")
    release = _safe_component(payload.get("release_version") or "unknown_release")
    ts = _utc_timestamp()
    return f"{strategy}__{release}__{ts}.json"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def archive_payload(payload: dict[str, Any]) -> Path:
    """Write *payload* to an archival JSON file and return its absolute path.

    The file is written atomically (write to ``.tmp`` then rename) so a
    partially-written file is never left behind.

    Args:
        payload: A dict representing the webhook JSON body.  Any JSON-
            serialisable dict is accepted; the only fields used for naming
            are ``strategy_id`` and ``release_version``.

    Returns:
        The :class:`~pathlib.Path` of the written file.
    """
    ingest_dir = _resolve_ingest_dir()
    ingest_dir.mkdir(parents=True, exist_ok=True)

    filename = _build_filename(payload)
    dest = ingest_dir / filename

    tmp = dest.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(dest)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    return dest


def ingest(payload_json: str) -> dict[str, Any]:
    """Parse *payload_json* and archive the result.

    This is the high-level entry point used by the HTTP endpoint and by
    CLI tooling.  It returns a lightweight summary dict that callers can
    use for logging or HTTP responses.

    Args:
        payload_json: Raw JSON string (the request body).

    Returns:
        A dict with keys ``status``, ``written_to``, ``strategy_id``,
        ``release_version``, and ``batch_id``.

    Raises:
        ValueError: If *payload_json* is not valid JSON.
    """
    try:
        payload: dict[str, Any] = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON payload: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object (dict), not a list or scalar")

    file_path = archive_payload(payload)

    return {
        "status": "archived",
        "written_to": str(file_path),
        "strategy_id": payload.get("strategy_id"),
        "release_version": payload.get("release_version"),
        "batch_id": payload.get("batch_id"),
    }
