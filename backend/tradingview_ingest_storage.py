from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INGEST_ROOT = PROJECT_ROOT / "data"


@dataclass(frozen=True)
class IngestPaths:
    root: Path
    intake_active_buy: Path
    intake_active_sell: Path
    intake_active_mixed: Path
    cache_used: Path
    cache_failed: Path
    state_raw: Path
    normalized_signal_events: Path
    normalized_micro_context: Path
    normalized_macro_context: Path
    normalized_processing_status: Path
    state_refined: Path
    state_outcomes: Path
    state_selector_audits: Path
    state_policy_scores: Path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_component(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value.strip())
    return cleaned or "unknown"


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_root() -> Path:
    configured = os.getenv("TRADINGVIEW_INGEST_ROOT")
    return Path(configured) if configured else DEFAULT_INGEST_ROOT


def _resolve_signal_log_file(root: Path) -> Path:
    configured = os.getenv("TRADINGVIEW_SIGNAL_LOG_FILE")
    if configured:
        return Path(configured)
    return root / "tv_ingest" / "signals.log"


def get_ingest_paths() -> IngestPaths:
    root = _resolve_root()
    return IngestPaths(
        root=root,
        intake_active_buy=root / "intake" / "active" / "buy",
        intake_active_sell=root / "intake" / "active" / "sell",
        intake_active_mixed=root / "intake" / "active" / "mixed",
        cache_used=root / "cache" / "used",
        cache_failed=root / "cache" / "failed",
        state_raw=root / "state" / "raw",
        normalized_signal_events=root / "state" / "normalized" / "signal_events",
        normalized_micro_context=root / "state" / "normalized" / "micro_context",
        normalized_macro_context=root / "state" / "normalized" / "macro_context",
        normalized_processing_status=root / "state" / "normalized" / "processing_status",
        state_refined=root / "state" / "refined",
        state_outcomes=root / "state" / "outcomes",
        state_selector_audits=root / "state" / "selector_audits",
        state_policy_scores=root / "state" / "policy_scores",
    )


def ensure_ingest_directories() -> IngestPaths:
    paths = get_ingest_paths()
    for path in (
        paths.intake_active_buy,
        paths.intake_active_sell,
        paths.intake_active_mixed,
        paths.cache_used,
        paths.cache_failed,
        paths.state_raw,
        paths.normalized_signal_events,
        paths.normalized_micro_context,
        paths.normalized_macro_context,
        paths.normalized_processing_status,
        paths.state_refined,
        paths.state_outcomes,
        paths.state_selector_audits,
        paths.state_policy_scores,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return paths


def _status_path(paths: IngestPaths, batch_id: str) -> Path:
    return paths.normalized_processing_status / f"{_safe_component(batch_id)}.json"


def _raw_path(paths: IngestPaths, batch_id: str) -> Path:
    return paths.state_raw / f"{_safe_component(batch_id)}.json"


def _active_bucket(paths: IngestPaths, trigger_side: str) -> Path:
    normalized = str(trigger_side or "").strip().lower()
    if normalized in {"buy", "long"}:
        return paths.intake_active_buy
    if normalized in {"sell", "short"}:
        return paths.intake_active_sell
    return paths.intake_active_mixed


def _append_signal_log_entry(
    *,
    paths: IngestPaths,
    payload: dict[str, Any],
    payload_hash: str,
    source_ip: Optional[str],
    duplicate: bool,
) -> None:
    log_file = _resolve_signal_log_file(paths.root)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "received_at": _utc_now_iso(),
        "batch_id": str(payload.get("batch_id") or ""),
        "payload_hash": payload_hash,
        "source_ip": source_ip,
        "duplicate": duplicate,
        "payload": payload,
    }
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False))
        handle.write("\n")


def _event_path(paths: IngestPaths, event_id: str) -> Path:
    return paths.normalized_signal_events / f"{_safe_component(event_id)}.json"


def _micro_path(paths: IngestPaths, event_id: str) -> Path:
    return paths.normalized_micro_context / f"{_safe_component(event_id)}.json"


def _macro_path(paths: IngestPaths, event_id: str) -> Path:
    return paths.normalized_macro_context / f"{_safe_component(event_id)}.json"


def save_raw_batch_to_active(
    *,
    payload: dict[str, Any],
    payload_hash: str,
    source_ip: Optional[str],
    headers: dict[str, str],
) -> dict[str, Any]:
    paths = ensure_ingest_directories()
    batch_id = str(payload.get("batch_id") or "")
    if not batch_id:
        raise ValueError("payload batch_id is required")

    status_file = _status_path(paths, batch_id)
    if status_file.exists():
        existing_status = _read_json(status_file)
        try:
            _append_signal_log_entry(
                paths=paths,
                payload=payload,
                payload_hash=payload_hash,
                source_ip=source_ip,
                duplicate=True,
            )
        except OSError:
            pass
        return {
            "duplicate": True,
            "batch_id": batch_id,
            "received_at": str(existing_status.get("received_at") or _utc_now_iso()),
            "status": existing_status,
            "active_path": existing_status.get("active_path"),
            "raw_path": existing_status.get("raw_path"),
        }

    received_at = _utc_now_iso()
    batch_key = _safe_component(batch_id)
    active_bucket = _active_bucket(paths, str(payload.get("batch_trigger_side") or "mixed"))
    active_name = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}__{batch_key}.json"
    active_path = active_bucket / active_name
    raw_path = _raw_path(paths, batch_id)

    raw_record = {
        "batch_id": batch_id,
        "received_at": received_at,
        "payload_hash": payload_hash,
        "source_ip": source_ip,
        "headers": headers,
        "payload": payload,
    }

    _atomic_write_json(raw_path, raw_record)
    _atomic_write_json(active_path, raw_record)

    try:
        _append_signal_log_entry(
            paths=paths,
            payload=payload,
            payload_hash=payload_hash,
            source_ip=source_ip,
            duplicate=False,
        )
    except OSError:
        pass

    status_payload = {
        "batch_id": batch_id,
        "status": "queued",
        "received_at": received_at,
        "updated_at": received_at,
        "event_count": len(payload.get("events") or []),
        "raw_path": str(raw_path),
        "active_path": str(active_path),
        "used_path": None,
        "last_error": None,
        "last_cycle_at": None,
        "payload_hash": payload_hash,
    }
    _atomic_write_json(status_file, status_payload)

    return {
        "duplicate": False,
        "batch_id": batch_id,
        "received_at": received_at,
        "status": status_payload,
        "active_path": str(active_path),
        "raw_path": str(raw_path),
    }


def update_batch_status(batch_id: str, **updates: Any) -> dict[str, Any]:
    paths = ensure_ingest_directories()
    status_file = _status_path(paths, batch_id)
    existing = _read_json(status_file) if status_file.exists() else {"batch_id": batch_id}
    existing.update(updates)
    existing["updated_at"] = _utc_now_iso()
    _atomic_write_json(status_file, existing)
    return existing


def list_recent_batches(limit: int, *, status: Optional[str] = None) -> list[dict[str, Any]]:
    paths = ensure_ingest_directories()
    safe_limit = max(1, min(int(limit), 500))
    rows: list[dict[str, Any]] = []

    for status_file in paths.normalized_processing_status.glob("*.json"):
        try:
            row = _read_json(status_file)
        except (json.JSONDecodeError, OSError):
            continue
        if status is not None and str(row.get("status")) != status:
            continue
        active_path = row.get("active_path")
        row["active_exists"] = bool(active_path and Path(active_path).exists())
        rows.append(row)

    rows.sort(
        key=lambda row: str(row.get("updated_at") or row.get("received_at") or ""),
        reverse=True,
    )
    return rows[:safe_limit]


def get_batch(batch_id: str) -> Optional[dict[str, Any]]:
    paths = ensure_ingest_directories()
    raw_file = _raw_path(paths, batch_id)
    status_file = _status_path(paths, batch_id)

    if not raw_file.exists() and not status_file.exists():
        return None

    raw_payload = _read_json(raw_file) if raw_file.exists() else None
    status_payload = _read_json(status_file) if status_file.exists() else None
    return {
        "batch_id": batch_id,
        "raw": raw_payload,
        "status": status_payload,
    }


def list_active_batch_files() -> list[Path]:
    paths = ensure_ingest_directories()
    files = [
        *paths.intake_active_buy.glob("*.json"),
        *paths.intake_active_sell.glob("*.json"),
        *paths.intake_active_mixed.glob("*.json"),
    ]
    files.sort(key=lambda item: item.stat().st_mtime)
    return files


def count_active_batch_files() -> int:
    return len(list_active_batch_files())


def load_active_batch(path: Path) -> dict[str, Any]:
    return _read_json(path)


def archive_consumed_active_file(active_path: Path) -> str:
    paths = ensure_ingest_directories()
    used_path = paths.cache_used / active_path.name
    used_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(active_path), str(used_path))
    return str(used_path)


def record_failed_batch(*, batch_id: str, active_path: Optional[Path], error_text: str) -> str:
    paths = ensure_ingest_directories()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    failed_name = f"{timestamp}__{_safe_component(batch_id)}.json"
    failed_path = paths.cache_failed / failed_name

    failed_payload: dict[str, Any] = {
        "batch_id": batch_id,
        "failed_at": _utc_now_iso(),
        "error": error_text,
    }
    if active_path is not None and active_path.exists():
        failed_payload["active_path"] = str(active_path)
        try:
            failed_payload["raw_record"] = _read_json(active_path)
        except (json.JSONDecodeError, OSError):
            failed_payload["raw_record"] = None

    _atomic_write_json(failed_path, failed_payload)
    return str(failed_path)


def persist_normalized_events(
    *,
    batch_id: str,
    normalized_events: list[dict[str, Any]],
    overwrite: bool = False,
) -> dict[str, int]:
    paths = ensure_ingest_directories()
    written_events = 0
    duplicate_events = 0

    for normalized in normalized_events:
        event_id = str(normalized.get("event_id") or "")
        if not event_id:
            continue

        event_path = _event_path(paths, event_id)
        if event_path.exists() and not overwrite:
            duplicate_events += 1
            continue

        event_payload = dict(normalized)
        micro_payload = {
            "event_id": event_id,
            "batch_id": batch_id,
            "captured_at": _utc_now_iso(),
            "micro": dict(normalized.get("micro_context") or {}),
        }
        macro_payload = {
            "event_id": event_id,
            "batch_id": batch_id,
            "captured_at": _utc_now_iso(),
            "macro": dict(normalized.get("macro_context") or {}),
        }

        _atomic_write_json(event_path, event_payload)
        _atomic_write_json(_micro_path(paths, event_id), micro_payload)
        _atomic_write_json(_macro_path(paths, event_id), macro_payload)
        written_events += 1

    return {
        "written_events": written_events,
        "duplicate_events": duplicate_events,
    }


def list_recent_events(
    limit: int,
    *,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    signal_type: Optional[str] = None,
    confirmed: Optional[bool] = None,
) -> list[dict[str, Any]]:
    paths = ensure_ingest_directories()
    safe_limit = max(1, min(int(limit), 500))
    rows: list[dict[str, Any]] = []

    for event_file in paths.normalized_signal_events.glob("*.json"):
        try:
            row = _read_json(event_file)
        except (json.JSONDecodeError, OSError):
            continue

        if symbol is not None and str(row.get("symbol")) != symbol:
            continue
        if side is not None and str(row.get("side")) != side:
            continue
        if signal_type is not None and str(row.get("signal_type")) != signal_type:
            continue
        if confirmed is not None and bool(row.get("confirmed")) != confirmed:
            continue

        rows.append(row)

    rows.sort(key=lambda row: int(row.get("event_time") or 0), reverse=True)
    return rows[:safe_limit]


def get_event(event_id: str) -> Optional[dict[str, Any]]:
    paths = ensure_ingest_directories()
    event_file = _event_path(paths, event_id)
    if not event_file.exists():
        return None

    event_row = _read_json(event_file)
    micro_file = _micro_path(paths, event_id)
    macro_file = _macro_path(paths, event_id)

    return {
        "event": event_row,
        "micro": _read_json(micro_file) if micro_file.exists() else None,
        "macro": _read_json(macro_file) if macro_file.exists() else None,
    }


def load_raw_batch_for_replay(batch_id: str) -> Optional[dict[str, Any]]:
    paths = ensure_ingest_directories()
    raw_file = _raw_path(paths, batch_id)
    if not raw_file.exists():
        return None
    return _read_json(raw_file)
