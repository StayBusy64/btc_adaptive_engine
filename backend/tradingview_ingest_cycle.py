from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from backend.market_bias_engine import compute_and_store_signal_bias
from backend.signal_outcome_engine import (
    get_outcome_engine_defaults,
    record_signal_snapshots,
    run_signal_outcome_evaluation_once,
)
from backend.tradingview_ingest_service import normalize_batch_record, persist_replay
from backend.tradingview_ingest_storage import (
    archive_consumed_active_file,
    count_active_batch_files,
    list_active_batch_files,
    load_active_batch,
    persist_normalized_events,
    record_failed_batch,
    update_batch_status,
)

logger = logging.getLogger(__name__)

_cycle_lock = Lock()
_cycle_task: Optional[asyncio.Task[Any]] = None


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _scheduler_enabled() -> bool:
    raw = str(os.getenv("TRADINGVIEW_INGEST_SCHEDULER_ENABLED", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _cycle_interval_seconds() -> int:
    raw = os.getenv("TRADINGVIEW_INGEST_CYCLE_SECONDS", "3600")
    try:
        parsed = int(raw)
    except ValueError:
        return 3600
    return max(30, parsed)


def _extract_batch_id_from_path(path: Path) -> str:
    stem = path.stem
    if "__" in stem:
        return stem.split("__", maxsplit=1)[1]
    return stem


def _process_signal_analytics(
    *,
    batch_id: str,
    normalized_events: list[dict[str, Any]],
    outcome_defaults: dict[str, Any],
) -> tuple[int, int, int]:
    if not normalized_events:
        return 0, 0, 0

    try:
        snapshot_result = record_signal_snapshots(normalized_events=normalized_events)
        signal_ids = [str(value) for value in snapshot_result.get("signal_ids") or []]
        if not signal_ids:
            return int(snapshot_result.get("written_count") or 0), 0, 0

        outcome_result = run_signal_outcome_evaluation_once(
            min_future_bars=int(outcome_defaults["min_future_bars"]),
            continuation_threshold_pct=float(outcome_defaults["continuation_threshold_pct"]),
            horizon_bars=tuple(outcome_defaults["horizon_bars"]),
        )

        bias_result = compute_and_store_signal_bias(signal_ids=signal_ids)
        return (
            int(snapshot_result.get("written_count") or 0),
            int(outcome_result.get("evaluated_new_count") or 0),
            int(bias_result.get("computed_count") or 0),
        )
    except Exception:
        logger.exception(
            "Signal outcome/bias processing failed for batch_id=%s",
            batch_id,
        )
        return 0, 0, 0


def run_ingest_cycle_once(*, trigger: str) -> dict[str, Any]:
    with _cycle_lock:
        cycle_started_at = _utc_now_iso()
        scanned_files = 0
        processed_batches = 0
        failed_batches = 0
        normalized_events_written = 0
        duplicate_events = 0
        signal_journal_rows_written = 0
        signal_outcomes_written = 0
        market_bias_rows_written = 0

        outcome_defaults = get_outcome_engine_defaults()

        active_files = list_active_batch_files()
        for active_file in active_files:
            scanned_files += 1
            batch_id = _extract_batch_id_from_path(active_file)
            try:
                raw_record = load_active_batch(active_file)
                payload_obj = raw_record.get("payload")
                payload_dict = payload_obj if isinstance(payload_obj, dict) else {}
                batch_id = str(payload_dict.get("batch_id") or batch_id)

                normalized = normalize_batch_record(raw_record)
                write_result = persist_normalized_events(
                    batch_id=batch_id,
                    normalized_events=normalized,
                    overwrite=False,
                )
                normalized_events_written += int(write_result.get("written_events") or 0)
                duplicate_events += int(write_result.get("duplicate_events") or 0)
                journal_count, outcome_count, bias_count = _process_signal_analytics(
                    batch_id=batch_id,
                    normalized_events=normalized,
                    outcome_defaults=outcome_defaults,
                )
                signal_journal_rows_written += journal_count
                signal_outcomes_written += outcome_count
                market_bias_rows_written += bias_count

                used_path = archive_consumed_active_file(active_file)
                update_batch_status(
                    batch_id,
                    status="processed",
                    last_cycle_at=_utc_now_iso(),
                    used_path=used_path,
                    active_path=None,
                    last_error=None,
                )
                processed_batches += 1
            except Exception as exc:
                failed_path = record_failed_batch(
                    batch_id=batch_id,
                    active_path=active_file,
                    error_text=str(exc),
                )
                update_batch_status(
                    batch_id,
                    status="failed",
                    last_cycle_at=_utc_now_iso(),
                    last_error=str(exc),
                    failed_path=failed_path,
                )
                failed_batches += 1
                logger.exception("TradingView ingest cycle failed for batch_id=%s", batch_id)

        cycle_finished_at = _utc_now_iso()
        return {
            "trigger": trigger,
            "cycle_started_at": cycle_started_at,
            "cycle_finished_at": cycle_finished_at,
            "scanned_files": scanned_files,
            "processed_batches": processed_batches,
            "failed_batches": failed_batches,
            "normalized_events_written": normalized_events_written,
            "duplicate_events": duplicate_events,
            "signal_journal_rows_written": signal_journal_rows_written,
            "signal_outcomes_written": signal_outcomes_written,
            "market_bias_rows_written": market_bias_rows_written,
            "active_remaining": count_active_batch_files(),
        }


def replay_batch_once(*, batch_id: str, overwrite: bool) -> dict[str, Any]:
    return persist_replay(batch_id=batch_id, overwrite=overwrite)


async def _scheduler_loop() -> None:
    interval = _cycle_interval_seconds()
    while True:
        await asyncio.sleep(interval)
        try:
            run_ingest_cycle_once(trigger="scheduled")
        except Exception:
            logger.exception("Scheduled TradingView ingest cycle failed")


def start_ingest_cycle_scheduler() -> None:
    global _cycle_task

    if _cycle_task is not None:
        return
    if not _scheduler_enabled():
        return

    _cycle_task = asyncio.create_task(_scheduler_loop(), name="tradingview-ingest-cycle")


async def stop_ingest_cycle_scheduler() -> None:
    global _cycle_task

    task = _cycle_task
    _cycle_task = None
    if task is None:
        return

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
