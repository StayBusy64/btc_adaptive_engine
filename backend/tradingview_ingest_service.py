from __future__ import annotations

import json
import os
from hashlib import sha256
from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException, status

from backend.tradingview_bridge_manifest import classify_research_fields, resolve_release_context
from backend.tradingview_ingest_models import TradingViewBatchPayload
from backend.tradingview_ingest_storage import (
    count_active_batch_files,
    get_batch,
    get_event,
    get_ingest_paths,
    list_recent_batches,
    list_recent_events,
    load_raw_batch_for_replay,
    persist_normalized_events,
    save_raw_batch_to_active,
    update_batch_status,
)

DEFAULT_MAX_PAYLOAD_BYTES = 512 * 1024


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _resolve_receipt_log_file() -> Path:
    configured = os.getenv("TRADINGVIEW_INGEST_RECEIPT_LOG_FILE")
    if configured:
        return Path(configured)
    return get_ingest_paths().root / "logs" / "tv_ingest_receipts.jsonl"


def _append_ingest_receipt_log(
    *,
    received_at: str,
    batch_id: str,
    symbol: str,
    chart_tf: str,
    batch_trigger_side: str,
    event_count: int,
    auth_result: str,
    parse_result: str,
    write_result: str,
    status: str,
    payload_size_bytes: int,
    source_ip: Optional[str],
    detail: Optional[str] = None,
) -> None:
    row: dict[str, Any] = {
        "received_at": received_at,
        "batch_id": batch_id,
        "symbol": symbol,
        "chart_tf": chart_tf,
        "batch_trigger_side": batch_trigger_side,
        "event_count": event_count,
        "auth_result": auth_result,
        "parse_result": parse_result,
        "write_result": write_result,
        "status": status,
        "payload_size_bytes": payload_size_bytes,
        "source_ip": source_ip,
    }
    if detail:
        row["detail"] = detail

    log_file = _resolve_receipt_log_file()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False))
        handle.write("\n")


def _resolve_expected_signal_key() -> str:
    return (
        os.getenv("TRADINGVIEW_INGEST_SIGNAL_KEY")
        or os.getenv("SIGNAL_WEBHOOK_KEY")
        or os.getenv("TV_SIGNAL_KEY")
        or "change-me-now"
    )


def _resolve_max_payload_bytes() -> int:
    configured = os.getenv("TRADINGVIEW_INGEST_MAX_PAYLOAD_BYTES")
    if configured is None:
        return DEFAULT_MAX_PAYLOAD_BYTES
    try:
        parsed = int(configured)
    except ValueError:
        return DEFAULT_MAX_PAYLOAD_BYTES
    return max(1024, parsed)


def _normalize_event_side(side: Any) -> str:
    cleaned = str(side or "").strip().lower()
    alias_map = {
        "buy": "long",
        "sell": "short",
    }
    normalized = alias_map.get(cleaned, cleaned)
    if normalized not in {"long", "short"}:
        return "long"
    return normalized


def enforce_payload_size(payload_size_bytes: int) -> None:
    max_bytes = _resolve_max_payload_bytes()
    if payload_size_bytes > max_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"payload exceeds max size of {max_bytes} bytes",
        )


def validate_signal_key(
    *,
    query_signal_key: Optional[str],
    header_signal_key: Optional[str],
    source_ip: Optional[str],
) -> None:
    expected = _resolve_expected_signal_key()
    provided = query_signal_key if query_signal_key is not None else header_signal_key

    if provided is None or provided.strip() != expected:
        try:
            _append_ingest_receipt_log(
                received_at=_utc_now_iso(),
                batch_id="",
                symbol="",
                chart_tf="",
                batch_trigger_side="",
                event_count=0,
                auth_result="failed",
                parse_result="unknown",
                write_result="skipped",
                status="rejected_unauthorized",
                payload_size_bytes=0,
                source_ip=source_ip,
                detail="invalid signal key",
            )
        except OSError:
            pass
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid signal key")


def accept_tradingview_batch(
    *,
    payload: TradingViewBatchPayload,
    payload_size_bytes: int,
    query_signal_key: Optional[str],
    header_signal_key: Optional[str],
    source_ip: Optional[str],
    request_headers: dict[str, str],
) -> dict[str, Any]:
    enforce_payload_size(payload_size_bytes)
    validate_signal_key(
        query_signal_key=query_signal_key,
        header_signal_key=header_signal_key,
        source_ip=source_ip,
    )

    payload_dict = payload.model_dump(mode="json")
    payload_hash = sha256(json.dumps(payload_dict, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()

    receipt = save_raw_batch_to_active(
        payload=payload_dict,
        payload_hash=payload_hash,
        source_ip=source_ip,
        headers=request_headers,
    )

    duplicate_batch = bool(receipt.get("duplicate"))
    status_value = "duplicate" if duplicate_batch else "accepted"
    response_payload = {
        "status": status_value,
        "batch_id": payload_dict["batch_id"],
        "event_count": len(payload_dict.get("events") or []),
        "raw_saved": True,
        "queued_for_cycle": not duplicate_batch,
        "duplicate_batch": duplicate_batch,
        "received_at": receipt["received_at"],
    }

    try:
        _append_ingest_receipt_log(
            received_at=str(receipt["received_at"]),
            batch_id=str(payload_dict.get("batch_id") or ""),
            symbol=str(payload_dict.get("symbol") or ""),
            chart_tf=str(payload_dict.get("chart_tf") or ""),
            batch_trigger_side=str(payload_dict.get("batch_trigger_side") or ""),
            event_count=len(payload_dict.get("events") or []),
            auth_result="passed",
            parse_result="passed",
            write_result="duplicate" if duplicate_batch else "stored",
            status=status_value,
            payload_size_bytes=payload_size_bytes,
            source_ip=source_ip,
        )
    except OSError:
        pass

    return response_payload


def normalize_batch_record(raw_record: dict[str, Any]) -> list[dict[str, Any]]:
    payload = raw_record.get("payload") if isinstance(raw_record.get("payload"), dict) else {}
    batch_id = str(payload.get("batch_id") or "")
    symbol = str(payload.get("symbol") or "")
    chart_tf = str(payload.get("chart_tf") or "")
    batch_trigger_side = str(payload.get("batch_trigger_side") or "mixed")
    batch_close_time = payload.get("batch_close_time")
    source = str(payload.get("source") or "tradingview")
    namespace = str(payload.get("namespace") or "")
    received_at = raw_record.get("received_at")

    normalized: list[dict[str, Any]] = []
    for index, event in enumerate(payload.get("events") or []):
        if not isinstance(event, dict):
            continue

        event_side = str(event.get("side") or "")
        release_context = resolve_release_context(payload=payload, event=event)
        signal_name = str(event.get("signal_name") or event.get("signal_type") or "")
        strategy_id = str(event.get("strategy_id") or release_context.get("strategy_id") or "").strip() or None
        raw_research = event.get("research") if isinstance(event.get("research"), dict) else {}
        research_context, research_unknown_context = classify_research_fields(dict(raw_research))

        normalized.append(
            {
                "batch_id": batch_id,
                "event_id": str(event.get("event_id") or ""),
                "event_order": index,
                "event_time": int(event.get("event_time") or 0),
                "side": _normalize_event_side(event_side),
                "side_raw": event_side,
                "signal_type": str(event.get("signal_type") or ""),
                "signal_name": signal_name,
                "signal_family": str(event.get("signal_family") or ""),
                "strategy_id": strategy_id,
                "price": event.get("price"),
                "confirmed": bool(event.get("confirmed", payload.get("confirmed", False))),
                "symbol": symbol,
                "chart_tf": chart_tf,
                "batch_trigger_side": batch_trigger_side,
                "batch_close_time": batch_close_time,
                "source": source,
                "namespace": namespace,
                "received_at": received_at,
                "release_id": release_context.get("release_id"),
                "release_version": release_context.get("release_version"),
                "release_channel": release_context.get("release_channel"),
                "contract_version": release_context.get("contract_version"),
                "telemetry_schema_version": release_context.get("telemetry_schema_version"),
                "release_context": release_context,
                "micro_context": dict(event.get("micro") or {}),
                "macro_context": dict(event.get("macro") or {}),
                "research_context": research_context,
                "research_unknown_context": research_unknown_context,
            }
        )

    return normalized


def persist_replay(
    *,
    batch_id: str,
    overwrite: bool,
) -> dict[str, Any]:
    raw_record = load_raw_batch_for_replay(batch_id)
    if raw_record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="batch not found")

    normalized = normalize_batch_record(raw_record)
    write_result = persist_normalized_events(batch_id=batch_id, normalized_events=normalized, overwrite=overwrite)

    status_value = "replayed_overwrite" if overwrite else "replayed"
    status_payload = update_batch_status(
        batch_id,
        status=status_value,
        last_cycle_at=_utc_now_iso(),
        last_error=None,
    )

    return {
        "status": status_value,
        "batch_id": batch_id,
        "events_in_batch": len(normalized),
        "written_events": int(write_result.get("written_events") or 0),
        "duplicate_events": int(write_result.get("duplicate_events") or 0),
        "active_remaining": count_active_batch_files(),
        "batch_status": status_payload,
    }


def get_recent_batch_rows(limit: int, *, status: Optional[str] = None) -> list[dict[str, Any]]:
    return list_recent_batches(limit, status=status)


def get_recent_event_rows(
    limit: int,
    *,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    signal_type: Optional[str] = None,
    confirmed: Optional[bool] = None,
) -> list[dict[str, Any]]:
    normalized_side: Optional[str]
    if side is None:
        normalized_side = None
    else:
        normalized_side = _normalize_event_side(side)

    return list_recent_events(
        limit,
        symbol=symbol,
        side=normalized_side,
        signal_type=signal_type,
        confirmed=confirmed,
    )


def get_batch_by_id(batch_id: str) -> Optional[dict[str, Any]]:
    return get_batch(batch_id)


def get_event_by_id(event_id: str) -> Optional[dict[str, Any]]:
    return get_event(event_id)
