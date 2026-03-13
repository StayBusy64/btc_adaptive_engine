from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, Literal, Optional

from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field

app = FastAPI(title="TradingView Bridge Standalone", version="1.0.0")

SIGNAL_KEY = os.getenv("TV_SIGNAL_KEY", "change-me-now")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TradingViewWebhookPayload(BaseModel):
    source: str = Field(..., description="Expected to be tradingview")
    namespace: Optional[str] = None
    strategy_id: Optional[str] = None
    ticker: str
    tickerid: Optional[str] = None
    exchange: Optional[str] = None
    timeframe: str
    side: Literal["long", "short"]
    signal_name: str
    price: float
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None
    bar_time: int
    bar_index: Optional[int] = None
    fast_ema: Optional[float] = None
    slow_ema: Optional[float] = None
    rsi: Optional[float] = None
    signal_key: Optional[str] = Field(default=None, exclude=True)


def resolve_signal_key(
    *,
    query_signal_key: Optional[str],
    body_signal_key: Optional[str],
    header_signal_key: Optional[str],
) -> Optional[str]:
    for candidate in (query_signal_key, body_signal_key, header_signal_key):
        if candidate is None:
            continue
        cleaned = candidate.strip()
        if cleaned:
            return cleaned
    return None


def normalize_tradingview_payload(payload: TradingViewWebhookPayload) -> Dict[str, Any]:
    symbol = payload.ticker.upper().strip()
    side = payload.side.lower().strip()

    normalized = {
        "event_type": "tradingview_alert",
        "source": payload.source,
        "namespace": payload.namespace or "default",
        "strategy_id": payload.strategy_id or "unknown_strategy",
        "symbol": symbol,
        "tickerid": payload.tickerid,
        "exchange": payload.exchange,
        "timeframe": payload.timeframe,
        "side": side,
        "signal_name": payload.signal_name,
        "market_price": payload.price,
        "bar_time": payload.bar_time,
        "received_at": utc_now_iso(),
        "features": {
            "open": payload.open,
            "high": payload.high,
            "low": payload.low,
            "volume": payload.volume,
            "bar_index": payload.bar_index,
            "fast_ema": payload.fast_ema,
            "slow_ema": payload.slow_ema,
            "rsi": payload.rsi,
        },
    }
    return normalized


def build_payload_hash(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_strategy_decision(normalized: Dict[str, Any]) -> Dict[str, Any]:
    rsi = normalized["features"].get("rsi")
    fast_ema = normalized["features"].get("fast_ema")
    slow_ema = normalized["features"].get("slow_ema")
    side = normalized["side"]

    approve = True
    decision_reason = "baseline_pass"

    if side == "long" and rsi is not None and rsi < 50:
        approve = False
        decision_reason = "rsi_below_long_threshold"

    if side == "short" and rsi is not None and rsi > 50:
        approve = False
        decision_reason = "rsi_above_short_threshold"

    if fast_ema is not None and slow_ema is not None:
        if side == "long" and fast_ema <= slow_ema:
            approve = False
            decision_reason = "ema_alignment_invalid_for_long"
        if side == "short" and fast_ema >= slow_ema:
            approve = False
            decision_reason = "ema_alignment_invalid_for_short"

    return {
        "decision": "approve" if approve else "reject",
        "reason": decision_reason,
        "confidence": 0.55 if approve else 0.25,
        "decision_time": utc_now_iso(),
    }


def build_risk_decision(strategy_decision: Dict[str, Any]) -> Dict[str, Any]:
    if strategy_decision["decision"] != "approve":
        return {
            "risk_decision": "deny",
            "reason": "strategy_not_approved",
            "position_size": 0.0,
            "risk_time": utc_now_iso(),
        }

    return {
        "risk_decision": "approve",
        "reason": "paper_risk_baseline_pass",
        "position_size": 1.0,
        "risk_time": utc_now_iso(),
    }


def build_execution_request(
    normalized: Dict[str, Any],
    strategy_decision: Dict[str, Any],
    risk_decision: Dict[str, Any],
) -> Dict[str, Any]:
    actionable = (
        strategy_decision["decision"] == "approve"
        and risk_decision["risk_decision"] == "approve"
    )

    return {
        "execution_mode": "paper",
        "execution_status": "ready_simulated" if actionable else "blocked",
        "symbol": normalized["symbol"],
        "side": normalized["side"],
        "order_type": "market",
        "qty": risk_decision["position_size"],
        "requested_price": normalized["market_price"],
        "created_at": utc_now_iso(),
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "time": utc_now_iso()}


@app.post(
    "/webhooks/tradingview",
    responses={
        401: {
            "description": "Invalid signal key",
        }
    },
)
async def tradingview_webhook(
    payload: TradingViewWebhookPayload,
    signal_key: Annotated[Optional[str], Query(min_length=1)] = None,
    x_signal_key: Annotated[Optional[str], Header()] = None,
) -> Dict[str, Any]:
    resolved_signal_key = resolve_signal_key(
        query_signal_key=signal_key,
        body_signal_key=payload.signal_key,
        header_signal_key=x_signal_key,
    )
    if resolved_signal_key != SIGNAL_KEY:
        raise HTTPException(status_code=401, detail="invalid signal key")

    raw_payload = payload.model_dump(exclude={"signal_key"})
    payload_hash = build_payload_hash(raw_payload)
    normalized = normalize_tradingview_payload(payload)
    strategy_decision = build_strategy_decision(normalized)
    risk_decision = build_risk_decision(strategy_decision)
    execution_request = build_execution_request(normalized, strategy_decision, risk_decision)

    return {
        "status": "accepted",
        "payload_hash": payload_hash,
        "raw_payload": raw_payload,
        "normalized_signal": normalized,
        "strategy_decision": strategy_decision,
        "risk_decision": risk_decision,
        "execution_request": execution_request,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.tradingview_bridge_standalone:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
    )
