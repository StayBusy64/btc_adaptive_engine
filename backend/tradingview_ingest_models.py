from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SignalSide = Literal["buy", "sell", "long", "short"]
BatchTriggerSide = Literal["buy", "sell", "long", "short", "mixed"]


def _parse_iso_epoch_ms(value: str, *, field_name: str) -> int:
    iso_candidate = value.replace("Z", "+00:00")
    try:
        parsed_dt = datetime.fromisoformat(iso_candidate)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must be epoch milliseconds or ISO-8601 timestamp"
        ) from exc

    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    return int(parsed_dt.timestamp() * 1000)


def _parse_string_epoch_ms(value: str, *, field_name: str) -> int:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty")

    numeric = cleaned[1:] if cleaned.startswith("-") else cleaned
    if numeric.isdigit():
        return int(cleaned)

    return _parse_iso_epoch_ms(cleaned, field_name=field_name)


def _parse_epoch_ms(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer epoch milliseconds value")

    if isinstance(value, (int, float)):
        parsed = int(value)
    elif isinstance(value, str):
        parsed = _parse_string_epoch_ms(value, field_name=field_name)
    else:
        raise ValueError(f"{field_name} has unsupported type")

    if parsed < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return parsed


def _normalize_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None

    cleaned = str(value).strip()
    return cleaned or None


def _now_epoch_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


class TradingViewSignalEventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(..., min_length=1, max_length=128)
    event_time: int = Field(..., ge=0)
    side: SignalSide
    signal_type: str = Field(..., min_length=1, max_length=128)
    signal_family: str = Field(..., min_length=1, max_length=128)
    signal_name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    strategy_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    price: float
    confirmed: bool = True
    micro: dict[str, Any] = Field(default_factory=dict)
    macro: dict[str, Any] = Field(default_factory=dict)
    research: Optional[dict[str, Any]] = None

    @field_validator("event_time", mode="before")
    @classmethod
    def coerce_event_time(cls, value: Any) -> int:
        return _parse_epoch_ms(value, field_name="event_time")

    @field_validator("signal_name", "strategy_id", mode="before")
    @classmethod
    def coerce_optional_text(cls, value: Any) -> Optional[str]:
        return _normalize_optional_text(value)

    @model_validator(mode="after")
    def validate_event_defaults(self) -> "TradingViewSignalEventPayload":
        if self.signal_name is None:
            self.signal_name = self.signal_type
        return self


class TradingViewBatchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(..., min_length=1, max_length=64)
    namespace: str = Field(..., min_length=1, max_length=128)
    symbol: str = Field(..., min_length=1, max_length=64)
    chart_tf: str = Field(..., min_length=1, max_length=16)
    batch_id: str = Field(..., min_length=1, max_length=128)
    batch_trigger_side: BatchTriggerSide
    batch_size: int = Field(..., ge=1, le=10_000)
    batch_close_time: int = Field(..., ge=0)
    confirmed: bool = True
    release_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    release_version: Optional[str] = Field(default=None, min_length=1, max_length=64)
    release_channel: Optional[str] = Field(default=None, min_length=1, max_length=64)
    contract_version: Optional[str] = Field(default=None, min_length=1, max_length=128)
    telemetry_schema_version: Optional[str] = Field(default=None, min_length=1, max_length=128)
    events: list[TradingViewSignalEventPayload] = Field(default_factory=list, min_length=1, max_length=10_000)

    @field_validator("batch_close_time", mode="before")
    @classmethod
    def coerce_batch_close_time(cls, value: Any) -> int:
        return _parse_epoch_ms(value, field_name="batch_close_time")

    @field_validator(
        "release_id",
        "release_version",
        "release_channel",
        "contract_version",
        "telemetry_schema_version",
        mode="before",
    )
    @classmethod
    def coerce_optional_batch_text(cls, value: Any) -> Optional[str]:
        return _normalize_optional_text(value)

    @model_validator(mode="after")
    def validate_batch_consistency(self) -> "TradingViewBatchPayload":
        if self.batch_close_time <= 0:
            self.batch_close_time = _now_epoch_ms()

        for event in self.events:
            if event.event_time <= 0:
                event.event_time = self.batch_close_time

        if self.batch_size != len(self.events):
            raise ValueError("batch_size must equal the number of events")

        event_ids = [event.event_id for event in self.events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("event_id values must be unique inside one batch")

        return self


class TradingViewIngestAcceptResponse(BaseModel):
    status: Literal["accepted", "duplicate"]
    batch_id: str
    event_count: int = Field(ge=0)
    raw_saved: bool
    queued_for_cycle: bool
    duplicate_batch: bool
    received_at: str


class TradingViewCycleSummary(BaseModel):
    trigger: str
    cycle_started_at: str
    cycle_finished_at: str
    scanned_files: int = Field(ge=0)
    processed_batches: int = Field(ge=0)
    failed_batches: int = Field(ge=0)
    normalized_events_written: int = Field(ge=0)
    duplicate_events: int = Field(ge=0)
    signal_journal_rows_written: int = Field(default=0, ge=0)
    signal_outcomes_written: int = Field(default=0, ge=0)
    market_bias_rows_written: int = Field(default=0, ge=0)
    active_remaining: int = Field(ge=0)
