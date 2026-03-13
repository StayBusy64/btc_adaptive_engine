from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRIDGE_MANIFEST_PATH = PROJECT_ROOT / "tradingview" / "bridge_manifest.json"


class BridgeReleaseMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    release_id: str = Field(..., min_length=1)
    release_version: str = Field(..., min_length=1)
    strategy_id: str = Field(..., min_length=1)
    contract_version: str = Field(..., min_length=1)
    telemetry_schema_version: str = Field(..., min_length=1)
    release_channel: str = Field(..., min_length=1)
    pine_script_name: str = Field(..., min_length=1)
    pine_language_version: str = Field(default="6", min_length=1)


class BridgeAlertSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1)
    condition: str = Field(..., min_length=1)
    trigger: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)
    webhook_url_template: str = Field(..., min_length=1)
    notes: list[str] = Field(default_factory=list)


class BridgePineDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signal_source: str = Field(..., min_length=1)
    signal_namespace: str = Field(..., min_length=1)
    signal_family: str = Field(..., min_length=1)
    signal_type_long: str = Field(..., min_length=1)
    signal_type_short: str = Field(..., min_length=1)
    fast_len: int = Field(..., ge=1)
    slow_len: int = Field(..., ge=1)
    trend_len: int = Field(..., ge=1)
    rsi_len: int = Field(..., ge=1)
    rsi_long_threshold: float
    rsi_short_threshold: float
    atr_len: int = Field(..., ge=1)
    volume_sma_len: int = Field(..., ge=1)
    slope_lookback: int = Field(..., ge=1)
    use_confirmed_bars_only: bool = True
    send_long_alerts: bool = True
    send_short_alerts: bool = True


class ExperimentalFieldDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    layer: Literal["batch", "event", "micro", "macro", "research"]
    status: Literal["research", "candidate", "promoted"] = "research"
    purpose: str = Field(..., min_length=1)
    description: Optional[str] = None


class BridgeTelemetryContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stable_batch_fields: list[str] = Field(default_factory=list)
    stable_event_fields: list[str] = Field(default_factory=list)
    stable_micro_fields: list[str] = Field(default_factory=list)
    stable_macro_fields: list[str] = Field(default_factory=list)
    experimental_fields: dict[str, ExperimentalFieldDefinition] = Field(default_factory=dict)


class DecayStateDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    half_life_bars: int = Field(..., ge=1)
    contradiction_multiplier: float = Field(..., ge=1.0)
    refresh_signals: list[str] = Field(default_factory=list)
    contradiction_signals: list[str] = Field(default_factory=list)
    floor: float = Field(default=0.0, ge=0.0)
    ceiling: float = Field(default=1.0, ge=0.0)


class FeatureReliabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rolling_windows: dict[str, int] = Field(default_factory=dict)
    condition_keys: list[str] = Field(default_factory=list)
    side_specific: bool = True
    regime_specific: bool = True
    promotion_min_samples: int = Field(..., ge=1)
    demotion_min_samples: int = Field(..., ge=1)
    penalty_memory_bars: int = Field(..., ge=1)


class BaselineReleaseEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tag: str = Field(..., min_length=1)
    release_version: str = Field(..., min_length=1)
    release_channel: str = Field(..., min_length=1)
    frozen_at: str = Field(..., min_length=1)
    manifest_snapshot: str = Field(..., min_length=1)


class BridgeRefinementManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    current_release: BridgeReleaseMetadata
    alert_settings: BridgeAlertSettings
    pine_defaults: BridgePineDefaults
    telemetry_contract: BridgeTelemetryContract
    decay_framework: dict[str, DecayStateDefinition] = Field(default_factory=dict)
    feature_reliability: FeatureReliabilityConfig
    baseline_releases: list[BaselineReleaseEntry] = Field(default_factory=list)
    roadmap: list[str] = Field(default_factory=list)


def _manifest_path(manifest_path: Optional[str | Path] = None) -> Path:
    if manifest_path is None:
        return DEFAULT_BRIDGE_MANIFEST_PATH
    return Path(manifest_path)


@lru_cache(maxsize=8)
def load_bridge_manifest(manifest_path: Optional[str | Path] = None) -> BridgeRefinementManifest:
    path = _manifest_path(manifest_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return BridgeRefinementManifest.model_validate(payload)


def clear_bridge_manifest_cache() -> None:
    load_bridge_manifest.cache_clear()


def resolve_release_context(*, payload: dict[str, Any], event: dict[str, Any]) -> dict[str, Optional[str]]:
    manifest = load_bridge_manifest()
    current = manifest.current_release
    micro = event.get("micro") if isinstance(event.get("micro"), dict) else {}

    strategy_id = str(event.get("strategy_id") or micro.get("strategy_id") or "").strip()
    if not strategy_id:
        strategy_id = current.strategy_id

    matches_current_release = strategy_id == current.strategy_id

    def _resolve_value(raw_value: Any, fallback_value: str) -> Optional[str]:
        cleaned = str(raw_value or "").strip()
        if cleaned:
            return cleaned
        if matches_current_release:
            return fallback_value
        return None

    return {
        "strategy_id": strategy_id or None,
        "release_id": _resolve_value(payload.get("release_id"), current.release_id),
        "release_version": _resolve_value(payload.get("release_version"), current.release_version),
        "release_channel": _resolve_value(payload.get("release_channel"), current.release_channel),
        "contract_version": _resolve_value(payload.get("contract_version"), current.contract_version),
        "telemetry_schema_version": _resolve_value(
            payload.get("telemetry_schema_version"),
            current.telemetry_schema_version,
        ),
    }


def classify_research_fields(research: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_bridge_manifest()
    registered = manifest.telemetry_contract.experimental_fields

    recognized: dict[str, Any] = {}
    unknown: dict[str, Any] = {}

    for key, value in research.items():
        definition = registered.get(str(key))
        if definition is not None and definition.layer == "research":
            recognized[str(key)] = value
        else:
            unknown[str(key)] = value

    return recognized, unknown


def expected_pine_defaults_fragment_map(
    manifest_path: Optional[str | Path] = None,
) -> dict[str, str]:
    manifest = load_bridge_manifest(manifest_path)
    current = manifest.current_release
    defaults = manifest.pine_defaults

    return {
        "strategy id input": f'strategyId = input.string("{current.strategy_id}", "Strategy ID", group=groupPayload)',
        "release id input": f'releaseId = input.string("{current.release_id}", "Release ID", group=groupPayload)',
        "release version input": f'releaseVersion = input.string("{current.release_version}", "Release Version", group=groupPayload)',
        "release channel input": f'releaseChannel = input.string("{current.release_channel}", "Release Channel", group=groupPayload)',
        "contract version input": f'contractVersion = input.string("{current.contract_version}", "Contract Version", group=groupPayload)',
        "telemetry schema input": f'telemetrySchemaVersion = input.string("{current.telemetry_schema_version}", "Telemetry Schema Version", group=groupPayload)',
        "signal source input": f'signalSourceInput = input.string("{defaults.signal_source}", "Signal Source", group=groupPayload)',
        "signal namespace input": f'signalNamespace = input.string("{defaults.signal_namespace}", "Signal Namespace", group=groupPayload)',
        "signal family input": f'signalFamily = input.string("{defaults.signal_family}", "Signal Family", group=groupPayload)',
        "long signal type input": f'signalTypeLong = input.string("{defaults.signal_type_long}", "Long Signal Type", group=groupPayload)',
        "short signal type input": f'signalTypeShort = input.string("{defaults.signal_type_short}", "Short Signal Type", group=groupPayload)',
    }