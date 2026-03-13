from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

FeaturePrimitive = float | int | str | bool | None
FeatureMap = dict[str, FeaturePrimitive]


@dataclass(frozen=True)
class FeatureSpec:
    engine: str
    key: str
    value_type: str
    description: str
    unit: str | None = None


@dataclass(frozen=True)
class FeatureContext:
    symbol: str
    timeframe: str
    bars: list[dict[str, Any]]
    latest_bar_id: int | None = None
    latest_timestamp: str | None = None


@runtime_checkable
class FeatureEngine(Protocol):
    name: str

    def specs(self) -> tuple[FeatureSpec, ...]:
        ...

    def compute(self, context: FeatureContext) -> FeatureMap:
        ...
