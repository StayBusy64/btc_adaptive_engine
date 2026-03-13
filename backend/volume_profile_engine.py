from pydantic import BaseModel, Field, model_validator
from typing import Literal

POC_MIGRATION_THRESHOLD_PCT = 0.005
MIN_POC_MIGRATION_THRESHOLD = 1e-9


class VolumeProfileSnapshot(BaseModel):
    timestamp: str
    symbol: str
    timeframe: str
    engine_version: str

    poc: float
    vah: float
    val: float
    profile_high: float
    profile_low: float

    shape_label: Literal[
        "p_shape",
        "b_shape",
        "d_shape",
        "neutral",
        "b_shape_skew",
    ]
    balance_state: Literal[
        "balanced",
        "imbalanced_up",
        "imbalanced_down",
        "developing",
        "unknown",
    ]

    source_bar_count: int = Field(ge=0)

    # Derived geometric metrics
    profile_range: float
    value_area_width: float
    value_area_width_pct: float
    poc_relative: float
    poc_distance_from_mid: float
    
    # Optional downstream current-price metrics
    close_position_in_profile: float | None = None
    distance_to_poc: float | None = None
    distance_to_vah: float | None = None
    distance_to_val: float | None = None
    distance_to_poc_pct: float | None = None
    distance_to_vah_pct: float | None = None
    distance_to_val_pct: float | None = None
    inside_value_area: bool | None = None
    above_vah: bool | None = None
    below_val: bool | None = None

    # Optional rolling migration metrics derived from the prior persisted snapshot
    poc_migration_delta: float = 0.0
    poc_migrating_up: bool = False
    poc_migrating_down: bool = False
    poc_migration_strength: float = 0.0

    @model_validator(mode="after")
    def validate_profile_ordering(self) -> "VolumeProfileSnapshot":
        if not (self.profile_low <= self.val <= self.poc <= self.vah <= self.profile_high):
            raise ValueError(
                f"Invalid profile ordering: expected "
                f"profile_low({self.profile_low}) <= val({self.val}) <= "
                f"poc({self.poc}) <= vah({self.vah}) <= profile_high({self.profile_high})"
            )
        return self

def compute_volume_profile_snapshot(
    bars: list[dict],
    symbol: str,
    timeframe: str,
    engine_version: str = "v1.1",
    value_area_pct: float = 0.68,
    bins: int = 50
) -> VolumeProfileSnapshot:
    if bins <= 0:
        raise ValueError(f"bins must be > 0, got {bins}")
    if not (0 < value_area_pct < 1):
        raise ValueError(f"value_area_pct must be strictly between 0 and 1, got {value_area_pct}")
    if not bars:
        raise ValueError("Cannot compute profile from empty bars.")

    profile_high = max(b.get("high", b.get("close", 0)) for b in bars)
    profile_low = min(b.get("low", b.get("close", 0)) for b in bars)

    if profile_high == profile_low:
        return VolumeProfileSnapshot(
            timestamp=bars[-1].get("timestamp", "unknown"),
            symbol=symbol,
            timeframe=timeframe,
            engine_version=engine_version,
            poc=profile_high, vah=profile_high, val=profile_high,
            profile_high=profile_high, profile_low=profile_low,
            shape_label="neutral",
            balance_state="developing",
            source_bar_count=len(bars),
            profile_range=0.0,
            value_area_width=0.0,
            value_area_width_pct=0.0,
            poc_relative=0.5,
            poc_distance_from_mid=0.0
        )

    bin_size = (profile_high - profile_low) / bins
    profile = {i: 0.0 for i in range(bins)}
    total_vol = 0.0

    for b in bars:
        h, l, v = b.get("high"), b.get("low"), b.get("volume", 1.0)
        if h is None or l is None:
            continue
        if h == l:
            idx = min(int((h - profile_low) / bin_size) if bin_size > 0 else 0, bins - 1)
            profile[idx] += v
            total_vol += v
            continue

        bar_bins = []
        for i in range(bins):
            bin_bottom = profile_low + i * bin_size
            bin_top = bin_bottom + bin_size
            overlap = max(0, min(h, bin_top) - max(l, bin_bottom))
            if overlap > 0:
                bar_bins.append((i, overlap))

        total_overlap = sum(overlap for _, overlap in bar_bins)
        if total_overlap > 0:
            for idx, overlap in bar_bins:
                allocated_v = v * (overlap / total_overlap)
                profile[idx] += allocated_v
                total_vol += allocated_v

    if total_vol == 0:
        # Fallback to mid if volume empty
        fallback_mid = (profile_high + profile_low) / 2
        p_range = profile_high - profile_low
        return VolumeProfileSnapshot(
            timestamp=bars[-1].get("timestamp", "unknown"),
            symbol=symbol, timeframe=timeframe, engine_version=engine_version,
            poc=fallback_mid, vah=fallback_mid, val=fallback_mid,
            profile_high=profile_high, profile_low=profile_low,
            shape_label="neutral", balance_state="unknown", source_bar_count=len(bars),
            profile_range=p_range, value_area_width=0.0, value_area_width_pct=0.0,
            poc_relative=0.5, poc_distance_from_mid=0.0
        )

    max_vol = max(profile.values())
    poc_candidates = [k for k, v in profile.items() if abs(v - max_vol) < 1e-9]
    poc_idx_exact = sum(poc_candidates) / len(poc_candidates)
    poc = profile_low + (poc_idx_exact + 0.5) * bin_size
    
    target_v = total_vol * value_area_pct
    
    center_idx = int(poc_idx_exact)
    current_v = profile[center_idx]
    low_idx, high_idx = center_idx, center_idx

    while current_v < target_v and (low_idx > 0 or high_idx < bins - 1):
        vol_down = profile[low_idx - 1] if low_idx > 0 else -1
        vol_up = profile[high_idx + 1] if high_idx < bins - 1 else -1
        
        if vol_down > vol_up and vol_down >= 0:
            low_idx -= 1
            current_v += profile[low_idx]
        elif vol_up > vol_down and vol_up >= 0:
            high_idx += 1
            current_v += profile[high_idx]
        else:
            if vol_down >= 0:
                low_idx -= 1
                current_v += profile[low_idx]
            if vol_up >= 0 and current_v < target_v:
                high_idx += 1
                current_v += profile[high_idx]

    val = profile_low + low_idx * bin_size
    vah = profile_low + (high_idx + 1) * bin_size

    p_range = profile_high - profile_low
    va_width = vah - val
    va_width_pct = va_width / p_range if p_range > 0 else 0.0
    poc_rel = (poc - profile_low) / p_range if p_range > 0 else 0.5
    poc_dist = poc - ((profile_high + profile_low) / 2)

    if poc_rel > 0.60:
        shape, balance = "p_shape", "imbalanced_up"
    elif poc_rel < 0.40:
        shape, balance = "b_shape", "imbalanced_down"
    else:
        shape, balance = "d_shape", "balanced"

    close = bars[-1].get("close")
    close_pos = None
    dist_poc = dist_vah = dist_val = None
    dist_poc_pct = dist_vah_pct = dist_val_pct = None
    in_va = above_vah = below_val = None

    if close is not None:
        effective_range = max(p_range, 1e-9)
        close_pos = (close - profile_low) / effective_range
        dist_poc = abs(close - poc)
        dist_vah = abs(close - vah)
        dist_val = abs(close - val)

        dist_poc_pct = dist_poc / effective_range
        dist_vah_pct = dist_vah / effective_range
        dist_val_pct = dist_val / effective_range

        in_va = val <= close <= vah
        above_vah = close > vah
        below_val = close < val

    return VolumeProfileSnapshot(
        timestamp=bars[-1].get("timestamp", "unknown"),
        symbol=symbol, timeframe=timeframe, engine_version=engine_version,
        poc=round(poc, 3), vah=round(vah, 3), val=round(val, 3),
        profile_high=round(profile_high, 3), profile_low=round(profile_low, 3),
        shape_label=shape, balance_state=balance, source_bar_count=len(bars),
        profile_range=round(p_range, 3),
        value_area_width=round(va_width, 3),
        value_area_width_pct=round(va_width_pct, 4),
        poc_relative=round(poc_rel, 4),
        poc_distance_from_mid=round(poc_dist, 3),
        close_position_in_profile=round(close_pos, 4) if close_pos is not None else None,
        distance_to_poc=round(dist_poc, 3) if dist_poc is not None else None,
        distance_to_vah=round(dist_vah, 3) if dist_vah is not None else None,
        distance_to_val=round(dist_val, 3) if dist_val is not None else None,
        distance_to_poc_pct=round(dist_poc_pct, 4) if dist_poc_pct is not None else None,
        distance_to_vah_pct=round(dist_vah_pct, 4) if dist_vah_pct is not None else None,
        distance_to_val_pct=round(dist_val_pct, 4) if dist_val_pct is not None else None,
        inside_value_area=in_va,
        above_vah=above_vah,
        below_val=below_val
    )

from backend.event_writer import (
    get_recent_volume_profile_snapshots,
    insert_volume_profile_snapshot,
)

def compute_and_store_volume_profile_snapshot(
    bars: list[dict],
    symbol: str,
    timeframe: str,
) -> VolumeProfileSnapshot:
    snapshot = compute_volume_profile_snapshot(bars, symbol, timeframe)

    prior_rows = get_recent_volume_profile_snapshots(
        symbol=symbol,
        timeframe=timeframe,
        limit=1,
    )
    if prior_rows:
        previous_poc_raw = prior_rows[0].get("poc")
        if previous_poc_raw is not None:
            previous_poc = float(previous_poc_raw)
            delta = float(snapshot.poc - previous_poc)
            threshold = max(
                float(snapshot.profile_range) * POC_MIGRATION_THRESHOLD_PCT,
                MIN_POC_MIGRATION_THRESHOLD,
            )

            snapshot.poc_migration_delta = delta
            snapshot.poc_migrating_up = delta > threshold
            snapshot.poc_migrating_down = delta < -threshold
            snapshot.poc_migration_strength = abs(delta) / threshold

    insert_volume_profile_snapshot(**snapshot.model_dump())
    return snapshot
