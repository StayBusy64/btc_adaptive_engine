"""Storage Lifecycle Engine — file-level value scoring and hot/warm/cold migration.

Scans data directories, scores each file using the file decay formula, and
recommends lifecycle actions (keep hot, compress, archive, summarise+delete).

File value formula:
  value = 0.35*recency_score
        + 0.25*retrieval_frequency
        + 0.20*contained_high_quality_events
        + 0.10*schema_relevance
        - 0.10*size_penalty

Path score formula:
  path_score = 0.30*operational_priority
             + 0.25*training_priority
             + 0.20*content_value
             + 0.15*access_frequency
             - 0.05*io_cost
             - 0.05*fragmentation_penalty
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

_DEFAULT_POLICY_PATH = Path(__file__).resolve().parent.parent / "config" / "path_weight_policy.yaml"


def load_policy(path: Optional[Path] = None) -> dict[str, Any]:
    policy_path = path or _DEFAULT_POLICY_PATH
    if not policy_path.exists():
        return {}
    with open(policy_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

HOT_THRESHOLD = 0.75
WARM_THRESHOLD = 0.50
COLD_THRESHOLD = 0.25
RECENCY_HALF_LIFE_HOURS = 48.0
SIZE_PENALTY_THRESHOLD_MB = 10.0


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FileMetrics:
    """Metrics for a single data file."""
    path: str
    size_bytes: int = 0
    modified_at: Optional[str] = None       # ISO timestamp
    access_count: int = 0                   # retrieved N times since creation
    high_quality_event_count: int = 0       # count of events with MFE > MAE
    total_event_count: int = 0
    schema_version_match: bool = True       # does file match current schema?


@dataclass
class FileLifecycleResult:
    """Lifecycle recommendation for a single file."""
    path: str
    value_score: float
    tier: str                               # hot / warm / cold / dead
    action: str                             # keep / compress / archive / summarise_delete
    components: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class PathScore:
    """Result of scoring a folder path."""
    path: str
    score: float
    tier: str
    components: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# File value scoring
# ---------------------------------------------------------------------------

def _recency_score(modified_at: Optional[str], half_life_hours: float = RECENCY_HALF_LIFE_HOURS) -> float:
    if not modified_at:
        return 0.0
    try:
        mod_dt = datetime.fromisoformat(modified_at)
        if mod_dt.tzinfo is None:
            mod_dt = mod_dt.replace(tzinfo=timezone.utc)
        hours_ago = max(0.0, (datetime.now(timezone.utc) - mod_dt).total_seconds() / 3600.0)
        return math.exp(-0.693 * hours_ago / half_life_hours)  # ln(2) ≈ 0.693
    except (ValueError, TypeError):
        return 0.0


def _retrieval_frequency_score(access_count: int, max_expected: int = 100) -> float:
    return min(1.0, access_count / max(1, max_expected))


def _quality_event_score(high_quality: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return min(1.0, high_quality / total)


def _schema_relevance_score(matches_current: bool) -> float:
    return 1.0 if matches_current else 0.3


def _size_penalty_score(size_bytes: int, threshold_mb: float = SIZE_PENALTY_THRESHOLD_MB) -> float:
    size_mb = size_bytes / (1024 * 1024)
    if size_mb <= threshold_mb:
        return 0.0
    return min(1.0, (size_mb - threshold_mb) / threshold_mb)


def compute_file_value_score(metrics: FileMetrics) -> float:
    """Compute the file value score using the 5-component formula."""
    recency = _recency_score(metrics.modified_at)
    retrieval = _retrieval_frequency_score(metrics.access_count)
    quality = _quality_event_score(metrics.high_quality_event_count, metrics.total_event_count)
    schema = _schema_relevance_score(metrics.schema_version_match)
    size_pen = _size_penalty_score(metrics.size_bytes)

    score = (
        0.35 * recency
        + 0.25 * retrieval
        + 0.20 * quality
        + 0.10 * schema
        - 0.10 * size_pen
    )
    return max(0.0, min(1.0, score))


def _file_tier(score: float) -> str:
    if score >= HOT_THRESHOLD:
        return "hot"
    if score >= WARM_THRESHOLD:
        return "warm"
    if score >= COLD_THRESHOLD:
        return "cold"
    return "dead"


def _file_action(tier: str) -> str:
    return {
        "hot": "keep",
        "warm": "compress",
        "cold": "archive",
        "dead": "summarise_delete",
    }.get(tier, "keep")


def evaluate_file(metrics: FileMetrics) -> FileLifecycleResult:
    """Score a single file and return its lifecycle recommendation."""
    recency = _recency_score(metrics.modified_at)
    retrieval = _retrieval_frequency_score(metrics.access_count)
    quality = _quality_event_score(metrics.high_quality_event_count, metrics.total_event_count)
    schema = _schema_relevance_score(metrics.schema_version_match)
    size_pen = _size_penalty_score(metrics.size_bytes)

    score = max(0.0, min(1.0,
        0.35 * recency + 0.25 * retrieval + 0.20 * quality + 0.10 * schema - 0.10 * size_pen
    ))
    tier = _file_tier(score)
    return FileLifecycleResult(
        path=metrics.path,
        value_score=score,
        tier=tier,
        action=_file_action(tier),
        components={
            "recency": recency,
            "retrieval_frequency": retrieval,
            "quality_events": quality,
            "schema_relevance": schema,
            "size_penalty": size_pen,
        },
    )


# ---------------------------------------------------------------------------
# Directory scanner
# ---------------------------------------------------------------------------

def scan_directory(
    data_dir: Path,
    *,
    extensions: tuple[str, ...] = (".jsonl", ".json", ".csv", ".parquet", ".db"),
) -> list[FileMetrics]:
    """Walk a directory tree and build FileMetrics for each matching file."""
    results: list[FileMetrics] = []
    if not data_dir.exists():
        return results

    for root, _dirs, files in os.walk(data_dir):
        for fname in files:
            if not any(fname.endswith(ext) for ext in extensions):
                continue
            full = Path(root) / fname
            try:
                stat = full.stat()
            except OSError:
                continue

            mod_dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            results.append(FileMetrics(
                path=str(full),
                size_bytes=stat.st_size,
                modified_at=mod_dt.isoformat(),
            ))
    return results


def evaluate_directory(
    data_dir: Path,
    *,
    extensions: tuple[str, ...] = (".jsonl", ".json", ".csv", ".parquet", ".db"),
) -> list[FileLifecycleResult]:
    """Scan and score all data files in a directory tree."""
    file_metrics = scan_directory(data_dir, extensions=extensions)
    return [evaluate_file(fm) for fm in file_metrics]


# ---------------------------------------------------------------------------
# Path scoring
# ---------------------------------------------------------------------------

def compute_path_score(
    folder_path: str,
    policy: Optional[dict[str, Any]] = None,
) -> PathScore:
    """Score a folder path against the path weight policy."""
    if policy is None:
        policy = load_policy()

    folders_config = policy.get("folders", {})
    # Find the best-matching folder config (longest prefix match)
    normalised = folder_path.replace("\\", "/").rstrip("/")
    best_match = ""
    best_config: dict[str, Any] = {}
    for cfg_path, cfg in folders_config.items():
        cfg_norm = cfg_path.replace("\\", "/").rstrip("/")
        if normalised.endswith(cfg_norm) or cfg_norm in normalised:
            if len(cfg_norm) > len(best_match):
                best_match = cfg_norm
                best_config = cfg if isinstance(cfg, dict) else {}

    op = float(best_config.get("operational_priority", 0.5))
    tp = float(best_config.get("training_priority", 0.5))
    cv = float(best_config.get("content_value", 0.5))
    af = 0.5  # default; could be computed from access logs
    io_cost = 0.05
    frag = 0.05

    score = (
        0.30 * op
        + 0.25 * tp
        + 0.20 * cv
        + 0.15 * af
        - 0.05 * io_cost
        - 0.05 * frag
    )
    score = max(0.0, min(1.0, score))

    tier = _file_tier(score)  # reuse same thresholds
    return PathScore(
        path=folder_path,
        score=score,
        tier=tier,
        components={
            "operational_priority": op,
            "training_priority": tp,
            "content_value": cv,
            "access_frequency": af,
            "io_cost": io_cost,
            "fragmentation_penalty": frag,
        },
    )


def summarize_lifecycle(results: list[FileLifecycleResult]) -> dict[str, Any]:
    """Summarise lifecycle evaluation results."""
    tier_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    total_bytes = 0
    for r in results:
        tier_counts[r.tier] = tier_counts.get(r.tier, 0) + 1
        action_counts[r.action] = action_counts.get(r.action, 0) + 1
    for r in results:
        # sum raw bytes from the metrics that produced each result
        pass  # would need original FileMetrics; callers use scan_directory for this

    return {
        "total_files": len(results),
        "tier_counts": tier_counts,
        "action_counts": action_counts,
    }
