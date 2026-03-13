import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

from backend.bar_utils import decode_json_payload as shared_decode_json_payload
from backend.bar_utils import first_float as shared_first_float
from backend.event_writer import (
    ExecutionOutcomeParams,
    get_bar_states_in_window,
    get_pending_filled_journal_for_outcomes,
    insert_execution_outcome,
)
from backend.feature_math import (
    get_env_bool as _get_env_bool,
    get_env_float as _get_env_float,
    get_env_int as _get_env_int,
    to_optional_float as shared_to_optional_float,
)

logger = logging.getLogger(__name__)

DEFAULT_EVALUATION_WINDOW_MINUTES = 15
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_BATCH_LIMIT = 200
DEFAULT_BAR_LIMIT = 5000


@dataclass(frozen=True)
class EvaluatorConfig:
    evaluation_window_minutes: int
    poll_interval_seconds: float
    oneshot: bool
    batch_limit: int
    symbol_filter: Optional[str]
    bar_limit: int

    @classmethod
    def from_env(cls) -> "EvaluatorConfig":
        symbol_filter_raw = os.getenv("OUTCOME_SYMBOL_FILTER")
        symbol_filter = symbol_filter_raw.strip() if symbol_filter_raw else None

        return cls(
            evaluation_window_minutes=_get_env_int(
                "EVALUATION_WINDOW_MINUTES",
                DEFAULT_EVALUATION_WINDOW_MINUTES,
                minimum=1,
            ),
            poll_interval_seconds=_get_env_float(
                "OUTCOME_POLL_INTERVAL_SECONDS",
                DEFAULT_POLL_INTERVAL_SECONDS,
                minimum=0.0,
            ),
            oneshot=_get_env_bool("OUTCOME_EVALUATOR_ONESHOT", default=False),
            batch_limit=_get_env_int(
                "OUTCOME_BATCH_LIMIT",
                DEFAULT_BATCH_LIMIT,
                minimum=1,
            ),
            symbol_filter=symbol_filter,
            bar_limit=_get_env_int(
                "OUTCOME_BAR_LIMIT",
                DEFAULT_BAR_LIMIT,
                minimum=1,
            ),
        )


class OutcomeEvaluator:
    def __init__(
        self,
        config: EvaluatorConfig,
        *,
        sleep_fn: Callable[[float], None] = time.sleep,
    ):
        self.config = config
        self.sleep_fn = sleep_fn

    def run(self) -> int:
        logger.info(
            "outcome evaluator starting oneshot=%s symbol_filter=%s evaluation_window_minutes=%s batch_limit=%s",
            self.config.oneshot,
            self.config.symbol_filter,
            self.config.evaluation_window_minutes,
            self.config.batch_limit,
        )

        processed_total = 0

        while True:
            processed_count = self.evaluate_once()
            processed_total += processed_count

            if self.config.oneshot:
                logger.info("one-shot mode exiting after processing=%s", processed_count)
                return processed_total

            if processed_count == 0:
                self.sleep_fn(self.config.poll_interval_seconds)

    def evaluate_once(self) -> int:
        pending_rows = get_pending_filled_journal_for_outcomes(
            symbol=self.config.symbol_filter,
            limit=self.config.batch_limit,
        )

        if not pending_rows:
            logger.info("no pending filled journal rows for outcomes")
            return 0

        processed_count = 0

        for row in pending_rows:
            journal_id = row.get("journal_id")

            try:
                inserted = self._evaluate_row(row)
                if inserted:
                    processed_count += 1
            except Exception:
                logger.exception("outcome evaluation failed journal_id=%s", journal_id)

        logger.info("outcome evaluation pass finished processed=%s pending=%s", processed_count, len(pending_rows))
        return processed_count

    def _evaluate_row(self, row: Dict[str, Any]) -> bool:
        journal_id = int(row["journal_id"])
        candidate_id = int(row["candidate_id"])
        signal_id = row.get("signal_id")
        worker_id = row.get("worker_id")
        symbol = row.get("symbol")

        direction_raw = str(row.get("direction") or "").strip().lower()
        direction = direction_raw if direction_raw in {"long", "short"} else None

        entry_price = _to_optional_float(row.get("entry_price"))
        reference_timestamp = row.get("candidate_timestamp") or row.get("journal_created_at")

        if not symbol or direction is None or entry_price is None or not reference_timestamp:
            return self._insert_insufficient_data_outcome(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=signal_id,
                worker_id=worker_id,
                symbol=symbol,
                direction=direction_raw or None,
                entry_price=entry_price,
                reference_timestamp=reference_timestamp,
                reason="missing_required_inputs",
                metadata={
                    "has_symbol": bool(symbol),
                    "has_direction": direction is not None,
                    "has_entry_price": entry_price is not None,
                    "has_reference_timestamp": bool(reference_timestamp),
                },
            )

        reference_dt = _parse_iso8601(reference_timestamp)
        if reference_dt is None:
            return self._insert_insufficient_data_outcome(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=signal_id,
                worker_id=worker_id,
                symbol=symbol,
                direction=direction,
                entry_price=entry_price,
                reference_timestamp=reference_timestamp,
                reason="invalid_reference_timestamp",
            )

        window_end_dt = reference_dt + timedelta(minutes=self.config.evaluation_window_minutes)
        window_end_iso = window_end_dt.isoformat()

        bar_rows = get_bar_states_in_window(
            symbol=symbol,
            since_timestamp=reference_dt.isoformat(),
            until_timestamp=window_end_iso,
            limit=self.config.bar_limit,
        )
        price_points = _extract_price_points(bar_rows)

        if not price_points:
            return self._insert_insufficient_data_outcome(
                journal_id=journal_id,
                candidate_id=candidate_id,
                signal_id=signal_id,
                worker_id=worker_id,
                symbol=symbol,
                direction=direction,
                entry_price=entry_price,
                reference_timestamp=reference_dt.isoformat(),
                reason="no_price_points",
                metadata={
                    "window_end": window_end_iso,
                    "bars_scanned": len(bar_rows),
                },
            )

        metrics = _compute_outcome_metrics(
            direction=direction,
            entry_price=entry_price,
            price_points=price_points,
        )

        return self._insert_outcome(
            journal_id=journal_id,
            candidate_id=candidate_id,
            signal_id=signal_id,
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            reference_timestamp=reference_dt.isoformat(),
            outcome_status="evaluated",
            outcome_metrics=metrics,
            metadata={
                "simulation": True,
                "window_end": window_end_iso,
                "bars_scanned": len(bar_rows),
                "bars_used": len(price_points),
                "exit_timestamp": price_points[-1].get("timestamp"),
            },
        )

    def _insert_insufficient_data_outcome(
        self,
        *,
        journal_id: int,
        candidate_id: int,
        signal_id: Optional[str],
        worker_id: Optional[str],
        symbol: Optional[str],
        direction: Optional[str],
        entry_price: Optional[float],
        reference_timestamp: Optional[str],
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        merged_metadata = {
            "simulation": True,
            "reason": reason,
        }
        if metadata:
            merged_metadata.update(metadata)

        return self._insert_outcome(
            journal_id=journal_id,
            candidate_id=candidate_id,
            signal_id=signal_id,
            worker_id=worker_id,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            reference_timestamp=reference_timestamp,
            outcome_status="insufficient_data",
            outcome_metrics=None,
            metadata=merged_metadata,
        )

    def _insert_outcome(
        self,
        *,
        journal_id: int,
        candidate_id: int,
        signal_id: Optional[str],
        worker_id: Optional[str],
        symbol: Optional[str],
        direction: Optional[str],
        entry_price: Optional[float],
        reference_timestamp: Optional[str],
        outcome_status: str,
        outcome_metrics: Optional[Dict[str, Optional[float]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        outcome_metrics = outcome_metrics or {}
        try:
            outcome_id = insert_execution_outcome(
                ExecutionOutcomeParams(
                    journal_id=journal_id,
                    candidate_id=candidate_id,
                    signal_id=signal_id,
                    worker_id=worker_id,
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    reference_timestamp=reference_timestamp,
                    evaluation_window_minutes=self.config.evaluation_window_minutes,
                    outcome_status=outcome_status,
                    exit_price=outcome_metrics.get("exit_price"),
                    pnl_points=outcome_metrics.get("pnl_points"),
                    pnl_pct=outcome_metrics.get("pnl_pct"),
                    max_favorable_excursion=outcome_metrics.get("max_favorable_excursion"),
                    max_adverse_excursion=outcome_metrics.get("max_adverse_excursion"),
                    metadata_json=metadata,
                )
            )
        except sqlite3.IntegrityError:
            logger.info("outcome already exists for journal_id=%s; skipping", journal_id)
            return False

        logger.info(
            "outcome stored id=%s journal_id=%s outcome_status=%s",
            outcome_id,
            journal_id,
            outcome_status,
        )
        return True


def create_evaluator_from_env() -> OutcomeEvaluator:
    config = EvaluatorConfig.from_env()
    return OutcomeEvaluator(config)


def _extract_price_points(bar_rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    points: list[Dict[str, Any]] = []

    for bar_row in bar_rows:
        payload = _decode_json_payload(bar_row.get("payload_json"))
        if not isinstance(payload, dict):
            continue

        high = _first_float(payload, ["high", "h", "bar_high"])
        low = _first_float(payload, ["low", "l", "bar_low"])
        close = _first_float(payload, ["close", "c", "last", "price"])

        if close is None and high is not None and low is not None:
            close = (high + low) / 2.0

        if high is None and close is not None:
            high = close

        if low is None and close is not None:
            low = close

        if high is None or low is None or close is None:
            continue

        points.append(
            {
                "timestamp": bar_row.get("timestamp"),
                "high": high,
                "low": low,
                "close": close,
            }
        )

    return points


def _compute_outcome_metrics(
    *,
    direction: str,
    entry_price: float,
    price_points: list[Dict[str, Any]],
) -> Dict[str, Optional[float]]:
    exit_price = float(price_points[-1]["close"])

    if direction == "long":
        pnl_points = exit_price - entry_price
        favorable_values = [float(point["high"]) - entry_price for point in price_points]
        adverse_values = [float(point["low"]) - entry_price for point in price_points]
    else:
        pnl_points = entry_price - exit_price
        favorable_values = [entry_price - float(point["low"]) for point in price_points]
        adverse_values = [entry_price - float(point["high"]) for point in price_points]

    pnl_pct = None if entry_price == 0 else (pnl_points / entry_price) * 100.0

    return {
        "exit_price": exit_price,
        "pnl_points": pnl_points,
        "pnl_pct": pnl_pct,
        "max_favorable_excursion": max(favorable_values) if favorable_values else None,
        "max_adverse_excursion": min(adverse_values) if adverse_values else None,
    }


def _decode_json_payload(raw_payload: Any) -> Any:
    return shared_decode_json_payload(raw_payload)


def _first_float(payload: Dict[str, Any], keys: list[str]) -> Optional[float]:
    return shared_first_float(payload, keys)


def _parse_iso8601(raw_value: Any) -> Optional[datetime]:
    if raw_value is None:
        return None

    text = str(raw_value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _to_optional_float(value: Any) -> Optional[float]:
    return shared_to_optional_float(value)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    evaluator = create_evaluator_from_env()
    evaluator.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
