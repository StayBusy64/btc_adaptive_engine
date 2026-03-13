import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Protocol, Tuple

from backend.event_writer import insert_execution_journal_entry
from backend.feature_math import (
    get_env_bool as _get_env_bool,
    get_env_float as _get_env_float,
    to_optional_float as shared_to_optional_float,
)

logger = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_WORKER_ID = "execution-worker-1"
DEFAULT_POLL_INTERVAL_SECONDS = 3.0
DEFAULT_CONFIDENCE_THRESHOLD = 0.75
DEFAULT_SIMULATE_PROCESSING_SECONDS = 0.0
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 1.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 15.0


class ApiClientError(RuntimeError):
    def __init__(self, message: str, *, status_code: Optional[int] = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


@dataclass(frozen=True)
class WorkerConfig:
    api_base_url: str
    signal_webhook_key: str
    worker_id: str
    poll_interval_seconds: float
    oneshot: bool
    confidence_threshold: float
    simulate_processing_seconds: float
    heartbeat_interval_seconds: float
    http_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        signal_webhook_key = os.getenv("SIGNAL_WEBHOOK_KEY", "").strip()
        if not signal_webhook_key:
            raise ValueError("SIGNAL_WEBHOOK_KEY must be set")

        api_base_url = os.getenv("API_BASE_URL", DEFAULT_API_BASE_URL).strip()
        worker_id = os.getenv("WORKER_ID", DEFAULT_WORKER_ID).strip() or DEFAULT_WORKER_ID

        return cls(
            api_base_url=api_base_url,
            signal_webhook_key=signal_webhook_key,
            worker_id=worker_id,
            poll_interval_seconds=_get_env_float("POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS, minimum=0.0),
            oneshot=_get_env_bool("WORKER_ONESHOT", default=False),
            confidence_threshold=_get_env_float(
                "SIMULATION_CONFIDENCE_THRESHOLD",
                DEFAULT_CONFIDENCE_THRESHOLD,
                minimum=0.0,
            ),
            simulate_processing_seconds=_get_env_float(
                "SIMULATE_PROCESSING_SECONDS",
                DEFAULT_SIMULATE_PROCESSING_SECONDS,
                minimum=0.0,
            ),
            heartbeat_interval_seconds=_get_env_float(
                "HEARTBEAT_INTERVAL_SECONDS",
                DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                minimum=0.0,
            ),
            http_timeout_seconds=_get_env_float(
                "WORKER_HTTP_TIMEOUT_SECONDS",
                DEFAULT_HTTP_TIMEOUT_SECONDS,
                minimum=0.1,
            ),
        )


class ApiClientProtocol(Protocol):
    """Structural interface for the API client used by ExecutionWorker.

    Both the production ``ApiClient`` and any test double (e.g. ``FakeApiClient``)
    are compatible as long as they expose these three methods with matching
    signatures.  No inheritance from this class is required.
    """

    def claim_next(self, worker_id: str) -> Dict[str, Any]: ...

    def send_heartbeat(self, candidate_id: int, worker_id: str, claim_token: str) -> Dict[str, Any]: ...

    def patch_status(self, candidate_id: int, execution_status: str, execution_note: str) -> Dict[str, Any]: ...


class ApiClient:
    def __init__(self, base_url: str, signal_webhook_key: str, *, timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS):
        self.base_url = base_url.rstrip("/")
        self.signal_webhook_key = signal_webhook_key
        self.timeout_seconds = timeout_seconds

    def claim_next(self, worker_id: str) -> Dict[str, Any]:
        status_code, payload = self._request_json(
            "POST",
            "/trade_candidates/claim_next",
            {"worker_id": worker_id},
        )
        if status_code != 200 or not isinstance(payload, dict):
            raise ApiClientError(
                "Unexpected claim response",
                status_code=status_code,
                payload=payload,
            )
        return payload

    def send_heartbeat(self, candidate_id: int, worker_id: str, claim_token: str) -> Dict[str, Any]:
        status_code, payload = self._request_json(
            "POST",
            f"/trade_candidates/{candidate_id}/heartbeat",
            {
                "worker_id": worker_id,
                "claim_token": claim_token,
            },
        )
        if status_code != 200 or not isinstance(payload, dict):
            raise ApiClientError(
                "Unexpected heartbeat response",
                status_code=status_code,
                payload=payload,
            )
        return payload

    def patch_status(self, candidate_id: int, execution_status: str, execution_note: str) -> Dict[str, Any]:
        status_code, payload = self._request_json(
            "PATCH",
            f"/trade_candidates/{candidate_id}/status",
            {
                "execution_status": execution_status,
                "execution_note": execution_note,
            },
        )
        if status_code != 200 or not isinstance(payload, dict):
            raise ApiClientError(
                "Unexpected status update response",
                status_code=status_code,
                payload=payload,
            )
        return payload

    def _request_json(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, Any]:
        url = f"{self.base_url}{path}"
        body = None if payload is None else json.dumps(payload).encode("utf-8")

        request = urllib.request.Request(url=url, data=body, method=method.upper())
        request.add_header("Accept", "application/json")
        request.add_header("X-SIGNAL-KEY", self.signal_webhook_key)
        if body is not None:
            request.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                response_body = response.read()
                status_code = int(response.getcode())
                return status_code, _decode_json_bytes(response_body)
        except urllib.error.HTTPError as exc:
            response_body = exc.read()
            raise ApiClientError(
                f"HTTP error during {method.upper()} {path}",
                status_code=exc.code,
                payload=_decode_json_bytes(response_body),
            ) from exc
        except urllib.error.URLError as exc:
            raise ApiClientError(f"Connection error during {method.upper()} {path}: {exc}") from exc


class ExecutionWorker:
    def __init__(
        self,
        config: WorkerConfig,
        api_client: ApiClientProtocol,
        *,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ):
        self.config = config
        self.api_client = api_client
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn

    def run(self) -> int:
        logger.info(
            "worker starting worker_id=%s api_base_url=%s oneshot=%s poll_interval=%.2fs",
            self.config.worker_id,
            self.config.api_base_url,
            self.config.oneshot,
            self.config.poll_interval_seconds,
        )

        processed_count = 0

        while True:
            claim_result = self.api_client.claim_next(self.config.worker_id)
            claim_status = claim_result.get("status")

            if claim_status == "empty":
                logger.info("queue empty")
                if self.config.oneshot:
                    logger.info("one-shot mode exiting after empty claim")
                    return processed_count
                self.sleep_fn(self.config.poll_interval_seconds)
                continue

            if claim_status != "claimed":
                raise RuntimeError(f"Unexpected claim status: {claim_status}")

            self._process_claim(claim_result)
            processed_count += 1

            if self.config.oneshot:
                logger.info("one-shot mode exiting after processed claim")
                return processed_count

    def _process_claim(self, claim_result: Dict[str, Any]) -> None:
        row = claim_result.get("row")
        if not isinstance(row, dict):
            raise RuntimeError("Claim result missing row payload")

        candidate_id = row.get("id")
        claim_token = claim_result.get("claim_token") or row.get("claim_token")
        if candidate_id is None or not claim_token:
            raise RuntimeError("Claim result missing id or claim_token")

        candidate_id_int = int(candidate_id)

        logger.info(
            "claim success id=%s signal_id=%s claimed_by=%s",
            candidate_id_int,
            row.get("signal_id"),
            row.get("claimed_by"),
        )

        started_at = self.monotonic_fn()
        heartbeat_sent = False

        if self.config.simulate_processing_seconds > 0:
            self.sleep_fn(self.config.simulate_processing_seconds)

        elapsed = self.monotonic_fn() - started_at

        if elapsed >= self.config.heartbeat_interval_seconds:
            heartbeat_payload = self.api_client.send_heartbeat(
                candidate_id_int,
                self.config.worker_id,
                str(claim_token),
            )
            heartbeat_sent = True
            logger.info(
                "heartbeat sent id=%s claimed_at=%s",
                candidate_id_int,
                heartbeat_payload.get("claimed_at"),
            )

        final_status, execution_note = simulate_execution_decision(
            row,
            confidence_threshold=self.config.confidence_threshold,
        )

        updated_payload = self.api_client.patch_status(
            candidate_id_int,
            final_status,
            execution_note,
        )

        logger.info(
            "status update id=%s execution_status=%s execution_note=%s",
            candidate_id_int,
            updated_payload.get("execution_status"),
            updated_payload.get("execution_note"),
        )

        metadata = {
            "simulation": True,
            "confidence_threshold": self.config.confidence_threshold,
            "heartbeat_sent": heartbeat_sent,
            "claim_token": str(claim_token),
            "api_execution_status": updated_payload.get("execution_status"),
        }

        try:
            journal_id = insert_execution_journal_entry(
                candidate_id=candidate_id_int,
                signal_id=row.get("signal_id"),
                worker_id=self.config.worker_id,
                action="simulation_decision",
                execution_status=final_status,
                execution_note=execution_note,
                confidence=_as_float_or_none(row.get("confidence")),
                symbol=row.get("symbol"),
                direction=row.get("direction"),
                entry_price=_as_float_or_none(row.get("entry_price")),
                metadata_json=metadata,
            )
            logger.info(
                "journal write id=%s candidate_id=%s execution_status=%s",
                journal_id,
                candidate_id_int,
                final_status,
            )
        except Exception:
            logger.exception(
                "journal write failed candidate_id=%s execution_status=%s",
                candidate_id_int,
                final_status,
            )


def simulate_execution_decision(candidate_row: Dict[str, Any], *, confidence_threshold: float) -> Tuple[str, str]:
    """Simulation placeholder only. This does not place live orders."""
    required_fields = ("symbol", "direction", "timestamp")
    missing_fields = [field for field in required_fields if not candidate_row.get(field)]

    if missing_fields:
        missing_csv = ",".join(missing_fields)
        return "rejected", f"simulation_rejected_missing_fields:{missing_csv}"

    confidence_raw = candidate_row.get("confidence")
    if confidence_raw is None:
        return "rejected", "simulation_rejected_missing_confidence"

    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        return "rejected", "simulation_rejected_invalid_confidence"

    if confidence >= confidence_threshold:
        return (
            "filled",
            f"simulation_filled_confidence_{confidence:.3f}_gte_{confidence_threshold:.3f}",
        )

    return (
        "skipped",
        f"simulation_skipped_confidence_{confidence:.3f}_lt_{confidence_threshold:.3f}",
    )


def create_worker_from_env() -> ExecutionWorker:
    config = WorkerConfig.from_env()
    api_client = ApiClient(
        config.api_base_url,
        config.signal_webhook_key,
        timeout_seconds=config.http_timeout_seconds,
    )
    return ExecutionWorker(config, api_client)


def _decode_json_bytes(raw_bytes: bytes) -> Any:
    if not raw_bytes:
        return None

    text = raw_bytes.decode("utf-8", errors="replace")
    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}


def _as_float_or_none(value: Any) -> Optional[float]:
    return shared_to_optional_float(value)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    worker = create_worker_from_env()
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
