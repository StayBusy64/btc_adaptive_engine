from __future__ import annotations

import os
import secrets
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INGEST_FOLDER = PROJECT_ROOT / "data" / "tv_ingest"
LOG_FILE = INGEST_FOLDER / "signals.log"
SIGNAL_KEY_FILE = INGEST_FOLDER / "signal_key.txt"
ENV_SCRIPT_FILE = INGEST_FOLDER / "webhook_env.ps1"

HOST = os.getenv("TRADINGVIEW_WEBHOOK_HOST", "127.0.0.1")
PORT = int(os.getenv("TRADINGVIEW_WEBHOOK_PORT", "8000"))
ENDPOINT_PATH = "/webhooks/tradingview/batch"
PUBLIC_BASE_URL = os.getenv("TRADINGVIEW_PUBLIC_BASE_URL", "").strip()


def _normalize_base_url(raw: str) -> str:
    if not raw:
        return ""
    return raw.rstrip("/")


def _load_or_create_signal_key(path: Path) -> tuple[str, bool]:
    if path.exists():
        existing = path.read_text(encoding="utf-8").strip()
        if existing:
            return existing, False

    generated = secrets.token_hex(16)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{generated}\n", encoding="utf-8")
    return generated, True


def _ps_quote(value: str) -> str:
    escaped = value.replace("`", "``").replace('"', '`"')
    return f'"{escaped}"'


def _write_env_script(
    *,
    env_file: Path,
    ingest_root: Path,
    signal_log_file: Path,
    signal_key: str,
) -> None:
    env_file.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"$env:TRADINGVIEW_INGEST_ROOT={_ps_quote(str(ingest_root))}",
        f"$env:TRADINGVIEW_SIGNAL_LOG_FILE={_ps_quote(str(signal_log_file))}",
        f"$env:TRADINGVIEW_INGEST_SIGNAL_KEY={_ps_quote(signal_key)}",
        f"$env:TV_SIGNAL_KEY={_ps_quote(signal_key)}",
    ]
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    INGEST_FOLDER.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.touch(exist_ok=True)

    signal_key, is_new_key = _load_or_create_signal_key(SIGNAL_KEY_FILE)
    _write_env_script(
        env_file=ENV_SCRIPT_FILE,
        ingest_root=PROJECT_ROOT / "data",
        signal_log_file=LOG_FILE,
        signal_key=signal_key,
    )

    local_webhook = f"http://{HOST}:{PORT}{ENDPOINT_PATH}?signal_key={signal_key}"

    normalized_public_base = _normalize_base_url(PUBLIC_BASE_URL)
    if normalized_public_base:
        permanent_webhook = f"{normalized_public_base}{ENDPOINT_PATH}?signal_key={signal_key}"
    else:
        permanent_webhook = f"https://YOUR-CLOUDFLARE-DOMAIN{ENDPOINT_PATH}?signal_key={signal_key}"

    print("\n========== TradingView Webhook Setup ==========")
    print("\nLocal ingestion folder:")
    print(INGEST_FOLDER)

    print("\nSignal log file:")
    print(LOG_FILE)

    print("\nSignal key file:")
    print(SIGNAL_KEY_FILE)

    print("\nSignal key status:")
    print("generated" if is_new_key else "reused")

    print("\nLocal webhook endpoint:")
    print(local_webhook)

    print("\nPermanent webhook endpoint:")
    print(permanent_webhook)

    print("\nTradingView message field:")
    print("{{alert_message}}")

    print("\nCondition:")
    print("Bridge Signal Sender -> Any alert() function call")

    print("\nPowerShell env helper script:")
    print(ENV_SCRIPT_FILE)

    print("\nTo load env vars in this shell:")
    print(f". {ENV_SCRIPT_FILE}")

    print("\n===============================================\n")


if __name__ == "__main__":
    main()
