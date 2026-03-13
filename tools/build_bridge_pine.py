from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.tradingview_bridge_manifest import (  # noqa: E402
    expected_pine_defaults_fragment_map,
    load_bridge_manifest,
)


def render_release_notes() -> str:
    manifest = load_bridge_manifest()
    release = manifest.current_release
    alert = manifest.alert_settings
    defaults = manifest.pine_defaults
    contract = manifest.telemetry_contract

    emitted_research_keys = [
        key
        for key, definition in contract.experimental_fields.items()
        if definition.layer == "research" and definition.status in {"candidate", "promoted"}
    ]

    lines = [
        "# Bridge Release Notes",
        "",
        f"## Current release: {release.release_id}",
        "",
        f"- Release version: {release.release_version}",
        f"- Strategy ID: {release.strategy_id}",
        f"- Contract version: {release.contract_version}",
        f"- Telemetry schema version: {release.telemetry_schema_version}",
        f"- Release channel: {release.release_channel}",
        "",
        "## Pine defaults",
        "",
        f"- Signal source: {defaults.signal_source}",
        f"- Signal namespace: {defaults.signal_namespace}",
        f"- Signal family: {defaults.signal_family}",
        f"- Long signal type: {defaults.signal_type_long}",
        f"- Short signal type: {defaults.signal_type_short}",
        f"- EMA lengths: fast {defaults.fast_len}, slow {defaults.slow_len}, trend {defaults.trend_len}",
        f"- RSI: length {defaults.rsi_len}, long threshold {defaults.rsi_long_threshold}, short threshold {defaults.rsi_short_threshold}",
        f"- ATR length: {defaults.atr_len}",
        f"- Volume SMA length: {defaults.volume_sma_len}",
        f"- EMA slope lookback: {defaults.slope_lookback}",
        f"- Confirmed bars only: {defaults.use_confirmed_bars_only}",
        "",
        "## Alert settings",
        "",
        f"- Name: {alert.name}",
        f"- Condition: {alert.condition}",
        f"- Trigger: {alert.trigger}",
        f"- Message: {alert.message}",
        f"- Webhook URL: {alert.webhook_url_template}",
    ]

    if alert.notes:
        lines.extend(["", "## Alert notes", ""])
        lines.extend([f"- {note}" for note in alert.notes])

    lines.extend(
        [
            "",
            "## Stable telemetry contract",
            "",
            f"- Stable batch fields: {', '.join(contract.stable_batch_fields)}",
            f"- Stable event fields: {', '.join(contract.stable_event_fields)}",
            f"- Stable micro fields: {', '.join(contract.stable_micro_fields)}",
            f"- Stable macro fields: {', '.join(contract.stable_macro_fields)}",
            "",
            "## Emitted research telemetry",
            "",
            f"- Research fields emitted in this release: {', '.join(emitted_research_keys)}",
            "",
            "## Deferred roadmap",
            "",
        ]
    )
    lines.extend([f"- {item}" for item in manifest.roadmap])
    lines.append("")
    return "\n".join(lines)


def validate_pine_defaults(pine_path: Path) -> list[str]:
    text = pine_path.read_text(encoding="utf-8")
    expected_fragments = expected_pine_defaults_fragment_map()

    missing = [
        description
        for description, fragment in expected_fragments.items()
        if fragment not in text
    ]
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and render the versioned TradingView bridge release artifacts.")
    parser.add_argument(
        "--pine",
        default=str(ROOT / "tradingview" / "bridge_signal_sender.pine"),
        help="Path to the canonical Pine bridge file.",
    )
    parser.add_argument(
        "--release-notes",
        default=str(ROOT / "tradingview" / "bridge_release_notes.md"),
        help="Path to the release notes output file.",
    )
    parser.add_argument(
        "--write-release-notes",
        action="store_true",
        help="Write the rendered release notes to disk.",
    )
    parser.add_argument(
        "--check-pine",
        action="store_true",
        help="Validate Pine defaults against tradingview/bridge_manifest.json.",
    )
    args = parser.parse_args()

    notes_text = render_release_notes()

    if args.write_release_notes:
        release_notes_path = Path(args.release_notes)
        release_notes_path.write_text(notes_text, encoding="utf-8")

    if args.check_pine:
        missing = validate_pine_defaults(Path(args.pine))
        if missing:
            print("Pine bridge file is out of sync with tradingview/bridge_manifest.json.", file=sys.stderr)
            for item in missing:
                print(f"- Missing expected fragment for {item}", file=sys.stderr)
            return 1

    print(notes_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())