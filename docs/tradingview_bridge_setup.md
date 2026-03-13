# TradingView Bridge Setup

## Current path

The live bridge path is:

1. `tradingview/bridge_signal_sender.pine` emits a batch webhook payload.
2. TradingView posts to the Cloudflare Worker.
3. The Worker forwards the payload to `POST /webhooks/tradingview/batch` on the FastAPI backend.
4. The ingest cycle normalizes the batch, writes normalized events, appends the signal journal, evaluates outcomes, and updates market bias.

The canonical release metadata for this bridge lives in `tradingview/bridge_manifest.json`.

## Backend startup

Run the full API surface on port `8010`.

```powershell
$env:TRADINGVIEW_INGEST_SIGNAL_KEY="change-me-now"
python -m uvicorn backend.api_server:app --host 0.0.0.0 --port 8010 --reload
```

Health check:

```text
http://127.0.0.1:8010/health
```

## Release workflow

Before updating the TradingView alert, validate the Pine file against the manifest and regenerate release notes.

```powershell
python tools/build_bridge_pine.py --check-pine --write-release-notes
```

This validates the Pine defaults and release inputs against `tradingview/bridge_manifest.json` and refreshes `tradingview/bridge_release_notes.md`.

## TradingView alert setup

1. Open the Pine Editor in TradingView.
2. Paste `tradingview/bridge_signal_sender.pine`.
3. Save the script as `Bridge Signal Sender`.
4. Add it to a BTC perpetual chart.
5. Create an alert with condition `Bridge Signal Sender -> Any alert() function call`.
6. Set trigger to `Once Per Bar Close`.
7. Set the webhook URL to the Worker URL from `tradingview/bridge_manifest.json`.
8. Put the Worker secret in the query string as `?secret=<TV_WEBHOOK_SECRET>`.
9. Keep the alert message minimal. The script-generated `alert()` body carries the full batch payload.

Example Worker URL:

```text
https://tv-webhook.staybusyent.workers.dev/?secret=<TV_WEBHOOK_SECRET>
```

## Pine inputs that must stay aligned with the manifest

- `Strategy ID`
- `Release ID`
- `Release Version`
- `Release Channel`
- `Contract Version`
- `Telemetry Schema Version`
- `Signal Source`
- `Signal Namespace`
- `Signal Family`
- `Long Signal Type`
- `Short Signal Type`

If any of those values change, regenerate release notes and recreate the TradingView alert so the running alert uses the new release metadata.

## Validation

Use the repo probe script to validate the backend path end to end.

```powershell
.\test_pipeline.ps1
```

That probe posts a batch-shaped payload, runs the ingest cycle, and verifies normalized-event and journal writes on the active backend surface.

## Notes

- TradingView cannot send custom auth headers, so the Worker uses the query-string secret.
- The backend still accepts `signal_key` on the batch endpoint for direct probes and internal tooling.
- The current bridge contract preserves stable batch fields, stable event fields, and a dedicated `research` block for experimental telemetry.
- The refinement process and promotion criteria are documented in `docs/tradingview_bridge_refinement_runbook.md`.
