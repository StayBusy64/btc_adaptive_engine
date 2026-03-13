# Webhook Archival Flow

## Overview

This document describes the **direct archival path** for TradingView signals:

```
TradingView alert
        │
        ▼
Cloudflare Worker  (validates secret, strips query-string credentials)
        │
        ▼
POST /webhooks/tradingview/archive  (FastAPI backend — auth via X-SIGNAL-KEY)
        │
        ▼
data/webhook_ingest/{strategy_id}__{release_version}__{timestamp}.json
        │
        ▼
GitHub Action (.github/workflows/commit_webhook_ingest.yml)
        │
        ▼
Committed to the repository  ← single source of truth for raw ingest history
```

The Pine script is **never modified** by this flow.  
Stored JSON files are raw telemetry only – they serve as the data lake for future Pine refinement and research.

---

## Components

| Component | Path | Purpose |
|---|---|---|
| Archive module | `backend/ingest_webhook_payload.py` | Writes webhook JSON to `data/webhook_ingest/` |
| Archive endpoint | `POST /webhooks/tradingview/archive` in `backend/api_server.py` | HTTP entry point for the archive path |
| Cloudflare Worker | `cloudflare/webhook_archive_worker.js` | Receives TradingView webhook, forwards to archive endpoint |
| Ingest directory | `data/webhook_ingest/` | Stores archived JSON files; tracked in git |
| Commit automation | `.github/workflows/commit_webhook_ingest.yml` | Commits new ingest files on a schedule |

---

## File naming convention

Every archived file is named:

```
{strategy_id}__{release_version}__{YYYY-MM-DDTHH-MM-SS}.json
```

Example:

```
bridge_signal_sender_v2__2.1.0__2026-03-13T23-55-12.json
```

The three components map directly to Pine payload fields:
- `strategy_id` — from `strategyId` in the alert
- `release_version` — from `releaseVersion` in the alert
- timestamp — UTC time the archive endpoint received the request

---

## Setting up Cloudflare

### 1. Create a new Cloudflare Worker

1. Log in to the [Cloudflare dashboard](https://dash.cloudflare.com/).
2. Go to **Workers & Pages → Create**.
3. Choose **"Import a Worker"** and upload `cloudflare/webhook_archive_worker.js`,  
   or paste the file contents into the inline editor.
4. Name the Worker (e.g. `tv-archive-worker`).

### 2. Set Worker secrets

Set the following secrets via the Cloudflare dashboard (**Worker → Settings → Variables → Secrets**) or with the Wrangler CLI:

```sh
wrangler secret put TV_WEBHOOK_SECRET   # the secret TradingView embeds in ?secret=
wrangler secret put BACKEND_SIGNAL_KEY  # value of TRADINGVIEW_INGEST_SIGNAL_KEY on the backend
wrangler secret put BACKEND_BASE_URL    # e.g. https://your-app.onrender.com
```

### 3. Configure the TradingView alert

Use the Worker URL as the webhook URL in TradingView:

```
https://<your-worker>.workers.dev/?secret=<TV_WEBHOOK_SECRET>
```

Set the alert:
- **Condition:** `Bridge Signal Sender → Any alert() function call`
- **Trigger:** `Once Per Bar Close`
- **Message:** `bridge` (the Pine script generates the full payload automatically)
- **Webhook URL:** the Worker URL above

> The Worker validates `?secret=` and strips it before forwarding to the backend, so TradingView's secret never reaches the FastAPI process.

---

## Archive endpoint

```
POST /webhooks/tradingview/archive
```

**Auth:** provide the signal key as either:
- `X-SIGNAL-KEY` request header, or
- `?signal_key=` query parameter

**Body:** any valid JSON object.  
No schema validation beyond JSON parsing – the payload is stored verbatim.

**Response (201):**

```json
{
  "status": "archived",
  "written_to": "data/webhook_ingest/bridge_signal_sender_v2__2.1.0__2026-03-13T23-55-12.json",
  "strategy_id": "bridge_signal_sender_v2",
  "release_version": "2.1.0",
  "batch_id": "batch-20260313-235512-abc"
}
```

---

## Automated commits

The GitHub Action at `.github/workflows/commit_webhook_ingest.yml`:

- Runs **every hour** (configurable via cron)
- Stages all new/modified files under `data/webhook_ingest/`
- Commits them with the message `chore: archive N new webhook ingest file(s) [skip ci]`
- Can be triggered manually via **Actions → Commit Webhook Ingest Files → Run workflow**
- Supports a `dry_run` input to preview changes without pushing

The `[skip ci]` tag prevents the commit from triggering further CI runs.

---

## Using archived data for research

Archived files are plain JSON and can be loaded with any standard tooling:

```python
import json
from pathlib import Path

ingest_dir = Path("data/webhook_ingest")

payloads = [
    json.loads(f.read_text())
    for f in sorted(ingest_dir.glob("*.json"))
    if not f.name.startswith(".")
]

# Filter by strategy
v2_payloads = [p for p in payloads if p.get("strategy_id") == "bridge_signal_sender_v2"]

# Filter by release
v2_1_payloads = [p for p in v2_payloads if p.get("release_version") == "2.1.0"]
```

Files are sorted chronologically by filename (the timestamp component is ISO-8601 sortable).

---

## Important constraints

- **Pine is never auto-patched.** The archive endpoint stores data only.
- **No downstream pipelines are triggered** by the archive endpoint. It is a pure write path.
- **The batch ingest path** (`POST /webhooks/tradingview/batch`) remains the live production pipeline path. The archive path is additive and independent.
- **Refinement is manual.** When using archived data to refine the Pine indicator, follow the workflow in `docs/tradingview_bridge_refinement_runbook.md`.
