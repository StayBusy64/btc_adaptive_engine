# TradingView Bridge Refinement Runbook

## Goal

Turn the TradingView -> Worker -> backend path into a versioned refinement loop where every Pine release is explicit, auditable, and comparable.

## Implemented in the first slice

- The canonical release state now lives in tradingview/bridge_manifest.json.
- Pine release metadata is treated as first-class contract data: strategy_id, release_version, contract_version, telemetry_schema_version, and release_channel.
- Experimental telemetry has a dedicated research block so new fields can be emitted without polluting the stable contract.
- Backend ingest and signal journaling preserve release context and research telemetry for future weighting, promotion, and decay work.
- tools/build_bridge_pine.py validates Pine defaults against the manifest and regenerates tradingview/bridge_release_notes.md.

## Stable vs research telemetry

Stable telemetry is the part of the contract downstream systems should rely on directly:

- batch envelope fields
- event identifiers and side/signal metadata
- core micro context such as OHLC, EMA, RSI, ATR, volume, and bar timing
- core macro context such as trend direction, regime tag, and input settings

Research telemetry is where new ideas go first. In this slice, the research block is used for:

- signal_quality_score
- trend_slope_score
- continuation_confidence
- mean_reversion_risk
- regime_bias_score
- contradiction_pressure

Promotion rule:

- keep a field in research until recent cohort/outcome review shows it improves filtering or learning value
- only promote it into the stable contract once it is interpretable and reliably useful

Removal rule:

- remove or demote research fields that add noise, duplicate existing information, or stop helping recent cohorts

## Decay and weighting model

This slice adds the structural configuration only. The operational logic is intentionally deferred until the versioned telemetry has enough clean history.

Configured decay concepts live in tradingview/bridge_manifest.json:

- recent_impulse: fast-decay context for immediate follow-through and displacement
- recent_rejection: fast-decay context for wick/rejection pressure
- regime_bias: slower macro memory for trend vs chop behavior
- session_bias: slower session-conditioned memory

Planned behavior for the next slice:

- refresh on confirmation
- faster fade on contradiction
- bounded state values with explicit floor/ceiling logic
- regime-conditioned feature weighting and side asymmetry

## Release workflow

1. Update tradingview/bridge_manifest.json.
2. Adjust tradingview/bridge_signal_sender.pine to match the manifest-driven defaults and the target release logic.
3. Run python tools/build_bridge_pine.py --check-pine --write-release-notes.
4. Review tradingview/bridge_release_notes.md for alert settings and emitted research telemetry.
5. Replace the TradingView alert instead of editing the old one in place.
6. Run powershell -ExecutionPolicy Bypass -File .\test_pipeline.ps1.
7. Verify the newest normalized event and signal journal row carry the expected strategy_id and release_version.

## How to verify the live alert version

Use one or more of these checks:

- open tradingview/bridge_release_notes.md and confirm the alert is configured with the current strategy settings
- run the pipeline probe and inspect the newest normalized event row for strategy_id and release_version
- inspect data/state/raw and recent event rows to confirm the event batch matches the current release metadata
- delete and recreate the TradingView alert whenever release metadata changes

## What is deferred

- release-conditioned feature usefulness scoring
- contradiction-driven state decay in backend weighting
- automatic promotion/demotion of research fields
- Pine generation from config alone without hand-edited logic blocks

The current slice is the foundation layer. It makes releases explicit, keeps telemetry evolvable, and ensures the backend stores enough context to support the next refinement stages.