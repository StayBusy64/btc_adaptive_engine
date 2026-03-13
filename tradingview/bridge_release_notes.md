# Bridge Release Notes

## Current release: bridge_signal_sender_v2.1.0

- Release version: 2.1.0
- Strategy ID: bridge_signal_sender_v2
- Contract version: tv-bridge-batch-v1
- Telemetry schema version: tv-telemetry-v1
- Release channel: production

## Pine defaults

- Signal source: tradingview
- Signal namespace: ghostprint
- Signal family: momentum
- Long signal type: continuation
- Short signal type: continuation
- EMA lengths: fast 9, slow 21, trend 50
- RSI: length 14, long threshold 52.0, short threshold 48.0
- ATR length: 14
- Volume SMA length: 20
- EMA slope lookback: 3
- Confirmed bars only: True

## Alert settings

- Name: BTCUSDT Bridge Sender v2
- Condition: Bridge Signal Sender -> Any alert() function call
- Trigger: Once Per Bar Close
- Message: bridge
- Webhook URL: https://tv-webhook.staybusyent.workers.dev/?secret=<TV_WEBHOOK_SECRET>

## Alert notes

- Delete and recreate alerts whenever Pine release metadata changes.
- Keep the alert message minimal because the script-generated alert() body carries the contract.
- Use the Worker URL, not the direct backend batch endpoint, for live TradingView alerts.

## Stable telemetry contract

- Stable batch fields: source, namespace, symbol, chart_tf, batch_id, batch_trigger_side, batch_size, batch_close_time, confirmed, contract_version, telemetry_schema_version, release_id, release_version, release_channel, events
- Stable event fields: event_id, event_time, side, signal_type, signal_family, signal_name, strategy_id, price, confirmed, micro, macro, research
- Stable micro fields: ticker, tickerid, exchange, base_currency, quote_currency, timeframe, strategy_id, signal_name, open, high, low, close, hl2, hlc3, ohlc4, volume, bar_time, bar_open_time, bar_close_time, bar_index, fast_ema, slow_ema, trend_ema, rsi, atr, atr_pct, vol_sma, rel_volume, ema_spread, ema_spread_abs, ema_spread_bps, fast_slope, slow_slope, dist_fast, dist_slow, dist_fast_bps, dist_slow_bps, candle_range, candle_body, upper_wick, lower_wick, body_to_range, upper_wick_to_range, lower_wick_to_range, is_bull_body, is_bear_body, is_expansion_bar, is_compression_bar
- Stable macro fields: trend_direction, regime_tag, price_vs_trend, momentum_regime, volume_regime, candle_bias, wick_bias, ema_bull_stack, ema_bear_stack, use_rsi_filter, rsi_long_threshold, rsi_short_threshold, fast_len, slow_len, trend_len, rsi_len, atr_len, vol_sma_len, slope_lookback, confirmed_bars_only

## Emitted research telemetry

- Research fields emitted in this release: signal_quality_score, trend_slope_score, continuation_confidence, mean_reversion_risk, regime_bias_score, contradiction_pressure

## Deferred roadmap

- Use research fields to score cohort usefulness by release_version and signal side.
- Add contradiction-driven decay refresh/fade logic to backend weighting services.
- Promote or retire research fields only after cohort-conditioned outcome review.
- Extend Pine with acceptance vs rejection and continuation vs exhaustion refinements once feature usefulness data is stable.
