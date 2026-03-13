import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import event_writer
from backend.api_server import app
from backend.feature_engine import run_feature_pipeline_for_latest_bar


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_feature_engine.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"

    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    monkeypatch.setenv("SIGNAL_WEBHOOK_KEY", "test-signal-key")

    event_writer.init_db()
    return test_db_path


@pytest.fixture()
def client(isolated_db):
    with TestClient(app) as test_client:
        yield test_client


def _seed_bar_series(*, symbol: str = "BTCUSDT.P", timeframe: str = "1m", count: int = 80) -> int:
    base_timestamp = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
    latest_id = 0

    for index in range(count):
        open_price = 100.0 + (index * 0.35)
        close_price = open_price + (0.18 if index % 4 != 0 else -0.06)
        high_price = max(open_price, close_price) + 0.24 + ((index % 5) * 0.01)
        low_price = min(open_price, close_price) - 0.19 - ((index % 3) * 0.01)
        volume = 1000.0 + (index * 7.5)
        timestamp = (base_timestamp + timedelta(minutes=index)).isoformat()

        latest_id = event_writer.insert_bar_state(
            timestamp=timestamp,
            symbol=symbol,
            timeframe=timeframe,
            long_score=0.65 if close_price >= open_price else 0.25,
            short_score=0.65 if close_price < open_price else 0.25,
            no_trade_score=0.1,
            pressure_index=0.55,
            participation_score=0.6,
            confidence_seed=0.5,
            payload_json={
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            },
        )

    return latest_id


def test_feature_pipeline_persists_registry_snapshots_and_outputs(isolated_db):
    latest_bar_id = _seed_bar_series()

    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_bar_id,
        lookback=200,
    )

    assert result is not None
    assert result["snapshot_id"] > 0

    feature_values = result["feature_values"]
    assert "candle_body_to_range" in feature_values
    assert "range_expansion_ratio" in feature_values
    assert "atr_14" in feature_values
    assert "ema_21" in feature_values
    assert "rsi_14" in feature_values

    with event_writer.get_connection() as conn:
        registry_count = int(conn.execute("SELECT COUNT(*) AS count FROM feature_registry").fetchone()["count"])
        snapshot_count = int(conn.execute("SELECT COUNT(*) AS count FROM feature_snapshots").fetchone()["count"])
        value_count = int(conn.execute("SELECT COUNT(*) AS count FROM feature_snapshot_values").fetchone()["count"])
        regime_count = int(conn.execute("SELECT COUNT(*) AS count FROM regime_states").fetchone()["count"])
        model_count = int(conn.execute("SELECT COUNT(*) AS count FROM model_predictions").fetchone()["count"])
        bar_row = conn.execute("SELECT payload_json FROM bar_states WHERE id = ?", (latest_bar_id,)).fetchone()

    assert registry_count >= 30
    assert snapshot_count == 1
    assert value_count >= 20
    assert regime_count == 1
    assert model_count == 1

    payload = json.loads(bar_row["payload_json"])
    assert payload["feature_snapshot_id"] == result["snapshot_id"]
    assert "computed_features" in payload
    assert "computed_regime" in payload
    assert "computed_model" in payload


def test_feature_pipeline_outputs_real_nontrivial_values(isolated_db):
    latest_bar_id = _seed_bar_series(count=90)

    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_bar_id,
        lookback=250,
    )

    assert result is not None
    features = result["feature_values"]

    assert features["candle_range_points"] > 0.0
    assert features["range_expansion_ratio"] > 0.0
    assert features["atr_14"] > 0.0
    assert features["realized_vol_10"] >= 0.0
    assert features["ema_9"] != features["ema_55"]
    assert features["trend_alignment_score"] in {-1.0, 0.0, 1.0}
    assert 0.0 <= features["rsi_14"] <= 100.0
    assert 0.0 <= features["stochastic_k_14"] <= 100.0


def test_tradingview_webhook_runs_feature_pipeline_and_persists_outputs(client):
    base_timestamp = datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc)
    last_response_body = None

    for index in range(40):
        open_price = 200.0 + (index * 0.2)
        close_price = open_price + (0.1 if index % 3 else -0.04)
        high_price = max(open_price, close_price) + 0.18
        low_price = min(open_price, close_price) - 0.16

        payload = {
            "symbol": "BTCUSDT.P",
            "timeframe": "1m",
            "timestamp": (base_timestamp + timedelta(minutes=index)).isoformat(),
            "long_score": 0.6,
            "short_score": 0.3,
            "no_trade_score": 0.1,
            "pressure_index": 0.58,
            "volatility_state": "normal",
            "participation_score": 0.63,
            "confidence_seed": 0.55,
            "payload_json": {
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": 1200 + (index * 6),
            },
        }

        response = client.post("/webhook/tradingview", json=payload)
        assert response.status_code == 201
        last_response_body = response.json()

    assert last_response_body is not None
    assert "feature_snapshot_id" in last_response_body

    with event_writer.get_connection() as conn:
        snapshot_count = int(conn.execute("SELECT COUNT(*) AS count FROM feature_snapshots").fetchone()["count"])
        regime_count = int(conn.execute("SELECT COUNT(*) AS count FROM regime_states").fetchone()["count"])
        model_count = int(conn.execute("SELECT COUNT(*) AS count FROM model_predictions").fetchone()["count"])
        latest_bar_payload_row = conn.execute(
            "SELECT payload_json FROM bar_states ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert snapshot_count >= 1
    assert regime_count >= 1
    assert model_count >= 1

    latest_payload = json.loads(latest_bar_payload_row["payload_json"])
    assert "computed_features" in latest_payload
    assert "computed_regime" in latest_payload
    assert "computed_model" in latest_payload

import backend.feature_engine as fe_mod
from backend.feature_engine import get_default_feature_pipeline, FeatureContext, classify_regime, run_feature_pipeline_for_latest_bar
from backend.volume_profile_engine import compute_and_store_volume_profile_snapshot, VolumeProfileSnapshot

def test_vp_profile_aware_features_generation(isolated_db, monkeypatch):
    # Test that setting a volume profile triggers the correct boolean flags in the pipeline
    latest_bar_id = _seed_bar_series(count=100) # This creates bars, last one closes below open
    
    # We will mock compute_and_store_volume_profile_snapshot to return specific profiles and observe the flags in the payload
    # Let's read the real bars to give to our mocked profile
    rows = event_writer.get_recent_bar_states_for_symbol_timeframe(symbol="BTCUSDT.P", timeframe="1m", limit=1)
    
    def _mock_reversion_snapshot(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11", symbol="BTC", timeframe="1m", engine_version="v1",
            poc=100.0, vah=105.0, val=95.0, profile_high=110.0, profile_low=90.0,
            shape_label="b_shape", balance_state="balanced", source_bar_count=10,
            profile_range=20.0, value_area_width=10.0, value_area_width_pct=0.5,
            poc_relative=0.5, poc_distance_from_mid=0.0,
            close_position_in_profile=0.9, distance_to_poc=8.0, distance_to_vah=3.0, distance_to_val=13.0,
            distance_to_poc_pct=0.15, distance_to_vah_pct=0.15, distance_to_val_pct=0.65,
            inside_value_area=False, above_vah=True, below_val=False
        )
    
    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_reversion_snapshot)
    
    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P", timeframe="1m", source_bar_id=latest_bar_id, lookback=100
    )
    assert result["feature_values"]["vp_reversion_candidate"] == 1
    assert result["feature_values"]["vp_acceptance_above_value"] == 0 # close pos is 0.9 (not > 1.0)
    
    def _mock_acceptance_above(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11", symbol="BTC", timeframe="1m", engine_version="v1",
            poc=100.0, vah=105.0, val=95.0, profile_high=110.0, profile_low=90.0,
            shape_label="b_shape", balance_state="balanced", source_bar_count=10,
            profile_range=20.0, value_area_width=10.0, value_area_width_pct=0.5,
            poc_relative=0.5, poc_distance_from_mid=0.0,
            close_position_in_profile=1.1, distance_to_poc=12.0, distance_to_vah=7.0, distance_to_val=17.0,
            distance_to_poc_pct=0.6, distance_to_vah_pct=0.35, distance_to_val_pct=0.85,
            inside_value_area=False, above_vah=True, below_val=False
        )
        
    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_acceptance_above)
    result = run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=latest_bar_id, lookback=100)
    assert result["feature_values"]["vp_acceptance_above_value"] == 1
    assert result["feature_values"]["vp_acceptance_below_value"] == 0
    assert result["feature_values"]["vp_reversion_candidate"] == 0
    
    def _mock_acceptance_below(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11", symbol="BTC", timeframe="1m", engine_version="v1",
            poc=100.0, vah=105.0, val=95.0, profile_high=110.0, profile_low=90.0,
            shape_label="b_shape", balance_state="balanced", source_bar_count=10,
            profile_range=20.0, value_area_width=10.0, value_area_width_pct=0.5,
            poc_relative=0.5, poc_distance_from_mid=0.0,
            close_position_in_profile=-0.1, distance_to_poc=12.0, distance_to_vah=17.0, distance_to_val=7.0,
            distance_to_poc_pct=0.6, distance_to_vah_pct=0.85, distance_to_val_pct=0.35,
            inside_value_area=False, above_vah=False, below_val=True
        )

    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_acceptance_below)
    result = run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=latest_bar_id, lookback=100)
    assert result["feature_values"]["vp_acceptance_below_value"] == 1
    
    def _mock_balanced_rotation(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11", symbol="BTC", timeframe="1m", engine_version="v1",
            poc=100.0, vah=105.0, val=95.0, profile_high=110.0, profile_low=90.0,
            shape_label="d_shape", balance_state="balanced", source_bar_count=10,
            profile_range=20.0, value_area_width=4.0, value_area_width_pct=0.2,
            poc_relative=0.5, poc_distance_from_mid=0.0,
            close_position_in_profile=0.5, distance_to_poc=0.0, distance_to_vah=5.0, distance_to_val=5.0,
            distance_to_poc_pct=0.0, distance_to_vah_pct=0.25, distance_to_val_pct=0.25,
            inside_value_area=True, above_vah=False, below_val=False
        )

    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_balanced_rotation)
    result = run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=latest_bar_id, lookback=100)
    assert result["feature_values"]["vp_balanced_rotation_context"] == 1
    assert result["feature_values"]["vp_compressed_value_area"] == 1
    assert result["feature_values"]["vp_poc_magnet_context"] == 1




def test_vp_multi_bar_acceptance_rejection_flags(isolated_db, monkeypatch):
    import backend.feature_engine as fe_mod
    from backend.volume_profile_engine import VolumeProfileSnapshot
    def _mock_snapshot(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11", symbol="BTC", timeframe="1m", engine_version="v1",
            poc=100.0, vah=105.0, val=95.0, profile_high=110.0, profile_low=90.0,
            shape_label="d_shape", balance_state="balanced", source_bar_count=10,
            profile_range=20.0, value_area_width=10.0, value_area_width_pct=0.5,
            poc_relative=0.5, poc_distance_from_mid=0.0,
            close_position_in_profile=0.5, distance_to_poc=0.0, distance_to_vah=5.0, distance_to_val=5.0,
            distance_to_poc_pct=0.0, distance_to_vah_pct=0.25, distance_to_val_pct=0.25,
            inside_value_area=True, above_vah=False, below_val=False
        )
    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_snapshot)
    def _mock_bars_acceptance_above(*args, **kwargs):
        return [{'id': 1, 'timestamp': '2026-03-11T12:01:00Z', 'payload_json': '{"close": 106.0, "high": 107.0, "low": 105.0, "volume": 100, "open": 100.0}'}, {'id': 2, 'timestamp': '2026-03-11T12:02:00Z', 'payload_json': '{"close": 104.0, "high": 106.0, "low": 103.0, "volume": 100, "open": 100.0}'}, {'id': 3, 'timestamp': '2026-03-11T12:03:00Z', 'payload_json': '{"close": 107.0, "high": 108.0, "low": 106.0, "volume": 100, "open": 100.0}'}]
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", _mock_bars_acceptance_above)
    result = fe_mod.run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=3, lookback=3)
    assert result["feature_values"]["vp_acceptance_above_value_confirmed"] == 1
    assert result["feature_values"]["vp_rejection_back_into_value_from_above"] == 0
    def _mock_bars_acceptance_below(*args, **kwargs):
        return [{'id': 1, 'timestamp': '2026-03-11T12:01:00Z', 'payload_json': '{"close": 94.0, "high": 95.0, "low": 93.0, "volume": 100, "open": 100.0}'}, {'id': 2, 'timestamp': '2026-03-11T12:02:00Z', 'payload_json': '{"close": 96.0, "high": 97.0, "low": 95.0, "volume": 100, "open": 100.0}'}, {'id': 3, 'timestamp': '2026-03-11T12:03:00Z', 'payload_json': '{"close": 93.0, "high": 94.0, "low": 92.0, "volume": 100, "open": 100.0}'}]
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", _mock_bars_acceptance_below)
    result = fe_mod.run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=3, lookback=3)
    assert result["feature_values"]["vp_acceptance_below_value_confirmed"] == 1
    assert result["feature_values"]["vp_rejection_back_into_value_from_below"] == 0
    def _mock_bars_rejection_above(*args, **kwargs):
        return [{'id': 1, 'timestamp': '2026-03-11T12:01:00Z', 'payload_json': '{"close": 100.0, "high": 101.0, "low": 99.0, "volume": 100, "open": 100.0}'}, {'id': 2, 'timestamp': '2026-03-11T12:02:00Z', 'payload_json': '{"close": 106.0, "high": 107.0, "low": 105.0, "volume": 100, "open": 100.0}'}, {'id': 3, 'timestamp': '2026-03-11T12:03:00Z', 'payload_json': '{"close": 104.0, "high": 106.0, "low": 103.0, "volume": 100, "open": 100.0}'}]
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", _mock_bars_rejection_above)
    result = fe_mod.run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=3, lookback=3)
    assert result["feature_values"]["vp_rejection_back_into_value_from_above"] == 1
    def _mock_bars_rejection_below(*args, **kwargs):
        return [{'id': 1, 'timestamp': '2026-03-11T12:01:00Z', 'payload_json': '{"close": 100.0, "high": 101.0, "low": 99.0, "volume": 100, "open": 100.0}'}, {'id': 2, 'timestamp': '2026-03-11T12:02:00Z', 'payload_json': '{"close": 94.0, "high": 95.0, "low": 93.0, "volume": 100, "open": 100.0}'}, {'id': 3, 'timestamp': '2026-03-11T12:03:00Z', 'payload_json': '{"close": 96.0, "high": 97.0, "low": 95.0, "volume": 100, "open": 100.0}'}]
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", _mock_bars_rejection_below)
    result = fe_mod.run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=3, lookback=3)
    assert result["feature_values"]["vp_rejection_back_into_value_from_below"] == 1
    def _mock_bars_balanced(*args, **kwargs):
        return [{'id': 1, 'timestamp': '2026-03-11T12:01:00Z', 'payload_json': '{"close": 100.5, "high": 101.0, "low": 99.0, "volume": 100, "open": 100.0}'}, {'id': 2, 'timestamp': '2026-03-11T12:02:00Z', 'payload_json': '{"close": 99.5, "high": 100.5, "low": 98.0, "volume": 100, "open": 100.0}'}, {'id': 3, 'timestamp': '2026-03-11T12:03:00Z', 'payload_json': '{"close": 100.0, "high": 101.0, "low": 99.0, "volume": 100, "open": 100.0}'}]
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", _mock_bars_balanced)
    result = fe_mod.run_feature_pipeline_for_latest_bar(symbol="BTCUSDT.P", timeframe="1m", source_bar_id=3, lookback=3)
    assert result["feature_values"]["vp_poc_magnet_context"] == 1


def test_vp_poc_migration_observables_exposed(isolated_db, monkeypatch):
    latest_bar_id = _seed_bar_series(count=60)

    def _mock_snapshot_with_migration(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11",
            symbol="BTC",
            timeframe="1m",
            engine_version="v1",
            poc=100.0,
            vah=105.0,
            val=95.0,
            profile_high=110.0,
            profile_low=90.0,
            shape_label="d_shape",
            balance_state="balanced",
            source_bar_count=10,
            profile_range=20.0,
            value_area_width=10.0,
            value_area_width_pct=0.5,
            poc_relative=0.5,
            poc_distance_from_mid=0.0,
            # Keep price-interaction fields absent to prove migration fields are independent.
            distance_to_poc_pct=None,
            poc_migration_delta=1.2,
            poc_migrating_up=True,
            poc_migrating_down=False,
            poc_migration_strength=12.0,
        )

    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_snapshot_with_migration)
    result = run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=latest_bar_id,
        lookback=100,
    )

    assert result is not None
    features = result["feature_values"]
    assert features["vp_poc_migration_delta"] == pytest.approx(1.2)
    assert features["vp_poc_migrating_up"] == 1
    assert features["vp_poc_migrating_down"] == 0
    assert features["vp_poc_migration_strength"] == pytest.approx(12.0)


def _bars_with_closes(*, closes: list[float]) -> list[dict]:
    rows: list[dict] = []
    for index, close in enumerate(closes, start=1):
        rows.append(
            {
                "id": index,
                "timestamp": f"2026-03-11T12:0{index}:00Z",
                "payload_json": (
                    f'{{"close": {close}, "high": {close + 1.0}, "low": {close - 1.0}, '
                    f'"volume": 100, "open": {close}}}'
                ),
            }
        )
    return rows


@pytest.mark.parametrize(
    ("migrating_up", "migrating_down", "bars", "expected"),
    [
        (
            True,
            False,
            _bars_with_closes(closes=[100.0, 100.5, 100.2]),
            {
                "vp_equilibrium_rising_context": 1,
                "vp_equilibrium_falling_context": 0,
                "vp_equilibrium_stable_context": 0,
                "vp_value_shift_with_acceptance_up": 0,
                "vp_value_shift_with_acceptance_down": 0,
            },
        ),
        (
            False,
            True,
            _bars_with_closes(closes=[100.0, 99.5, 99.8]),
            {
                "vp_equilibrium_rising_context": 0,
                "vp_equilibrium_falling_context": 1,
                "vp_equilibrium_stable_context": 0,
                "vp_value_shift_with_acceptance_up": 0,
                "vp_value_shift_with_acceptance_down": 0,
            },
        ),
        (
            False,
            False,
            _bars_with_closes(closes=[100.0, 100.1, 99.9]),
            {
                "vp_equilibrium_rising_context": 0,
                "vp_equilibrium_falling_context": 0,
                "vp_equilibrium_stable_context": 1,
                "vp_value_shift_with_acceptance_up": 0,
                "vp_value_shift_with_acceptance_down": 0,
            },
        ),
        (
            True,
            False,
            _bars_with_closes(closes=[106.0, 107.0, 108.0]),
            {
                "vp_equilibrium_rising_context": 1,
                "vp_equilibrium_falling_context": 0,
                "vp_equilibrium_stable_context": 0,
                "vp_value_shift_with_acceptance_up": 1,
                "vp_value_shift_with_acceptance_down": 0,
            },
        ),
        (
            False,
            True,
            _bars_with_closes(closes=[94.0, 93.0, 92.0]),
            {
                "vp_equilibrium_rising_context": 0,
                "vp_equilibrium_falling_context": 1,
                "vp_equilibrium_stable_context": 0,
                "vp_value_shift_with_acceptance_up": 0,
                "vp_value_shift_with_acceptance_down": 1,
            },
        ),
    ],
)
def test_vp_value_migration_context_flags(isolated_db, monkeypatch, migrating_up, migrating_down, bars, expected):
    def _mock_snapshot(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11",
            symbol="BTC",
            timeframe="1m",
            engine_version="v1",
            poc=100.0,
            vah=105.0,
            val=95.0,
            profile_high=110.0,
            profile_low=90.0,
            shape_label="d_shape",
            balance_state="balanced",
            source_bar_count=10,
            profile_range=20.0,
            value_area_width=10.0,
            value_area_width_pct=0.5,
            poc_relative=0.5,
            poc_distance_from_mid=0.0,
            distance_to_poc_pct=0.2,
            poc_migration_delta=1.0 if migrating_up else (-1.0 if migrating_down else 0.0),
            poc_migrating_up=migrating_up,
            poc_migrating_down=migrating_down,
            poc_migration_strength=1.0 if (migrating_up or migrating_down) else 0.0,
        )

    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_snapshot)
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", lambda *args, **kwargs: bars)

    result = fe_mod.run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=3,
        lookback=3,
    )
    assert result is not None

    features = result["feature_values"]
    assert features["vp_equilibrium_rising_context"] == expected["vp_equilibrium_rising_context"]
    assert features["vp_equilibrium_falling_context"] == expected["vp_equilibrium_falling_context"]
    assert features["vp_equilibrium_stable_context"] == expected["vp_equilibrium_stable_context"]
    assert features["vp_value_shift_with_acceptance_up"] == expected["vp_value_shift_with_acceptance_up"]
    assert features["vp_value_shift_with_acceptance_down"] == expected["vp_value_shift_with_acceptance_down"]


@pytest.mark.parametrize(
    (
        "migrating_up",
        "migrating_down",
        "distance_to_poc_pct",
        "inside_value_area",
        "bars",
        "expected",
    ),
    [
        (
            False,
            False,
            0.30,
            False,
            _bars_with_closes(closes=[100.0, 106.0, 104.0]),
            {
                "vp_failed_auction_above": 1,
                "vp_failed_auction_below": 0,
                "vp_reversion_to_value_from_above_context": 0,
                "vp_reversion_to_value_from_below_context": 0,
            },
        ),
        (
            False,
            False,
            0.30,
            False,
            _bars_with_closes(closes=[100.0, 94.0, 96.0]),
            {
                "vp_failed_auction_above": 0,
                "vp_failed_auction_below": 1,
                "vp_reversion_to_value_from_above_context": 0,
                "vp_reversion_to_value_from_below_context": 0,
            },
        ),
        (
            False,
            False,
            0.20,
            False,
            _bars_with_closes(closes=[100.0, 106.0, 104.0]),
            {
                "vp_failed_auction_above": 1,
                "vp_failed_auction_below": 0,
                "vp_reversion_to_value_from_above_context": 1,
                "vp_reversion_to_value_from_below_context": 0,
            },
        ),
        (
            False,
            False,
            0.20,
            False,
            _bars_with_closes(closes=[100.0, 94.0, 96.0]),
            {
                "vp_failed_auction_above": 0,
                "vp_failed_auction_below": 1,
                "vp_reversion_to_value_from_above_context": 0,
                "vp_reversion_to_value_from_below_context": 1,
            },
        ),
        (
            True,
            False,
            0.20,
            False,
            _bars_with_closes(closes=[100.0, 106.0, 104.0]),
            {
                "vp_failed_auction_above": 0,
                "vp_failed_auction_below": 0,
                "vp_reversion_to_value_from_above_context": 0,
                "vp_reversion_to_value_from_below_context": 0,
            },
        ),
    ],
)
def test_vp_failed_auction_context_flags(
    isolated_db,
    monkeypatch,
    migrating_up,
    migrating_down,
    distance_to_poc_pct,
    inside_value_area,
    bars,
    expected,
):
    def _mock_snapshot(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11",
            symbol="BTC",
            timeframe="1m",
            engine_version="v1",
            poc=100.0,
            vah=105.0,
            val=95.0,
            profile_high=110.0,
            profile_low=90.0,
            shape_label="d_shape",
            balance_state="balanced",
            source_bar_count=10,
            profile_range=20.0,
            value_area_width=10.0,
            value_area_width_pct=0.5,
            poc_relative=0.5,
            poc_distance_from_mid=0.0,
            distance_to_poc_pct=distance_to_poc_pct,
            inside_value_area=inside_value_area,
            poc_migration_delta=1.0 if migrating_up else (-1.0 if migrating_down else 0.0),
            poc_migrating_up=migrating_up,
            poc_migrating_down=migrating_down,
            poc_migration_strength=1.0 if (migrating_up or migrating_down) else 0.0,
        )

    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_snapshot)
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", lambda *args, **kwargs: bars)

    result = fe_mod.run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=3,
        lookback=3,
    )
    assert result is not None

    features = result["feature_values"]
    assert features["vp_failed_auction_above"] == expected["vp_failed_auction_above"]
    assert features["vp_failed_auction_below"] == expected["vp_failed_auction_below"]
    assert (
        features["vp_reversion_to_value_from_above_context"]
        == expected["vp_reversion_to_value_from_above_context"]
    )
    assert (
        features["vp_reversion_to_value_from_below_context"]
        == expected["vp_reversion_to_value_from_below_context"]
    )


@pytest.mark.parametrize(
    (
        "above_vah",
        "below_val",
        "close_position_in_profile",
        "migrating_up",
        "migrating_down",
        "bars",
        "expected",
    ),
    [
        (
            True,
            False,
            1.1,
            True,
            False,
            _bars_with_closes(closes=[106.0, 104.0, 107.0]),
            {
                "vp_acceptance_outside_value_above": 1,
                "vp_acceptance_outside_value_below": 0,
                "vp_continuation_auction_up": 1,
                "vp_continuation_auction_down": 0,
            },
        ),
        (
            True,
            False,
            1.1,
            False,
            False,
            _bars_with_closes(closes=[106.0, 104.0, 107.0]),
            {
                "vp_acceptance_outside_value_above": 1,
                "vp_acceptance_outside_value_below": 0,
                "vp_continuation_auction_up": 0,
                "vp_continuation_auction_down": 0,
            },
        ),
        (
            False,
            True,
            -0.1,
            False,
            True,
            _bars_with_closes(closes=[94.0, 96.0, 93.0]),
            {
                "vp_acceptance_outside_value_above": 0,
                "vp_acceptance_outside_value_below": 1,
                "vp_continuation_auction_up": 0,
                "vp_continuation_auction_down": 1,
            },
        ),
        (
            False,
            False,
            0.5,
            False,
            False,
            _bars_with_closes(closes=[100.0, 100.5, 100.2]),
            {
                "vp_acceptance_outside_value_above": 0,
                "vp_acceptance_outside_value_below": 0,
                "vp_continuation_auction_up": 0,
                "vp_continuation_auction_down": 0,
            },
        ),
    ],
)
def test_vp_acceptance_and_continuation_context_flags(
    isolated_db,
    monkeypatch,
    above_vah,
    below_val,
    close_position_in_profile,
    migrating_up,
    migrating_down,
    bars,
    expected,
):
    def _mock_snapshot(*args, **kwargs):
        return VolumeProfileSnapshot(
            timestamp="2026-03-11",
            symbol="BTC",
            timeframe="1m",
            engine_version="v1",
            poc=100.0,
            vah=105.0,
            val=95.0,
            profile_high=110.0,
            profile_low=90.0,
            shape_label="d_shape",
            balance_state="balanced",
            source_bar_count=10,
            profile_range=20.0,
            value_area_width=10.0,
            value_area_width_pct=0.5,
            poc_relative=0.5,
            poc_distance_from_mid=0.0,
            close_position_in_profile=close_position_in_profile,
            distance_to_poc_pct=0.3,
            distance_to_vah_pct=0.1 if above_vah else 0.25,
            distance_to_val_pct=0.1 if below_val else 0.25,
            inside_value_area=not above_vah and not below_val,
            above_vah=above_vah,
            below_val=below_val,
            poc_migration_delta=1.0 if migrating_up else (-1.0 if migrating_down else 0.0),
            poc_migrating_up=migrating_up,
            poc_migrating_down=migrating_down,
            poc_migration_strength=1.0 if (migrating_up or migrating_down) else 0.0,
        )

    monkeypatch.setattr(fe_mod, "compute_and_store_volume_profile_snapshot", _mock_snapshot)
    monkeypatch.setattr(fe_mod, "get_recent_bar_states_for_symbol_timeframe", lambda *args, **kwargs: bars)

    result = fe_mod.run_feature_pipeline_for_latest_bar(
        symbol="BTCUSDT.P",
        timeframe="1m",
        source_bar_id=3,
        lookback=3,
    )
    assert result is not None

    features = result["feature_values"]
    assert (
        features["vp_acceptance_outside_value_above"]
        == expected["vp_acceptance_outside_value_above"]
    )
    assert (
        features["vp_acceptance_outside_value_below"]
        == expected["vp_acceptance_outside_value_below"]
    )
    assert features["vp_continuation_auction_up"] == expected["vp_continuation_auction_up"]
    assert features["vp_continuation_auction_down"] == expected["vp_continuation_auction_down"]


@pytest.mark.parametrize(
    ("feature_values", "expected"),
    [
        (
            {
                "vp_reversion_to_value_from_above_context": 1,
                "vp_continuation_auction_up": 1,
                "vp_acceptance_outside_value_above": 1,
                "vp_failed_auction_above": 1,
            },
            "reversion_from_above",
        ),
        (
            {
                "vp_reversion_to_value_from_below_context": 1,
                "vp_continuation_auction_down": 1,
                "vp_acceptance_outside_value_below": 1,
                "vp_failed_auction_below": 1,
            },
            "reversion_from_below",
        ),
        (
            {
                "vp_continuation_auction_up": 1,
                "vp_acceptance_outside_value_above": 1,
            },
            "continuation_up",
        ),
        (
            {
                "vp_continuation_auction_down": 1,
                "vp_acceptance_outside_value_below": 1,
            },
            "continuation_down",
        ),
        ({"vp_acceptance_outside_value_above": 1}, "acceptance_above"),
        ({"vp_acceptance_outside_value_below": 1}, "acceptance_below"),
        ({"vp_failed_auction_above": 1}, "failed_auction_above"),
        ({"vp_failed_auction_below": 1}, "failed_auction_below"),
        ({}, "neutral"),
    ],
)
def test_resolve_vp_auction_regime_priority_and_neutral(feature_values, expected):
    assert fe_mod.resolve_vp_auction_regime(feature_values) == expected


@pytest.mark.parametrize(
    ("vp_auction_regime", "expected"),
    [
        ("reversion_from_below", "long_reversion"),
        ("reversion_from_above", "short_reversion"),
        ("continuation_up", "long_continuation"),
        ("continuation_down", "short_continuation"),
        ("acceptance_above", "neutral"),
        ("acceptance_below", "neutral"),
        ("failed_auction_above", "neutral"),
        ("failed_auction_below", "neutral"),
        ("neutral", "neutral"),
    ],
)
def test_resolve_vp_trade_bias_mappings_and_neutral(vp_auction_regime, expected):
    assert fe_mod.resolve_vp_trade_bias(vp_auction_regime) == expected


@pytest.mark.parametrize(
    ("vp_trade_bias", "expected"),
    [
        ("long_reversion", 1),
        ("short_reversion", 1),
        ("long_continuation", 1),
        ("short_continuation", 1),
        ("neutral", 0),
    ],
)
def test_resolve_vp_trade_bias_actionable_mappings_and_neutral(vp_trade_bias, expected):
    assert fe_mod.resolve_vp_trade_bias_actionable(vp_trade_bias) == expected


@pytest.mark.parametrize(
    ("vp_auction_regime", "vp_trade_bias_actionable", "expected"),
    [
        ("continuation_up", 1, "high"),
        ("continuation_down", 1, "high"),
        ("reversion_from_above", 1, "medium"),
        ("reversion_from_below", 1, "medium"),
        ("acceptance_above", 0, "low"),
        ("acceptance_below", 0, "low"),
        ("failed_auction_above", 0, "low"),
        ("failed_auction_below", 0, "low"),
        ("neutral", 0, "none"),
        ("continuation_up", 0, "none"),
    ],
)
def test_resolve_vp_trade_bias_confidence_mappings_and_fallback(
    vp_auction_regime,
    vp_trade_bias_actionable,
    expected,
):
    assert (
        fe_mod.resolve_vp_trade_bias_confidence(vp_auction_regime, vp_trade_bias_actionable)
        == expected
    )


@pytest.mark.parametrize(
    ("vp_trade_bias_confidence", "expected"),
    [
        ("high", 3),
        ("medium", 2),
        ("low", 1),
        ("none", 0),
        ("unexpected", 0),
    ],
)
def test_resolve_vp_trade_bias_score_mappings_and_fallback(vp_trade_bias_confidence, expected):
    assert fe_mod.resolve_vp_trade_bias_score(vp_trade_bias_confidence) == expected


@pytest.mark.parametrize(
    ("vp_trade_bias_actionable", "vp_trade_bias_score", "expected"),
    [
        (1, 3, 1),
        (1, 2, 1),
        (1, 1, 0),
        (0, 3, 0),
        (1, "unexpected", 0),
    ],
)
def test_resolve_vp_policy_candidate_positive_and_negative(
    vp_trade_bias_actionable,
    vp_trade_bias_score,
    expected,
):
    assert (
        fe_mod.resolve_vp_policy_candidate(vp_trade_bias_actionable, vp_trade_bias_score)
        == expected
    )


@pytest.mark.parametrize(
    ("vp_trade_bias", "vp_policy_candidate", "expected"),
    [
        ("long_reversion", 1, "long"),
        ("long_continuation", 1, "long"),
        ("short_reversion", 1, "short"),
        ("short_continuation", 1, "short"),
        ("neutral", 1, "none"),
        ("long_reversion", 0, "none"),
    ],
)
def test_resolve_vp_policy_side_long_short_and_none(vp_trade_bias, vp_policy_candidate, expected):
    assert fe_mod.resolve_vp_policy_side(vp_trade_bias, vp_policy_candidate) == expected


@pytest.mark.parametrize(
    (
        "vp_auction_regime",
        "vp_trade_bias",
        "vp_trade_bias_confidence",
        "vp_trade_bias_score",
        "expected",
    ),
    [
        (
            "continuation_up",
            "long_continuation",
            "high",
            3,
            "continuation_up|long_continuation|high|score=3",
        ),
        (
            "reversion_from_above",
            "short_reversion",
            "medium",
            2,
            "reversion_from_above|short_reversion|medium|score=2",
        ),
        (
            "acceptance_above",
            "neutral",
            "low",
            1,
            "acceptance_above|neutral|low|score=1",
        ),
        (
            "neutral",
            "neutral",
            "none",
            0,
            "neutral|neutral|none|score=0",
        ),
    ],
)
def test_resolve_vp_trade_bias_summary_representative_mappings(
    vp_auction_regime,
    vp_trade_bias,
    vp_trade_bias_confidence,
    vp_trade_bias_score,
    expected,
):
    assert (
        fe_mod.resolve_vp_trade_bias_summary(
            vp_auction_regime,
            vp_trade_bias,
            vp_trade_bias_confidence,
            vp_trade_bias_score,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("vp_policy_side", "vp_trade_bias_summary", "vp_policy_candidate", "expected"),
    [
        (
            "long",
            "continuation_up|long_continuation|high|score=3",
            1,
            "long|continuation_up|long_continuation|high|score=3|candidate=1",
        ),
        (
            "short",
            "reversion_from_above|short_reversion|medium|score=2",
            1,
            "short|reversion_from_above|short_reversion|medium|score=2|candidate=1",
        ),
        (
            "none",
            "neutral|neutral|none|score=0",
            0,
            "none|neutral|neutral|none|score=0|candidate=0",
        ),
    ],
)
def test_resolve_vp_policy_reason_representative_mappings(
    vp_policy_side,
    vp_trade_bias_summary,
    vp_policy_candidate,
    expected,
):
    assert (
        fe_mod.resolve_vp_policy_reason(
            vp_policy_side,
            vp_trade_bias_summary,
            vp_policy_candidate,
        )
        == expected
    )
