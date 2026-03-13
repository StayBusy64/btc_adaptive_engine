import pytest
from pydantic import ValidationError
from backend.volume_profile_engine import VolumeProfileSnapshot

def test_volume_profile_snapshot_valid_order():
    """Test that a semantically valid profile successfully instantiates."""
    snapshot = VolumeProfileSnapshot(
        timestamp="2026-03-11T12:00:00Z",
        symbol="BTCUSDT.P",
        timeframe="5m",
        engine_version="v1.0.0",
        profile_low=50000.0,
        val=50100.0,
        poc=50200.0,
        vah=50300.0,
        profile_high=50400.0,
        shape_label="b_shape",
        balance_state="balanced",
        source_bar_count=100,
        profile_range=400.0,
        value_area_width=200.0,
        value_area_width_pct=0.5,
        poc_relative=0.5,
        poc_distance_from_mid=0.0
    )
    assert snapshot.symbol == "BTCUSDT.P"
    assert snapshot.source_bar_count == 100


def test_volume_profile_snapshot_invalid_order():
    """Test that an inverted profile ordering raises a ValueError."""
    with pytest.raises(ValidationError) as exc:
        # Cause poc to be lower than val, which is invalid
        VolumeProfileSnapshot(
            timestamp="2026-03-11T12:00:00Z",
            symbol="BTCUSDT.P",
            timeframe="5m",
            engine_version="v1.0.0",
            profile_low=50000.0,
            val=50200.0,
            poc=50100.0,  # INVALID: POC < VAL
            vah=50300.0,
            profile_high=50400.0,
            shape_label="p_shape",
            balance_state="developing",
            source_bar_count=20,
            profile_range=400.0,
            value_area_width=100.0,
            value_area_width_pct=0.25,
            poc_relative=0.25,
            poc_distance_from_mid=0.0
        )
    
    assert "Invalid profile ordering" in str(exc.value)


def test_volume_profile_snapshot_invalid_shape_or_balance():
    """Test that invalid literals are rejected."""
    with pytest.raises(ValidationError):
        VolumeProfileSnapshot(
            timestamp="2026-03-11T12:00:00Z",
            symbol="BTCUSDT.P",
            timeframe="5m",
            engine_version="v1.0.0",
            profile_low=50000.0,
            val=50100.0,
            poc=50200.0,
            vah=50300.0,
            profile_high=50400.0,
            shape_label="pizza_shape",  # INVALID
            balance_state="balanced",
            source_bar_count=20,
            profile_range=400.0,
            value_area_width=200.0,
            value_area_width_pct=0.5,
            poc_relative=0.5,
            poc_distance_from_mid=0.0
        )

from backend.volume_profile_engine import compute_volume_profile_snapshot

def test_vp_computation_balanced_distribution():
    bars = [
        {"timestamp": "t1", "high": 105, "low": 95, "volume": 10},
        {"timestamp": "t2", "high": 110, "low": 90, "volume": 50},
        {"timestamp": "t3", "high": 105, "low": 95, "volume": 10}
    ]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m")
    assert snapshot.profile_high == 110
    assert snapshot.profile_low == 90
    assert snapshot.shape_label == "d_shape"  # POC is in the middle
    assert snapshot.balance_state == "balanced"

def test_vp_computation_upper_skew_profile():
    bars = [
        {"timestamp": "t1", "high": 100, "low": 90, "volume": 10},
        {"timestamp": "t2", "high": 108, "low": 98, "volume": 20},
        {"timestamp": "t3", "high": 110, "low": 105, "volume": 100} # Heavy volume near top
    ]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m")
    assert snapshot.shape_label == "p_shape"
    assert snapshot.balance_state == "imbalanced_up"

def test_vp_computation_lower_skew_profile():
    bars = [
        {"timestamp": "t1", "high": 95, "low": 90, "volume": 100}, # Heavy volume near bottom
        {"timestamp": "t2", "high": 102, "low": 92, "volume": 20},
        {"timestamp": "t3", "high": 110, "low": 100, "volume": 10}
    ]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m")
    assert snapshot.shape_label == "b_shape"
    assert snapshot.balance_state == "imbalanced_down"

def test_vp_computation_thin_profile():
    bars = [{"timestamp": "t1", "high": 100, "low": 99, "volume": 1}]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m", bins=10)
    assert snapshot.poc == 99.5
    assert snapshot.shape_label == "d_shape" # With 1 bar its perfectly balanced

def test_vp_computation_single_band_edge_case():
    bars = [{"timestamp": "t1", "high": 100, "low": 100, "volume": 100}]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m")
    assert snapshot.profile_high == 100
    assert snapshot.profile_low == 100
    assert snapshot.poc == 100
    assert snapshot.val == 100
    assert snapshot.vah == 100

import pytest
def test_vp_computation_plateau_poc():
    # We create a structured set of bars guaranteeing equal volume over a flat top
    bars = [
        {"timestamp": "t1", "high": 100, "low": 90, "volume": 10}, # background
        {"timestamp": "t2", "high": 96, "low": 94, "volume": 100}, # core overlap
    ]
    # Within 94-96, multiple bins will have the exact same volume
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m")
    # POC should center correctly at 95.0, not stick to 94 or 96.
    assert snapshot.poc == 95.0

def test_vp_computation_value_area_tie_expansion():
    # Deterministic tie breaking: if vol_up == vol_down, engine expands symmetrically
    bars = [
        {"timestamp": "t1", "high": 110, "low": 105, "volume": 20}, # Top tail
        {"timestamp": "t2", "high": 105, "low": 95, "volume": 100}, # Heavy center
        {"timestamp": "t3", "high": 95, "low": 90, "volume": 20}, # Bottom tail (perfectly symmetrical to top)
    ]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m")
    # Total volume = 140. 68% = 95.2.
    # Bin size = (110 - 90) / 50 = 0.4.
    # Core overlapping 100 vol hits target quickly.
    # Tie breaking happens iteratively up/down from POC(100.0) outwards symmetrically.
    assert snapshot.vah == 104.8
    assert snapshot.val == 95.2

def test_vp_computation_missing_bar_fields():
    # Missing volume defaults to 1.0, missing timestamp to unknown, missing h/l skips gracefully.
    bars = [
        {"timestamp": "t1", "high": 100, "low": 90}, # No volume
        {"high": 100, "low": 90, "volume": 10}, # No timestamp
        {"volume": 50}, # No pricing at all and no timestamp
    ]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m")
    assert snapshot.timestamp == "unknown"
    assert snapshot.source_bar_count == 3
    assert snapshot.profile_high == 100

def test_vp_computation_bad_inputs():
    bars = [{"timestamp": "t1", "high": 100, "low": 90, "volume": 10}]
    
    with pytest.raises(ValueError, match="bins must be > 0"):
        compute_volume_profile_snapshot(bars, "BTC", "1m", bins=0)
    
    with pytest.raises(ValueError, match="between 0 and 1"):
        compute_volume_profile_snapshot(bars, "BTC", "1m", value_area_pct=1.5)
    
    with pytest.raises(ValueError, match="empty bars"):
        compute_volume_profile_snapshot([], "BTC", "1m")

from backend.volume_profile_engine import compute_and_store_volume_profile_snapshot
from backend.event_writer import get_recent_volume_profile_snapshots

def test_vp_end_to_end_chain(local_db):
    bars = [
        {"timestamp": "2026-03-11T12:00:00Z", "high": 105, "low": 95, "volume": 10},
        {"timestamp": "2026-03-11T12:05:00Z", "high": 110, "low": 90, "volume": 50},
        {"timestamp": "2026-03-11T12:10:00Z", "high": 105, "low": 95, "volume": 10}
    ]
    
    snapshot = compute_and_store_volume_profile_snapshot(bars, "BTCUSDT.P", "5m")
    assert snapshot.profile_high == 110
    
    # Verify persistence chain read back
    rows = get_recent_volume_profile_snapshots(symbol="BTCUSDT.P", timeframe="5m")
    assert len(rows) >= 1
    assert rows[0]["symbol"] == "BTCUSDT.P"
    assert rows[0]["timestamp"] == "2026-03-11T12:10:00Z" # Picks up last bar timestamp
    assert rows[0]["profile_high"] == 110.0

from pathlib import Path
from backend import event_writer

@pytest.fixture()
def local_db(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_vp_system.db"
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
    monkeypatch.setattr(event_writer, "DB_PATH", test_db_path)
    monkeypatch.setattr(event_writer, "SCHEMA_PATH", schema_path)
    event_writer.init_db()
    yield

def test_vp_derived_metrics():
    # Test numerical output of derived variables: profile_range, poc_relative, value_area_width, etc.
    bars = [
        {"timestamp": "t1", "high": 110, "low": 90, "volume": 100},
    ]
    # Simple balanced profile 90 to 110. Length=20.
    # POC will be 100.
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m", bins=2)
    assert snapshot.profile_high == 110
    assert snapshot.profile_low == 90
    assert snapshot.profile_range == 20.0
    
    # Mid point is 100
    # POC relative: (POC - 90) / 20 = 10 / 20 = 0.5
    assert snapshot.poc_relative == 0.5
    assert snapshot.poc_distance_from_mid == 0.0

    # Since it's symmetric and bounds are defined, let's just make sure derived properties exist and are mathematically sensible.
    assert snapshot.value_area_width > 0
    assert snapshot.value_area_width <= 20.0
    assert snapshot.value_area_width_pct == snapshot.value_area_width / 20.0

def test_vp_derived_metrics_skew():
    bars = [
        {"timestamp": "t1", "high": 110, "low": 90, "volume": 10},
        {"timestamp": "t2", "high": 110, "low": 105, "volume": 100}
    ]
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m", bins=10)
    # Range is 90 to 110 -> 20. Mid is 100.
    # POC is clearly up near 107.5
    assert snapshot.profile_range == 20.0
    assert snapshot.poc_relative > 0.5
    assert snapshot.poc_distance_from_mid > 0.0


def test_vp_price_interaction_close_at_poc():
    bars = [
        {"timestamp": "t1", "high": 110, "low": 90, "close": 100, "volume": 100},
    ]
    # Symmetrical distribution 90 to 110. POC = 100. Close is 100.
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m", bins=5)
    assert snapshot.distance_to_poc == 0.0
    assert snapshot.inside_value_area is True

def test_vp_price_interaction_close_above_vah():
    bars = [
        {"timestamp": "t1", "high": 110, "low": 90, "close": 95, "volume": 100},
        {"timestamp": "t2", "high": 110, "low": 90, "close": 109, "volume": 1},
    ]
    # Heavy volume 90 to 110. POC is near 100. VAH is likely near 105. Close 109.
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m", bins=10)
    assert snapshot.above_vah is True
    assert snapshot.inside_value_area is False

def test_vp_price_interaction_close_below_val():
    bars = [
        {"timestamp": "t1", "high": 110, "low": 90, "close": 105, "volume": 100},
        {"timestamp": "t2", "high": 110, "low": 90, "close": 91, "volume": 1},
    ]
    # Heavy volume 90 to 110. POC near 100. VAL likely near 95. Close 91.
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m", bins=10)
    assert snapshot.below_val is True
    assert snapshot.inside_value_area is False

def test_vp_price_interaction_inside_va_away_from_poc():
    bars = [
        {"timestamp": "t1", "high": 120, "low": 80, "close": 100, "volume": 100},
        {"timestamp": "t2", "high": 120, "low": 80, "close": 105, "volume": 1},
    ]
    # Base block forms POC near 100. Range is 80 to 120. VAL~90, VAH~110.
    # Close is 105. It's inside VA but distance from POC > 0
    snapshot = compute_volume_profile_snapshot(bars, "BTC", "1m", bins=20)
    assert snapshot.inside_value_area is True
    assert snapshot.distance_to_poc > 0.0
    assert snapshot.above_vah is False
    assert snapshot.below_val is False


def _bars_skewed_low(*, t1: str, t2: str) -> list[dict]:
    return [
        {"timestamp": t1, "high": 110, "low": 90, "volume": 1},
        {"timestamp": t2, "high": 95, "low": 90, "volume": 200},
    ]


def _bars_skewed_high(*, t1: str, t2: str) -> list[dict]:
    return [
        {"timestamp": t1, "high": 110, "low": 90, "volume": 1},
        {"timestamp": t2, "high": 110, "low": 105, "volume": 200},
    ]


def _bars_balanced(*, t1: str, t2: str) -> list[dict]:
    return [
        {"timestamp": t1, "high": 110, "low": 90, "volume": 50},
        {"timestamp": t2, "high": 110, "low": 90, "volume": 50},
    ]


def test_vp_poc_migration_up_with_prior_snapshot(local_db):
    first = compute_and_store_volume_profile_snapshot(
        _bars_skewed_low(t1="2026-03-11T12:00:00Z", t2="2026-03-11T12:01:00Z"),
        "BTCUSDT.P",
        "1m",
    )
    second = compute_and_store_volume_profile_snapshot(
        _bars_skewed_high(t1="2026-03-11T12:02:00Z", t2="2026-03-11T12:03:00Z"),
        "BTCUSDT.P",
        "1m",
    )

    expected_delta = second.poc - first.poc
    threshold = max(second.profile_range * 0.005, 1e-9)
    assert second.poc_migration_delta == pytest.approx(expected_delta)
    assert second.poc_migrating_up is True
    assert second.poc_migrating_down is False
    assert second.poc_migration_delta > threshold
    assert second.poc_migration_strength == pytest.approx(abs(expected_delta) / threshold)


def test_vp_poc_migration_down_with_prior_snapshot(local_db):
    first = compute_and_store_volume_profile_snapshot(
        _bars_skewed_high(t1="2026-03-11T13:00:00Z", t2="2026-03-11T13:01:00Z"),
        "BTCUSDT.P",
        "1m",
    )
    second = compute_and_store_volume_profile_snapshot(
        _bars_skewed_low(t1="2026-03-11T13:02:00Z", t2="2026-03-11T13:03:00Z"),
        "BTCUSDT.P",
        "1m",
    )

    expected_delta = second.poc - first.poc
    threshold = max(second.profile_range * 0.005, 1e-9)
    assert second.poc_migration_delta == pytest.approx(expected_delta)
    assert second.poc_migrating_up is False
    assert second.poc_migrating_down is True
    assert second.poc_migration_delta < -threshold
    assert second.poc_migration_strength == pytest.approx(abs(expected_delta) / threshold)


def test_vp_poc_migration_tiny_delta_below_threshold(local_db):
    compute_and_store_volume_profile_snapshot(
        _bars_balanced(t1="2026-03-11T14:00:00Z", t2="2026-03-11T14:01:00Z"),
        "BTCUSDT.P",
        "1m",
    )
    second = compute_and_store_volume_profile_snapshot(
        _bars_balanced(t1="2026-03-11T14:02:00Z", t2="2026-03-11T14:03:00Z"),
        "BTCUSDT.P",
        "1m",
    )

    threshold = max(second.profile_range * 0.005, 1e-9)
    assert second.poc_migration_delta == pytest.approx(0.0)
    assert abs(second.poc_migration_delta) < threshold
    assert second.poc_migrating_up is False
    assert second.poc_migrating_down is False
    assert second.poc_migration_strength == pytest.approx(0.0)


def test_vp_poc_migration_neutral_without_prior_snapshot(local_db):
    snapshot = compute_and_store_volume_profile_snapshot(
        _bars_skewed_high(t1="2026-03-11T15:00:00Z", t2="2026-03-11T15:01:00Z"),
        "BTCUSDT.P",
        "1m",
    )

    assert snapshot.poc_migration_delta == pytest.approx(0.0)
    assert snapshot.poc_migrating_up is False
    assert snapshot.poc_migrating_down is False
    assert snapshot.poc_migration_strength == pytest.approx(0.0)

