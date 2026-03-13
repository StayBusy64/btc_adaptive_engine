from backend.decay_engine import apply_time_decay


def test_apply_time_decay():
    result = apply_time_decay(1.0, 0.1, 1.0)
    assert result < 1.0
