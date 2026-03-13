from backend.label_engine import label_outcome


def test_label_outcome():
    result = label_outcome({})
    assert "chop_state" in result
