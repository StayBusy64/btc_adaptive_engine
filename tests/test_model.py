import pytest

from backend.model_engine import score_state


def test_score_state():
    result = score_state(
        {
            "trend_alignment_score": 1.0,
            "trend_strength_pct": 2.4,
            "macd_hist": 0.22,
            "rsi_14": 61.0,
            "momentum_10_pct": 1.4,
            "range_expansion_ratio": 1.25,
            "atr_pct_14": 0.9,
            "atr_14": 38.0,
            "volatility_state_score": 0.1,
        }
    )

    assert result["long_probability"] > 0.0
    assert result["short_probability"] > 0.0
    assert result["no_trade_probability"] > 0.0
    assert result["long_probability"] + result["short_probability"] + result["no_trade_probability"] == pytest.approx(1.0)
    assert result["long_probability"] > result["short_probability"]
    assert result["setup_trust_score"] >= 0.0
    assert result["setup_trust_score"] <= 1.0
