from backend.regime_engine import classify_regime


def test_classify_regime():
    result = classify_regime(
        {
            "trend_alignment_score": 1.0,
            "trend_strength_pct": 2.1,
            "trend_slope_21": 0.4,
            "range_expansion_ratio": 1.35,
            "volatility_zscore_20": 1.1,
            "momentum_10_pct": 1.0,
        }
    )

    assert result["regime_id"] == "trend_expansion"
    assert 0.0 <= float(result["regime_confidence"]) <= 0.99
    assert 0.0 <= float(result["transition_risk"]) <= 1.0
