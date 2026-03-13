from backend.tradingview_bridge_manifest import (
    classify_research_fields,
    expected_pine_defaults_fragment_map,
    load_bridge_manifest,
    resolve_release_context,
)


def test_resolve_release_context_defaults_to_current_release_for_current_strategy():
    manifest = load_bridge_manifest()

    resolved = resolve_release_context(
        payload={},
        event={"strategy_id": manifest.current_release.strategy_id},
    )

    assert resolved["strategy_id"] == manifest.current_release.strategy_id
    assert resolved["release_id"] == manifest.current_release.release_id
    assert resolved["release_version"] == manifest.current_release.release_version
    assert resolved["release_channel"] == manifest.current_release.release_channel
    assert resolved["contract_version"] == manifest.current_release.contract_version
    assert resolved["telemetry_schema_version"] == manifest.current_release.telemetry_schema_version


def test_classify_research_fields_splits_registered_and_unknown_metrics():
    recognized, unknown = classify_research_fields(
        {
            "signal_quality_score": 0.81,
            "continuation_confidence": 0.74,
            "custom_probe": 0.19,
        }
    )

    assert recognized == {
        "signal_quality_score": 0.81,
        "continuation_confidence": 0.74,
    }
    assert unknown == {"custom_probe": 0.19}


def test_expected_pine_defaults_fragment_map_includes_release_inputs():
    fragments = expected_pine_defaults_fragment_map()

    assert "release id input" in fragments
    assert 'Release ID' in fragments["release id input"]
    assert "release version input" in fragments
    assert "contract version input" in fragments