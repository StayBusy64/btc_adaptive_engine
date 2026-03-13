"""Tests for backend.path_config – canonical path/endpoint/env-var constants."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from backend.path_config import (
    DATA_DIR,
    ENV_INGEST_SIGNAL_KEY,
    ENV_PUBLIC_BASE_URL,
    ENV_SIGNAL_WEBHOOK_KEY,
    ENV_TV_SIGNAL_KEY,
    ENV_WEBHOOK_HOST,
    ENV_WEBHOOK_PORT,
    LOGS_DIR,
    REPO_ROOT,
    STATE_DIR,
    TV_INGEST_DIR,
    TV_INGEST_PENDING_DIR,
    TV_INGEST_PROCESSED_DIR,
    WEBHOOK_BATCH_PATH,
    WEBHOOK_PHASE1_PATH,
)


class TestRepoRoot:
    def test_repo_root_is_absolute(self) -> None:
        assert REPO_ROOT.is_absolute()

    def test_repo_root_exists(self) -> None:
        assert REPO_ROOT.exists()

    def test_repo_root_contains_backend(self) -> None:
        assert (REPO_ROOT / "backend").is_dir()

    def test_repo_root_contains_tests(self) -> None:
        assert (REPO_ROOT / "tests").is_dir()

    def test_path_config_is_inside_backend(self) -> None:
        path_config_file = REPO_ROOT / "backend" / "path_config.py"
        assert path_config_file.exists()


class TestWebhookPaths:
    def test_batch_path_canonical(self) -> None:
        assert WEBHOOK_BATCH_PATH == "/webhooks/tradingview/batch"

    def test_phase1_path_canonical(self) -> None:
        assert WEBHOOK_PHASE1_PATH == "/webhooks/tradingview"

    def test_batch_path_starts_with_slash(self) -> None:
        assert WEBHOOK_BATCH_PATH.startswith("/")

    def test_batch_path_does_not_end_with_slash(self) -> None:
        assert not WEBHOOK_BATCH_PATH.endswith("/")

    def test_batch_path_differs_from_phase1_path(self) -> None:
        assert WEBHOOK_BATCH_PATH != WEBHOOK_PHASE1_PATH

    def test_batch_path_extends_phase1_path(self) -> None:
        assert WEBHOOK_BATCH_PATH.startswith(WEBHOOK_PHASE1_PATH)


class TestDataDirectories:
    def test_data_dir_under_repo_root(self) -> None:
        assert DATA_DIR == REPO_ROOT / "data"

    def test_tv_ingest_dir_under_data(self) -> None:
        assert TV_INGEST_DIR == DATA_DIR / "tv_ingest"

    def test_pending_dir_under_tv_ingest(self) -> None:
        assert TV_INGEST_PENDING_DIR == TV_INGEST_DIR / "pending"

    def test_processed_dir_under_tv_ingest(self) -> None:
        assert TV_INGEST_PROCESSED_DIR == TV_INGEST_DIR / "processed"

    def test_state_dir_under_data(self) -> None:
        assert STATE_DIR == DATA_DIR / "state"

    def test_logs_dir_under_data(self) -> None:
        assert LOGS_DIR == DATA_DIR / "logs"

    def test_all_dirs_are_path_objects(self) -> None:
        for d in (
            DATA_DIR,
            TV_INGEST_DIR,
            TV_INGEST_PENDING_DIR,
            TV_INGEST_PROCESSED_DIR,
            STATE_DIR,
            LOGS_DIR,
        ):
            assert isinstance(d, Path)

    def test_all_dirs_are_under_repo_root(self) -> None:
        for d in (
            DATA_DIR,
            TV_INGEST_DIR,
            TV_INGEST_PENDING_DIR,
            TV_INGEST_PROCESSED_DIR,
            STATE_DIR,
            LOGS_DIR,
        ):
            assert str(d).startswith(str(REPO_ROOT))


class TestEnvVarNames:
    def test_ingest_signal_key_name(self) -> None:
        assert ENV_INGEST_SIGNAL_KEY == "TRADINGVIEW_INGEST_SIGNAL_KEY"

    def test_signal_webhook_key_name(self) -> None:
        assert ENV_SIGNAL_WEBHOOK_KEY == "SIGNAL_WEBHOOK_KEY"

    def test_tv_signal_key_name(self) -> None:
        assert ENV_TV_SIGNAL_KEY == "TV_SIGNAL_KEY"

    def test_public_base_url_name(self) -> None:
        assert ENV_PUBLIC_BASE_URL == "TRADINGVIEW_PUBLIC_BASE_URL"

    def test_webhook_host_name(self) -> None:
        assert ENV_WEBHOOK_HOST == "TRADINGVIEW_WEBHOOK_HOST"

    def test_webhook_port_name(self) -> None:
        assert ENV_WEBHOOK_PORT == "TRADINGVIEW_WEBHOOK_PORT"

    def test_all_env_var_names_are_nonempty_strings(self) -> None:
        for name in (
            ENV_INGEST_SIGNAL_KEY,
            ENV_SIGNAL_WEBHOOK_KEY,
            ENV_TV_SIGNAL_KEY,
            ENV_PUBLIC_BASE_URL,
            ENV_WEBHOOK_HOST,
            ENV_WEBHOOK_PORT,
        ):
            assert isinstance(name, str) and name


class TestWebhookSetupImportsPathConfig:
    """Verify tradingview_webhook_setup re-uses path_config constants."""

    def test_endpoint_path_matches_canonical(self) -> None:
        from backend.tradingview_webhook_setup import ENDPOINT_PATH

        assert ENDPOINT_PATH == WEBHOOK_BATCH_PATH

    def test_project_root_matches_repo_root(self) -> None:
        from backend.tradingview_webhook_setup import PROJECT_ROOT

        assert PROJECT_ROOT == REPO_ROOT
