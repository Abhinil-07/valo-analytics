"""Unit tests for IngestionControlManager."""

from datetime import date
import pytest
from configs.settings import IngestionConfig
from src.landing.control_table import IngestionControlManager


def test_control_table_record_and_get_successful():
    config = IngestionConfig()
    manager = IngestionControlManager(config=config, spark=None)

    # Initially empty
    assert manager.get_successful_match_ids(["m1", "m2"]) == set()

    # Record m1 as SUCCESS, m2 as FAILED
    manager.record_status(
        match_id="m1",
        match_date=date(2024, 1, 16),
        landing_path="/Volumes/main/valorant/landing/matches/2024/01/16/m1.json",
        status="SUCCESS"
    )
    manager.record_status(
        match_id="m2",
        match_date=date(2024, 1, 16),
        landing_path=None,
        status="FAILED",
        error_message="Network error"
    )

    success_ids = manager.get_successful_match_ids(["m1", "m2", "m3"])
    assert success_ids == {"m1"}


def test_control_table_reconciliation_missing_file():
    config = IngestionConfig()
    manager = IngestionControlManager(config=config, spark=None)

    manager.record_status(
        match_id="m1",
        match_date=date(2024, 1, 16),
        landing_path="/non/existent/path/m1.json",
        status="SUCCESS"
    )

    # File exists checker returns False
    def fake_file_exists(path: str) -> bool:
        return False

    success_ids = manager.get_successful_match_ids(["m1"], verify_file_exists_fn=fake_file_exists)
    # Because file does not exist, reconciliation removes it from successful set
    assert success_ids == set()
