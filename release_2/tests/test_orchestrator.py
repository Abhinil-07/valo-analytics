"""Unit and integration tests for LandingOrchestrator."""

import json
import os
import pytest
from configs.settings import IngestionConfig
from src.landing.control_table import IngestionControlManager
from src.landing.orchestrator import LandingOrchestrator
from src.landing.validator import MatchValidator
from src.landing.volume_writer import VolumeWriter


@pytest.fixture
def sample_api_response():
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "sample_matches.json")
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)


def test_orchestrator_initial_ingestion_and_idempotency(tmp_path, sample_api_response):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    validator = MatchValidator()
    volume_writer = VolumeWriter(config)
    control_manager = IngestionControlManager(config=config, spark=None)

    orchestrator = LandingOrchestrator(
        config=config,
        validator=validator,
        volume_writer=volume_writer,
        control_manager=control_manager
    )

    # --- Run 1: Initial Ingestion ---
    summary1 = orchestrator.process_api_response(sample_api_response)

    assert summary1.total_received == 2
    assert summary1.total_valid == 2
    assert summary1.ingested == 2
    assert summary1.skipped == 0
    assert summary1.failed == 0
    assert "ab4a85fb-8f9e-4298-b22e-b2a3648cb58b" in summary1.successful_match_ids
    assert "1391571f-c2bc-4589-9dac-01b02b3563bc" in summary1.successful_match_ids

    # Verify both files are written on the volume
    match1_path = os.path.join(str(tmp_path), "matches", "2024", "01", "16", "ab4a85fb-8f9e-4298-b22e-b2a3648cb58b.json")
    match2_path = os.path.join(str(tmp_path), "matches", "2024", "01", "17", "1391571f-c2bc-4589-9dac-01b02b3563bc.json")
    assert os.path.exists(match1_path)
    assert os.path.exists(match2_path)

    # --- Run 2: Exact Same Ingestion (Idempotency check) ---
    summary2 = orchestrator.process_api_response(sample_api_response)

    assert summary2.total_received == 2
    assert summary2.total_valid == 2
    assert summary2.ingested == 0
    assert summary2.skipped == 2
    assert summary2.failed == 0
    assert len(summary2.skipped_match_ids) == 2


def test_orchestrator_handles_malformed_matches(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    validator = MatchValidator()
    volume_writer = VolumeWriter(config)
    control_manager = IngestionControlManager(config=config, spark=None)

    orchestrator = LandingOrchestrator(
        config=config,
        validator=validator,
        volume_writer=volume_writer,
        control_manager=control_manager
    )

    bad_payload = {
        "status": 200,
        "data": [
            {"metadata": {"matchid": "good-match-1", "game_start": 1705363200}},
            {"metadata": {"missing_matchid": "bad"}}  # Malformed: no matchid
        ]
    }

    summary = orchestrator.process_api_response(bad_payload)
    assert summary.total_received == 2
    assert summary.total_valid == 1
    assert summary.ingested == 1
    assert summary.failed == 1
    assert len(summary.errors) == 1


def test_orchestrator_allows_retry_on_previous_failure(tmp_path, sample_api_response):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    validator = MatchValidator()
    volume_writer = VolumeWriter(config)
    control_manager = IngestionControlManager(config=config, spark=None)

    # Simulate match 1 having previously failed in control table
    failed_match_id = "ab4a85fb-8f9e-4298-b22e-b2a3648cb58b"
    control_manager.record_status(
        match_id=failed_match_id,
        match_date=None,
        landing_path=None,
        status="FAILED",
        error_message="Previous transient failure"
    )

    orchestrator = LandingOrchestrator(
        config=config,
        validator=validator,
        volume_writer=volume_writer,
        control_manager=control_manager
    )

    summary = orchestrator.process_api_response(sample_api_response)
    # The previously failed match should be retried and successfully ingested
    assert failed_match_id in summary.successful_match_ids
    assert summary.ingested == 2
