"""Integration tests for Raw loaders, normalization, and Landing pipeline."""

import json
import os
from datetime import date
from unittest.mock import MagicMock, patch
import pytest

from release_2.configs.settings import IngestionConfig
from release_2.src.raw.raw_storage import RawStorageManager
from release_2.src.raw.github_loader import GitHubHistoricalLoader
from release_2.src.raw.api_loader import ApiRawLoader
from release_2.src.landing.control_table import IngestionControlManager
from release_2.src.landing.orchestrator import LandingOrchestrator


def test_github_loader_ingest_file(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    loader = GitHubHistoricalLoader(config=config)

    sample_historical_json = json.dumps({
        "hasWon": True,
        "matchData": {
            "status": 200,
            "data": {
                "metadata": {"matchid": "gh-match-001", "game_start": 1789579869},
                "players": {"all_players": []},
                "teams": {"red": {"roaster": None}, "blue": {"roaster": None}},
                "rounds": []
            }
        }
    }).encode("utf-8")

    rel_path = "raw_matches/017c1d91-f7b8-4f2e-b3f6-8daa13a360e8.json"
    target_path, sha256_hash, is_new = loader.ingest_file(rel_path, raw_bytes=sample_historical_json)

    assert is_new is True
    assert os.path.exists(target_path)
    # Check that relative path was preserved under historical/
    assert "raw/historical/raw_matches/017c1d91-f7b8-4f2e-b3f6-8daa13a360e8.json" in target_path.replace("\\", "/")

    # Re-ingest same file -> should be skipped (idempotent)
    target_path2, sha256_hash2, is_new2 = loader.ingest_file(rel_path, raw_bytes=sample_historical_json)
    assert is_new2 is False
    assert sha256_hash == sha256_hash2


def test_api_raw_loader_archives_unaltered_response(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    api_loader = ApiRawLoader(config=config)

    full_api_response = {
        "status": 200,
        "data": [
            {
                "is_available": True,
                "metadata": {"matchid": "api-match-001", "game_start": 1789578342},
                "players": {},
                "teams": {},
                "rounds": []
            }
        ]
    }

    result = api_loader.archive_raw_payload(full_api_response, request_label="test_run")
    assert os.path.exists(result.raw_path)
    assert "raw/api/" in result.raw_path.replace("\\", "/")
    assert result.match_count == 1

    # Verify content in raw is preserved exactly
    with open(result.raw_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded == full_api_response


def test_end_to_end_source_to_raw_to_landing(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    raw_storage = RawStorageManager(config)
    control_mgr = IngestionControlManager(config=config, spark=None)
    orchestrator = LandingOrchestrator(
        config=config,
        control_manager=control_mgr,
        raw_storage=raw_storage
    )

    # 1. Historical payload
    hist_raw = {
        "hasWon": False,
        "matchData": {
            "status": 200,
            "data": {
                "metadata": {
                    "matchid": "hist-e2e-001",
                    "game_start": 1781628688
                },
                "players": {
                    "all_players": [
                        {
                            "puuid": "p-hist-1",
                            "behaviour": {"afk_rounds": 0.0},
                            "ability_casts": {"c_cast": 3}
                        }
                    ]
                },
                "teams": {"red": {"roaster": None}},
                "rounds": []
            }
        }
    }

    # Save to Raw
    hist_bytes = json.dumps(hist_raw).encode("utf-8")
    raw_hist_path = raw_storage.get_historical_target_path("raw_matches/hist-e2e-001.json")
    raw_storage.write_raw_file(raw_hist_path, hist_bytes)

    # Normalize and Land
    hist_summary = orchestrator.process_raw_payload(
        raw_payload=hist_raw,
        source="github",
        load_type="historical",
        source_file=raw_hist_path
    )

    assert hist_summary.ingested == 1
    assert "hist-e2e-001" in hist_summary.successful_match_ids

    # Verify Landing file exists and is normalized
    landing_file = config.build_match_landing_path(
        match_id="hist-e2e-001",
        match_date=date(2026, 6, 15)  # 1781628688 is in June 2026
    )
    # Check that a landing file exists in landing/matches/
    landing_matches_dir = config.landing_matches_root
    found_files = []
    for root, _, files in os.walk(landing_matches_dir):
        for f in files:
            if f == "hist-e2e-001.json":
                found_files.append(os.path.join(root, f))
    assert len(found_files) == 1

    with open(found_files[0], "r", encoding="utf-8") as f:
        canonical_m = json.load(f)

    # Assert Canonical Contract
    assert "hasWon" not in canonical_m
    assert "matchData" not in canonical_m
    assert canonical_m["is_available"] is None
    assert "kills" in canonical_m
    p0 = canonical_m["players"]["all_players"][0]
    assert "behavior" in p0
    assert "behaviour" not in p0
    assert "c_casts" in p0["ability_casts"]
    assert "c_cast" not in p0["ability_casts"]
