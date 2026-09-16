"""Comprehensive unit tests covering all 23 normalization and canonical landing requirements."""

from datetime import date, datetime, timezone
import json
import os
import pytest

from release_2.configs.settings import IngestionConfig
from release_2.src.normalization.normalizer import (
    detect_source_format,
    unwrap_matches,
    normalize_match,
    validate_canonical_match,
    NormalizationError,
)
from release_2.src.raw.raw_storage import RawStorageManager, compute_sha256
from release_2.src.landing.control_table import IngestionControlManager
from release_2.src.landing.orchestrator import LandingOrchestrator


# 1. Current API response with data[] containing multiple matches
def test_unwrap_api_response_multiple_matches():
    api_payload = {
        "status": 200,
        "data": [
            {"metadata": {"matchid": "m1", "game_start": 1705363200}},
            {"metadata": {"matchid": "m2", "game_start": 1705363300}},
        ]
    }
    assert detect_source_format(api_payload) == "api_envelope"
    matches = unwrap_matches(api_payload)
    assert len(matches) == 2
    assert matches[0]["metadata"]["matchid"] == "m1"
    assert matches[1]["metadata"]["matchid"] == "m2"


# 2 & 3. Historical wrapper: hasWon -> matchData -> status -> data
def test_unwrap_historical_wrapper():
    hist_payload = {
        "hasWon": False,
        "matchData": {
            "status": 200,
            "data": {
                "metadata": {"matchid": "hist-123", "game_start": 1705363200},
                "players": {},
                "teams": {},
                "rounds": []
            },
            "errors": None
        }
    }
    assert detect_source_format(hist_payload) == "historical_wrapper"
    matches = unwrap_matches(hist_payload)
    assert len(matches) == 1
    assert matches[0]["metadata"]["matchid"] == "hist-123"

    norm = normalize_match(matches[0])
    assert "hasWon" not in norm
    assert "matchData" not in norm
    assert "status" not in norm
    assert norm["metadata"]["matchid"] == "hist-123"


# 4. behaviour -> behavior
def test_normalize_behaviour_to_behavior():
    raw = {
        "metadata": {"matchid": "m1", "game_start": 1705363200},
        "players": {
            "all_players": [
                {"puuid": "p1", "behaviour": {"afk_rounds": 0.0}}
            ]
        }
    }
    norm = normalize_match(raw)
    p0 = norm["players"]["all_players"][0]
    assert "behavior" in p0
    assert "behaviour" not in p0
    assert p0["behavior"]["afk_rounds"] == 0.0


# 5, 6, 7, 8. Ability cast field names: c_cast, q_cast, e_cast, x_cast -> c_casts, q_casts, e_casts, x_casts
def test_normalize_ability_casts():
    raw = {
        "metadata": {"matchid": "m1", "game_start": 1705363200},
        "players": {
            "all_players": [
                {
                    "puuid": "p1",
                    "ability_casts": {
                        "c_cast": 1,
                        "q_cast": 2,
                        "e_cast": 3,
                        "x_cast": 4
                    }
                }
            ]
        }
    }
    norm = normalize_match(raw)
    casts = norm["players"]["all_players"][0]["ability_casts"]
    assert casts == {"c_casts": 1, "q_casts": 2, "e_casts": 3, "x_casts": 4}
    for old_key in ("c_cast", "q_cast", "e_cast", "x_cast"):
        assert old_key not in casts


# 9. roaster -> roster
def test_normalize_roaster_to_roster():
    raw = {
        "metadata": {"matchid": "m1", "game_start": 1705363200},
        "teams": {
            "red": {"roaster": None, "has_won": False},
            "blue": {"roaster": ["p1", "p2"], "has_won": True}
        }
    }
    norm = normalize_match(raw)
    red = norm["teams"]["red"]
    blue = norm["teams"]["blue"]
    assert "roster" in red and "roaster" not in red
    assert red["roster"] is None
    assert "roster" in blue and "roaster" not in blue
    assert blue["roster"] == ["p1", "p2"]


# 10. Historical nested kill_events -> top-level kills[]
def test_historical_nested_kill_events_flattened_with_derived_round():
    raw = {
        "metadata": {"matchid": "m1", "game_start": 1705363200},
        "rounds": [
            {
                "round_num": 1,
                "player_stats": [
                    {
                        "player_puuid": "p1",
                        "kill_events": [
                            {
                                "kill_time_in_match": 1000,
                                "killer_puuid": "p1",
                                "victim_puuid": "p2"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    norm = normalize_match(raw)
    assert "kills" in norm
    assert len(norm["kills"]) == 1
    k = norm["kills"][0]
    assert k["killer_puuid"] == "p1"
    assert k["victim_puuid"] == "p2"
    # Round derived from parent index (0-based)
    assert k["round"] == 0


# 11. Current top-level kills[] preserved
def test_current_top_level_kills_preserved():
    existing_kills = [
        {"kill_time_in_match": 500, "round": 0, "killer_puuid": "p1", "victim_puuid": "p2"}
    ]
    raw = {
        "metadata": {"matchid": "m1", "game_start": 1705363200},
        "kills": existing_kills
    }
    norm = normalize_match(raw)
    assert norm["kills"] == existing_kills


# 12. No duplicate kill events
def test_no_duplicate_kill_events_in_flattening():
    identical_kill = {
        "kill_time_in_match": 1000,
        "kill_time_in_round": 200,
        "killer_puuid": "p1",
        "victim_puuid": "p2"
    }
    raw = {
        "metadata": {"matchid": "m1", "game_start": 1705363200},
        "rounds": [
            {
                "player_stats": [
                    {"kill_events": [identical_kill]},
                    {"kill_events": [identical_kill]}  # Duplicate in same round
                ]
            }
        ]
    }
    norm = normalize_match(raw)
    assert len(norm["kills"]) == 1
    is_valid, errors = validate_canonical_match(norm)
    assert is_valid
    assert not errors


# 13, 14, 15. is_available handling
def test_is_available_preserved_when_present():
    raw_true = {"metadata": {"matchid": "m1", "game_start": 1705363200}, "is_available": True}
    raw_false = {"metadata": {"matchid": "m2", "game_start": 1705363200}, "is_available": False}
    assert normalize_match(raw_true)["is_available"] is True
    assert normalize_match(raw_false)["is_available"] is False


def test_missing_is_available_becomes_null_without_inference():
    raw_hist = {
        "status": 200,  # HTTP 200 should NOT infer is_available = True
        "metadata": {"matchid": "m1", "game_start": 1705363200}
    }
    norm = normalize_match(raw_hist)
    assert norm["is_available"] is None


# 16, 17, 18. match_id and Landing date & filename
def test_match_id_and_landing_date_path(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    match_dt = date(2026, 9, 14)
    landing_path = config.build_match_landing_path("ab4a85fb-8f9e-4298-b22e-b2a3648cb58b", match_dt)
    expected_suffix = "matches/2026/09/14/ab4a85fb-8f9e-4298-b22e-b2a3648cb58b.json"
    assert landing_path.replace("\\", "/").endswith(expected_suffix)


# 19. Same source can be processed repeatedly without duplicate Landing records (Idempotency)
def test_pipeline_idempotency(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    control_mgr = IngestionControlManager(config=config, spark=None)
    orchestrator = LandingOrchestrator(config=config, control_manager=control_mgr)

    raw_payload = {
        "status": 200,
        "data": [
            {
                "is_available": True,
                "metadata": {"matchid": "match-repeat-test", "game_start": 1789579869},
                "players": {},
                "teams": {},
                "rounds": []
            }
        ]
    }

    # Run 1: Ingests match
    res1 = orchestrator.process_raw_payload(raw_payload)
    assert res1.ingested == 1
    assert res1.skipped == 0

    # Run 2: Exact same payload
    res2 = orchestrator.process_raw_payload(raw_payload)
    assert res2.ingested == 0
    assert res2.skipped == 1


# 20 & 21. Malformed JSON and missing match ID rejection
def test_missing_match_id_rejected():
    raw_no_id = {"metadata": {"game_start": 1705363200}}
    with pytest.raises(NormalizationError, match="missing a valid 'matchid'"):
        normalize_match(raw_no_id)


def test_malformed_json_unwrapping():
    with pytest.raises(NormalizationError):
        unwrap_matches("not a dict")


# 22. Normalization is strictly idempotent: normalize(normalize(x)) == normalize(x)
def test_normalization_idempotent():
    raw = {
        "is_available": True,
        "metadata": {"matchid": "m1", "game_start": 1705363200},
        "players": {
            "all_players": [
                {
                    "puuid": "p1",
                    "behaviour": {"afk": 0},
                    "ability_casts": {"c_cast": 5}
                }
            ]
        },
        "teams": {"red": {"roaster": None}},
        "rounds": [
            {
                "player_stats": [
                    {"kill_events": [{"kill_time_in_match": 10, "killer_puuid": "p1"}]}
                ]
            }
        ]
    }

    pass1 = normalize_match(raw)
    pass2 = normalize_match(pass1)
    assert pass1 == pass2


# 23. Raw source JSON is not modified by the raw ingestion process
def test_raw_source_bytes_preservation(tmp_path):
    config = IngestionConfig(base_volume_path_override=str(tmp_path))
    raw_storage = RawStorageManager(config)

    original_bytes = b'{\n  "hasWon": false,\n  "matchData": { "status": 200 }\n}'
    target_path = raw_storage.get_historical_target_path("raw_matches/test.json")
    final_path, sha_hash, is_new = raw_storage.write_raw_file(target_path, original_bytes)

    assert is_new is True
    assert sha_hash == compute_sha256(original_bytes)

    with open(final_path, "rb") as read_f:
        read_bytes = read_f.read()
    assert read_bytes == original_bytes
