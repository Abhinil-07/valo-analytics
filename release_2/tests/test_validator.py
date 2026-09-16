"""Unit tests for MatchValidator."""

from datetime import date
import json
import pytest
from src.landing.validator import MatchValidator, ValidationError


def test_validate_api_response_valid():
    payload = {"status": 200, "data": [{"metadata": {"matchid": "m1"}}]}
    matches = MatchValidator.validate_api_response(payload)
    assert len(matches) == 1


def test_validate_api_response_invalid_status():
    payload = {"status": 500, "message": "Internal Server Error"}
    with pytest.raises(ValidationError, match="status is not 200"):
        MatchValidator.validate_api_response(payload)


def test_validate_api_response_missing_data():
    payload = {"status": 200}
    with pytest.raises(ValidationError, match="missing 'data' field"):
        MatchValidator.validate_api_response(payload)


def test_validate_api_response_non_list_data():
    payload = {"status": 200, "data": "not-a-list"}
    with pytest.raises(ValidationError, match="'data' field must be a list"):
        MatchValidator.validate_api_response(payload)


def test_validate_match_success_epoch():
    raw_match = {
        "metadata": {
            "matchid": "ab4a85fb-8f9e-4298-b22e-b2a3648cb58b",
            "game_start": 1705363200,
            "map": "Ascent"
        },
        "players": {},
        "teams": {},
        "rounds": []
    }
    validated = MatchValidator.validate_match(raw_match)
    assert validated.match_id == "ab4a85fb-8f9e-4298-b22e-b2a3648cb58b"
    assert validated.match_date == date(2024, 1, 16)
    assert validated.raw_payload == raw_match


def test_validate_match_success_patched_string():
    raw_match = {
        "metadata": {
            "matchid": "1391571f-c2bc-4589-9dac-01b02b3563bc",
            "game_start_patched": "Wednesday, January 17, 2024 12:00 AM",
            "map": "Bind"
        },
        "players": {},
        "teams": {},
        "rounds": []
    }
    validated = MatchValidator.validate_match(raw_match)
    assert validated.match_id == "1391571f-c2bc-4589-9dac-01b02b3563bc"
    assert validated.match_date == date(2024, 1, 17)


def test_validate_match_missing_matchid():
    raw_match = {
        "metadata": {
            "game_start": 1705363200
        }
    }
    with pytest.raises(ValidationError, match="missing valid 'matchid'"):
        MatchValidator.validate_match(raw_match)


def test_validate_match_missing_date():
    raw_match = {
        "metadata": {
            "matchid": "some-id"
        }
    }
    with pytest.raises(ValidationError, match="Could not derive valid match date"):
        MatchValidator.validate_match(raw_match)
