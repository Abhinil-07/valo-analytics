"""Unit tests for HenrikValApiClient."""

import pytest
from unittest.mock import MagicMock, patch
import requests
from src.landing.api_client import (
    HenrikValApiClient,
    AuthenticationError,
    RateLimitExceededError,
    ResourceNotFoundError,
    ApiClientError
)


def test_api_client_init_requires_key():
    with pytest.raises(ValueError, match="API key must be provided"):
        HenrikValApiClient(api_key="")


def test_api_client_headers():
    client = HenrikValApiClient(api_key="test_api_key_123")
    assert client.session.headers.get("Authorization") == "test_api_key_123"
    assert client.session.headers.get("Accept") == "application/json"


@patch("requests.Session.request")
def test_api_client_fetch_matches_success(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": 200, "data": []}
    mock_request.return_value = mock_response

    client = HenrikValApiClient(api_key="valid_key")
    result = client.fetch_matches_by_player(region="na", name="TenZ", tag="0001", queue="competitive")

    assert result == {"status": 200, "data": []}
    mock_request.assert_called_once()
    args, kwargs = mock_request.call_args
    assert "https://api.henrikdev.xyz/valorant/v3/matches/na/TenZ/0001" in kwargs["url"]
    assert kwargs["params"] == {"filter": "competitive"}


@patch("requests.Session.request")
def test_api_client_auth_error(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_request.return_value = mock_response

    client = HenrikValApiClient(api_key="invalid_key", max_retries=0)
    with pytest.raises(AuthenticationError):
        client.fetch_matches_by_player(region="na", name="TenZ", tag="0001")


@patch("requests.Session.request")
def test_api_client_not_found(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_request.return_value = mock_response

    client = HenrikValApiClient(api_key="valid_key", max_retries=0)
    with pytest.raises(ResourceNotFoundError):
        client.fetch_match_by_id("non-existent-id")


@patch("time.sleep", return_value=None)
@patch("requests.Session.request")
def test_api_client_rate_limit_retry(mock_request, mock_sleep):
    mock_429 = MagicMock()
    mock_429.status_code = 429
    mock_429.headers = {"Retry-After": "1"}

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = {"status": 200, "data": [{"id": "m1"}]}

    mock_request.side_effect = [mock_429, mock_200]

    client = HenrikValApiClient(api_key="valid_key", max_retries=2)
    res = client.fetch_match_by_id("m1")

    assert res["status"] == 200
    assert mock_request.call_count == 2
    mock_sleep.assert_called_once_with(1)
