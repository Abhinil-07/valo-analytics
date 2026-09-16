"""HenrikDev Valorant API client with retry and rate-limiting support."""

import time
from typing import Any, Dict, Optional
import requests
from requests.exceptions import RequestException

from release_2.src.common.logger import get_logger, mask_secret

logger = get_logger(__name__)


class ApiClientError(Exception):
    """Base exception for API client errors."""
    pass


class AuthenticationError(ApiClientError):
    """Raised when API key is missing or invalid (HTTP 401/403)."""
    pass


class RateLimitExceededError(ApiClientError):
    """Raised when API rate limit is reached (HTTP 429)."""
    pass


class ResourceNotFoundError(ApiClientError):
    """Raised when requested resource does not exist (HTTP 404)."""
    pass


class HenrikValApiClient:
    """Client for fetching Valorant match data from HenrikDev API."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.henrikdev.xyz",
        api_version: str = "v3",
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 1.5,
    ):
        """Initializes HenrikValApiClient.
        
        Args:
            api_key: HenrikDev Valorant API key.
            base_url: Base URL of HenrikDev API.
            api_version: API version string (e.g. 'v3').
            timeout: Timeout in seconds for HTTP requests.
            max_retries: Maximum number of retries on transient errors.
            backoff_factor: Multiplier for exponential backoff sleep.
        """
        if not api_key or not api_key.strip():
            raise ValueError("API key must be provided and non-empty")

        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version.strip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": self.api_key,
            "Accept": "application/json",
            "User-Agent": "Valorant-Analytics-ETL/1.0"
        })

    def fetch_matches_by_player(
        self,
        region: str,
        name: str,
        tag: str,
        queue: Optional[str] = None,
        mode: Optional[str] = None,
        size: Optional[int] = None
    ) -> Dict[str, Any]:
        """Fetches match list for a specific player from HenrikDev API.
        
        Endpoint: /valorant/{api_version}/matches/{region}/{name}/{tag}
        
        Args:
            region: Player region (e.g. 'na', 'eu', 'ap', 'kr').
            name: In-game player name.
            tag: Player tagline.
            queue: Match queue filter (e.g. 'competitive', 'unrated').
            mode: Game mode filter (e.g. 'bomb').
            size: Number of matches to fetch (max 10-20 depending on HenrikDev tier).
            
        Returns:
            JSON response payload dictionary.
        """
        endpoint = f"{self.base_url}/valorant/{self.api_version}/matches/{region}/{name}/{tag}"
        params: Dict[str, Any] = {}
        if queue:
            params["filter"] = queue
        if mode:
            params["mode"] = mode
        if size:
            params["size"] = size

        return self._make_request("GET", endpoint, params=params)

    def fetch_match_by_id(self, match_id: str) -> Dict[str, Any]:
        """Fetches detailed single match data by match ID.
        
        Endpoint: /valorant/{api_version}/match/{match_id}
        """
        endpoint = f"{self.base_url}/valorant/{self.api_version}/match/{match_id}"
        return self._make_request("GET", endpoint)

    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Executes HTTP request with retry logic and error handling."""
        logger.info("Calling HenrikDev API endpoint: %s with params: %s", url, params)

        attempts = 0
        last_exception: Optional[Exception] = None

        while attempts <= self.max_retries:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.timeout
                )

                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError as json_err:
                        raise ApiClientError(f"Malformed JSON response from API: {json_err}") from json_err

                elif response.status_code in (401, 403):
                    raise AuthenticationError(
                        f"Authentication failed (HTTP {response.status_code}). Check HenrikDev API key."
                    )

                elif response.status_code == 404:
                    raise ResourceNotFoundError(
                        f"Requested resource not found at {url} (HTTP 404)."
                    )

                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2 ** attempts))
                    logger.warning("Rate limit hit (HTTP 429). Backing off for %s seconds.", retry_after)
                    if attempts == self.max_retries:
                        raise RateLimitExceededError(f"Rate limit exceeded after {self.max_retries} retries.")
                    time.sleep(retry_after)

                elif response.status_code >= 500:
                    logger.warning("API server error (HTTP %s). Attempt %s/%s.", response.status_code, attempts + 1, self.max_retries)
                    if attempts == self.max_retries:
                        raise ApiClientError(f"Server error HTTP {response.status_code}: {response.text}")
                    sleep_time = self.backoff_factor * (2 ** attempts)
                    time.sleep(sleep_time)

                else:
                    raise ApiClientError(f"Unexpected HTTP status {response.status_code}: {response.text}")

            except (requests.Timeout, requests.ConnectionError) as req_err:
                logger.warning("Network/Timeout error: %s. Attempt %s/%s.", req_err, attempts + 1, self.max_retries)
                last_exception = req_err
                if attempts == self.max_retries:
                    raise ApiClientError(f"Network error after {self.max_retries} retries: {req_err}") from req_err
                sleep_time = self.backoff_factor * (2 ** attempts)
                time.sleep(sleep_time)

            attempts += 1

        raise ApiClientError(f"Failed to execute request after {self.max_retries} retries: {last_exception}")
