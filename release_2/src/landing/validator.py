"""Validation utilities for HenrikDev API payloads and match structures."""

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
import dateutil.parser

from release_2.src.common.logger import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """Raised when payload or match record fails validation."""
    pass


@dataclass(frozen=True)
class ValidatedMatch:
    """Represents a validated individual match object ready for landing.
    
    Attributes:
        match_id: Natural unique match identifier (from metadata.matchid).
        match_date: Date derived from metadata.game_start or game_start_patched.
        raw_payload: Original unmodified raw match dictionary.
    """
    match_id: str
    match_date: date
    raw_payload: Dict[str, Any]


class MatchValidator:
    """Validates top-level API response and individual match payloads."""

    @staticmethod
    def validate_api_response(response_payload: Any) -> List[Dict[str, Any]]:
        """Validates the top-level API response structure.
        
        Expected structure:
        {
            "status": 200,
            "data": [ { ...match... }, ... ]
        }
        
        Args:
            response_payload: Decoded JSON response from the API.
            
        Returns:
            List of raw match dictionaries under `data`.
            
        Raises:
            ValidationError: If response structure is invalid or data is missing.
        """
        if not isinstance(response_payload, dict):
            raise ValidationError(f"Expected API response to be a dict, got {type(response_payload).__name__}")

        status = response_payload.get("status")
        if status != 200:
            raise ValidationError(f"API response status is not 200 (received: {status})")

        data = response_payload.get("data")
        if data is None:
            raise ValidationError("API response is missing 'data' field")

        if not isinstance(data, list):
            raise ValidationError(f"API response 'data' field must be a list, got {type(data).__name__}")

        logger.info("API envelope valid. Found %d match object(s) in payload.", len(data))
        return data

    @classmethod
    def validate_match(cls, match_data: Any) -> ValidatedMatch:
        """Validates an individual match object and extracts match_id and match_date.
        
        Args:
            match_data: Dictionary representing a single match.
            
        Returns:
            ValidatedMatch instance.
            
        Raises:
            ValidationError: If required fields are missing or invalid.
        """
        if not isinstance(match_data, dict):
            raise ValidationError(f"Match object must be a dict, got {type(match_data).__name__}")

        metadata = match_data.get("metadata")
        if not isinstance(metadata, dict):
            raise ValidationError("Match is missing 'metadata' dictionary")

        # Extract and validate matchid
        match_id = metadata.get("matchid")
        if not match_id or not isinstance(match_id, str) or not match_id.strip():
            raise ValidationError("Match metadata is missing valid 'matchid'")
        match_id = match_id.strip()

        # Derive match date
        match_date = cls.derive_match_date(metadata)
        if not match_date:
            raise ValidationError(f"Could not derive valid match date for match {match_id}")

        # Basic integrity check for core sections
        for section in ("players", "teams", "rounds"):
            if section not in match_data:
                logger.warning("Match %s is missing '%s' section", match_id, section)

        return ValidatedMatch(
            match_id=match_id,
            match_date=match_date,
            raw_payload=match_data
        )

    @classmethod
    def derive_match_date(cls, metadata: Dict[str, Any]) -> Optional[date]:
        """Derives match date from metadata fields: game_start (epoch) or game_start_patched.
        
        Args:
            metadata: Match metadata dictionary.
            
        Returns:
            datetime.date object in UTC, or None if neither can be parsed.
        """
        # Try game_start first (standard epoch timestamp in seconds)
        game_start = metadata.get("game_start")
        if game_start is not None:
            try:
                ts = float(game_start)
                # If timestamp is in milliseconds (greater than 1e11), convert to seconds
                if ts > 1e11:
                    ts = ts / 1000.0
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                return dt.date()
            except (ValueError, TypeError, OverflowError) as e:
                logger.debug("Failed to parse game_start timestamp '%s': %s", game_start, e)

        # Fallback to game_start_patched
        game_start_patched = metadata.get("game_start_patched")
        if game_start_patched and isinstance(game_start_patched, str):
            try:
                dt = dateutil.parser.parse(game_start_patched)
                return dt.date()
            except (ValueError, TypeError) as e:
                logger.debug("Failed to parse game_start_patched string '%s': %s", game_start_patched, e)

        return None
