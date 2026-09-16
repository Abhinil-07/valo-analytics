"""Landing orchestrator coordinating raw archiving, normalization, canonical landing, and control tracking."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, List, Optional

from release_2.configs.settings import IngestionConfig
from release_2.src.common.logger import get_logger
from release_2.src.landing.api_client import HenrikValApiClient
from release_2.src.landing.validator import MatchValidator, ValidatedMatch, ValidationError
from release_2.src.landing.volume_writer import VolumeWriter, VolumeWriterError
from release_2.src.landing.control_table import IngestionControlManager
from release_2.src.normalization.normalizer import (
    unwrap_matches,
    normalize_match,
    validate_canonical_match,
    NormalizationError
)
from release_2.src.raw.raw_storage import RawStorageManager

logger = get_logger(__name__)


@dataclass
class IngestionSummary:
    total_received: int = 0
    total_valid: int = 0
    ingested: int = 0
    skipped: int = 0
    failed: int = 0
    successful_match_ids: List[str] = field(default_factory=list)
    skipped_match_ids: List[str] = field(default_factory=list)
    failed_match_ids: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class LandingOrchestrator:
    """Orchestrates the ingestion pipeline: Raw Archival -> Normalization -> Canonical Landing -> Control Table."""

    def __init__(
        self,
        config: Optional[IngestionConfig] = None,
        api_client: Optional[HenrikValApiClient] = None,
        validator: Optional[MatchValidator] = None,
        volume_writer: Optional[VolumeWriter] = None,
        control_manager: Optional[IngestionControlManager] = None,
        raw_storage: Optional[RawStorageManager] = None
    ):
        self.config = config or IngestionConfig()
        self.api_client = api_client
        self.validator = validator or MatchValidator()
        self.volume_writer = volume_writer or VolumeWriter(self.config)
        self.control_manager = control_manager or IngestionControlManager(self.config)
        self.raw_storage = raw_storage or RawStorageManager(self.config)

    def run_api_ingestion(
        self,
        region: str,
        puuid: str,
        mode: Optional[str] = "competitive",
        size: Optional[int] = 5
    ) -> IngestionSummary:
        """1. Calls API -> 2. Archives raw response to raw/api/ -> 3. Normalizes & Lands to Landing."""
        logger.info("Executing API ingestion for %s (region: %s)", puuid, region)
        from release_2.src.raw.api_loader import ApiRawLoader
        raw_loader = ApiRawLoader(self.config, self.api_client, self.raw_storage)
        raw_result = raw_loader.fetch_and_archive_by_puuid(
            region=region,
            puuid=puuid,
            mode=mode,
            size=size
        )

        provenance = {
            "source": "api",
            "region": region,
            "puuid": puuid,
            "mode": mode,
            "size": size,
            "raw_api_path": raw_result.raw_path,
            "content_hash": raw_result.content_hash
        }

        return self.process_raw_payload(
            raw_payload=raw_result.raw_payload,
            source="api",
            load_type="incremental",
            source_file=raw_result.raw_path,
            provenance=provenance
        )

    def process_api_response(
        self,
        response_payload: Dict[str, Any],
        source: str = "api",
        load_type: str = "incremental",
        source_file: Optional[str] = None,
        provenance: Optional[Dict[str, Any]] = None
    ) -> IngestionSummary:
        """Backward compatibility alias for process_raw_payload."""
        return self.process_raw_payload(
            raw_payload=response_payload,
            source=source,
            load_type=load_type,
            source_file=source_file,
            provenance=provenance
        )

    def process_raw_payload(
        self,
        raw_payload: Dict[str, Any],
        source: str = "api",
        load_type: str = "incremental",
        source_file: Optional[str] = None,
        provenance: Optional[Dict[str, Any]] = None
    ) -> IngestionSummary:
        """Unwraps, normalizes, validates, and idempotently lands matches to Landing."""
        summary = IngestionSummary()

        # Step 1: Unwrap matches from container
        try:
            raw_matches = unwrap_matches(raw_payload)
        except Exception as e:
            err = f"Failed unwrapping raw payload from {source_file}: {e}"
            logger.error(err)
            summary.errors.append(err)
            return summary

        summary.total_received = len(raw_matches)

        # Step 2: Normalize and validate each match
        canonical_matches: List[Dict[str, Any]] = []
        for raw_m in raw_matches:
            try:
                norm_match = normalize_match(raw_m, source_type=source)
                is_valid, validation_errors = validate_canonical_match(norm_match)
                if not is_valid:
                    raise NormalizationError(f"Canonical validation failed: {validation_errors}")
                canonical_matches.append(norm_match)
            except Exception as ve:
                err_msg = f"Match normalization/validation failed: {ve}"
                logger.error(err_msg)
                summary.failed += 1
                summary.errors.append(err_msg)

        summary.total_valid = len(canonical_matches)

        if not canonical_matches:
            return summary

        # Step 3: Idempotency check via control table
        candidate_ids = [m["metadata"]["matchid"] for m in canonical_matches]
        successful_ids = self.control_manager.get_successful_match_ids(
            candidate_match_ids=candidate_ids,
            verify_file_exists_fn=self.volume_writer.file_exists
        )

        # Step 4: Land new or previously failed matches
        for match in canonical_matches:
            match_id = match["metadata"]["matchid"]
            game_start = match["metadata"].get("game_start")
            
            # Derive match date
            try:
                ts = float(game_start)
                if ts > 1e11:
                    ts = ts / 1000.0
                match_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
            except Exception:
                match_date = datetime.now(timezone.utc).date()

            validated_obj = ValidatedMatch(
                match_id=match_id,
                match_date=match_date,
                raw_payload=match
            )

            if match_id in successful_ids:
                logger.info("Match %s is already successfully landed. Skipping.", match_id)
                summary.skipped += 1
                summary.skipped_match_ids.append(match_id)
                continue

            try:
                target_path = self.volume_writer.write_match_json(validated_obj)
                self.control_manager.record_status(
                    match_id=match_id,
                    match_date=match_date,
                    landing_path=target_path,
                    status="SUCCESS",
                    source=source,
                    load_type=load_type,
                    source_file=source_file,
                    provenance=provenance,
                    error_message=None
                )
                summary.ingested += 1
                summary.successful_match_ids.append(match_id)
            except Exception as e:
                err_msg = f"Failed landing canonical match {match_id}: {e}"
                logger.error(err_msg)
                self.control_manager.record_status(
                    match_id=match_id,
                    match_date=match_date,
                    landing_path=None,
                    status="FAILED",
                    source=source,
                    load_type=load_type,
                    source_file=source_file,
                    provenance=provenance,
                    error_message=str(e)
                )
                summary.failed += 1
                summary.failed_match_ids.append(match_id)
                summary.errors.append(err_msg)

        logger.info(
            "Landing run complete. Total: %d, Landed: %d, Skipped: %d, Failed: %d",
            summary.total_received, summary.ingested, summary.skipped, summary.failed
        )
        return summary
