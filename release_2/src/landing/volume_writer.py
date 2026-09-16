"""Writes raw match JSON files to Databricks Unity Catalog Volumes with verification."""

import json
import os
from typing import Any, Dict
from release_2.configs.settings import IngestionConfig
from release_2.src.common.logger import get_logger
from release_2.src.landing.validator import ValidatedMatch

logger = get_logger(__name__)


class VolumeWriterError(Exception):
    """Raised when writing to Databricks Volume fails."""
    pass


class VolumeWriter:
    """Manages writing raw match JSONs to partitioned paths in Databricks Volumes."""

    def __init__(self, config: IngestionConfig):
        """Initializes VolumeWriter with ingestion configuration.
        
        Args:
            config: IngestionConfig containing catalog, schema, volume, or volume_root override.
        """
        self.config = config

    def get_target_path(self, validated_match: ValidatedMatch) -> str:
        """Computes the target file path for a match: .../matches/YYYY/MM/DD/<match_id>.json."""
        return self.config.build_match_landing_path(
            match_id=validated_match.match_id,
            match_date=validated_match.match_date
        )

    def write_match_json(self, validated_match: ValidatedMatch) -> str:
        """Atomically writes raw match JSON to destination volume path and verifies file.
        
        Args:
            validated_match: ValidatedMatch containing match_id, match_date, and raw_payload.
            
        Returns:
            The final written file path.
            
        Raises:
            VolumeWriterError: If writing or post-write verification fails.
        """
        target_path = self.get_target_path(validated_match)
        parent_dir = os.path.dirname(target_path)
        temp_path = f"{target_path}.tmp"

        try:
            os.makedirs(parent_dir, exist_ok=True)

            # Write to temp file first to ensure atomic write
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(validated_match.raw_payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            # Atomic rename / replace
            os.replace(temp_path, target_path)

            # Post-write verification
            if not os.path.exists(target_path):
                raise VolumeWriterError(f"Post-write verification failed: {target_path} not found")

            file_size = os.path.getsize(target_path)
            if file_size == 0:
                raise VolumeWriterError(f"Post-write verification failed: {target_path} is empty (0 bytes)")

            logger.info(
                "Successfully landed match %s to %s (%d bytes)",
                validated_match.match_id,
                target_path,
                file_size
            )
            return target_path

        except Exception as e:
            # Clean up temp file if present
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            logger.error("Failed to write match %s to volume: %s", validated_match.match_id, e)
            raise VolumeWriterError(f"Failed to land match {validated_match.match_id}: {e}") from e

    def file_exists(self, target_path: str) -> bool:
        """Checks if a landed match file exists and is non-empty."""
        return os.path.exists(target_path) and os.path.getsize(target_path) > 0
