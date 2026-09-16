"""Storage manager for writing unaltered source payloads to Databricks Raw volume."""

import hashlib
import json
import os
from typing import Any, Dict, Optional, Tuple

from release_2.configs.settings import IngestionConfig
from release_2.src.common.logger import get_logger

logger = get_logger(__name__)


def compute_sha256(content_bytes: bytes) -> str:
    """Computes hexadecimal SHA256 digest of bytes."""
    return hashlib.sha256(content_bytes).hexdigest()


class RawStorageManager:
    """Manages writing and reading from /Volumes/<catalog>/raw/<volume>/..."""

    def __init__(self, config: Optional[IngestionConfig] = None):
        self.config = config or IngestionConfig()

    def get_historical_target_path(self, relative_source_path: str) -> str:
        """Constructs target path for a GitHub historical file:
        e.g. /Volumes/<catalog>/raw/<volume>/historical/raw_matches/<filename>.json
        """
        clean_rel = relative_source_path.replace("\\", "/").lstrip("/")
        return f"{self.config.raw_historical_root}/{clean_rel}"

    def get_api_target_path(self, request_timestamp: str, request_hash: str) -> str:
        """Constructs target path for a raw API response archive:
        e.g. /Volumes/<catalog>/raw/<volume>/api/YYYY/MM/DD/api_response_<timestamp>_<hash>.json
        """
        date_folder = request_timestamp[:10].replace("-", "/")
        filename = f"api_response_{request_timestamp}_{request_hash}.json"
        return f"{self.config.raw_api_root}/{date_folder}/{filename}"

    def write_raw_file(
        self,
        target_path: str,
        content_bytes: bytes,
        overwrite_if_same_hash: bool = False
    ) -> Tuple[str, str, bool]:
        """Atomically writes raw bytes to destination path.
        
        Returns:
            (final_path: str, sha256_hash: str, is_new_or_updated: bool)
        """
        content_hash = compute_sha256(content_bytes)

        if os.path.exists(target_path):
            with open(target_path, "rb") as existing_f:
                existing_bytes = existing_f.read()
            existing_hash = compute_sha256(existing_bytes)
            if existing_hash == content_hash and not overwrite_if_same_hash:
                logger.debug("Raw file %s already exists with identical SHA256. Skipping write.", target_path)
                return target_path, content_hash, False

        parent_dir = os.path.dirname(target_path)
        os.makedirs(parent_dir, exist_ok=True)
        temp_path = f"{target_path}.tmp"

        with open(temp_path, "wb") as f:
            f.write(content_bytes)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_path, target_path)
        logger.info("Landed raw source file to %s (%d bytes, sha256: %s)", target_path, len(content_bytes), content_hash[:8])
        return target_path, content_hash, True
