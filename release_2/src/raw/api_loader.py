"""API loader fetching HenrikDev match payloads and archiving unaltered responses to Raw storage."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from typing import Any, Dict, Optional, Tuple

from release_2.configs.settings import IngestionConfig
from release_2.src.common.logger import get_logger
from release_2.src.landing.api_client import HenrikValApiClient
from release_2.src.raw.raw_storage import RawStorageManager

logger = get_logger(__name__)


@dataclass
class ApiRawIngestionResult:
    raw_path: str
    content_hash: str
    match_count: int
    raw_payload: Dict[str, Any]


class ApiRawLoader:
    """Retrieves live match payloads from HenrikDev API and archives the unaltered response to raw/api/."""

    def __init__(
        self,
        config: Optional[IngestionConfig] = None,
        api_client: Optional[HenrikValApiClient] = None,
        storage_manager: Optional[RawStorageManager] = None
    ):
        self.config = config or IngestionConfig()
        self.api_client = api_client
        self.storage_manager = storage_manager or RawStorageManager(self.config)

    def fetch_and_archive_by_puuid(
        self,
        region: str,
        puuid: str,
        mode: Optional[str] = "competitive",
        size: Optional[int] = 5
    ) -> ApiRawIngestionResult:
        """Calls HenrikDev API by PUUID and writes the full, unaltered response to raw/api/."""
        if not self.api_client:
            raise ValueError("HenrikValApiClient instance required")

        url = f"{self.config.api_base_url}/valorant/{self.config.api_version}/by-puuid/matches/{region}/{puuid}"
        params: Dict[str, Any] = {}
        if mode:
            params["mode"] = mode
        if size:
            params["size"] = size

        logger.info("Fetching raw API response from %s (params: %s)", url, params)
        raw_response_dict = self.api_client._make_request("GET", url, params=params)

        now_utc = datetime.now(timezone.utc)
        ts_str = now_utc.strftime("%Y%m%dT%H%M%SZ")
        req_signature = f"{region}_{puuid}_{mode}_{size}"
        req_hash = hashlib.sha256(req_signature.encode("utf-8")).hexdigest()[:8]

        target_path = self.storage_manager.get_api_target_path(
            request_timestamp=ts_str,
            request_hash=req_hash
        )

        raw_bytes = json.dumps(raw_response_dict, ensure_ascii=False, indent=2).encode("utf-8")
        final_path, content_hash, _ = self.storage_manager.write_raw_file(target_path, raw_bytes)

        match_count = len(raw_response_dict.get("data", [])) if isinstance(raw_response_dict.get("data"), list) else 0

        return ApiRawIngestionResult(
            raw_path=final_path,
            content_hash=content_hash,
            match_count=match_count,
            raw_payload=raw_response_dict
        )

    def archive_raw_payload(
        self,
        raw_payload_dict: Dict[str, Any],
        request_label: str = "manual"
    ) -> ApiRawIngestionResult:
        """Archives an already-fetched raw API payload dictionary directly into raw/api/."""
        now_utc = datetime.now(timezone.utc)
        ts_str = now_utc.strftime("%Y%m%dT%H%M%SZ")
        req_hash = hashlib.sha256(request_label.encode("utf-8")).hexdigest()[:8]

        target_path = self.storage_manager.get_api_target_path(
            request_timestamp=ts_str,
            request_hash=req_hash
        )

        raw_bytes = json.dumps(raw_payload_dict, ensure_ascii=False, indent=2).encode("utf-8")
        final_path, content_hash, _ = self.storage_manager.write_raw_file(target_path, raw_bytes)

        match_count = len(raw_payload_dict.get("data", [])) if isinstance(raw_payload_dict.get("data"), list) else 0

        return ApiRawIngestionResult(
            raw_path=final_path,
            content_hash=content_hash,
            match_count=match_count,
            raw_payload=raw_payload_dict
        )
