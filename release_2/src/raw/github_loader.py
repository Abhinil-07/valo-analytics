"""GitHub historical match loader ingesting raw matches into Databricks Raw storage."""

from dataclasses import dataclass, field
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple
import requests

from release_2.configs.settings import IngestionConfig
from release_2.src.common.logger import get_logger
from release_2.src.raw.raw_storage import RawStorageManager

logger = get_logger(__name__)

PINNED_GITHUB_REPO = "Kunal-deve1oper/valorant-data-collector"
PINNED_GITHUB_COMMIT = "f2f983fce113aea9e02407e024439fd1f41fd8d2"
RAW_MATCHES_PREFIX = "raw_matches"


@dataclass
class GitHubIngestionSummary:
    total_discovered: int = 0
    newly_ingested: int = 0
    skipped_existing: int = 0
    failed: int = 0
    files_processed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class GitHubHistoricalLoader:
    """Discovers, downloads, and archives unaltered historical match JSONs from pinned GitHub commit."""

    def __init__(
        self,
        config: Optional[IngestionConfig] = None,
        storage_manager: Optional[RawStorageManager] = None,
        commit_sha: str = PINNED_GITHUB_COMMIT,
        repo: str = PINNED_GITHUB_REPO
    ):
        self.config = config or IngestionConfig()
        self.storage_manager = storage_manager or RawStorageManager(self.config)
        self.commit_sha = commit_sha
        self.repo = repo
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Valorant-Historical-Loader/1.0"})

    def discover_files(self) -> List[str]:
        """Discovers all JSON files under raw_matches/ in the pinned commit via GitHub Git Trees API."""
        url = f"https://api.github.com/repos/{self.repo}/git/trees/{self.commit_sha}?recursive=1"
        logger.info("Discovering historical files from GitHub: %s (commit: %s)", self.repo, self.commit_sha[:8])
        resp = self.session.get(url, timeout=self.config.http_timeout_seconds)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to query GitHub tree (HTTP {resp.status_code}): {resp.text}")

        tree = resp.json().get("tree", [])
        raw_files = [
            item["path"] for item in tree
            if item.get("path", "").startswith(RAW_MATCHES_PREFIX)
            and item.get("path", "").endswith(".json")
            and item.get("type") == "blob"
        ]
        logger.info("Discovered %d historical JSON file(s) under %s/", len(raw_files), RAW_MATCHES_PREFIX)
        return raw_files

    def download_file_bytes(self, relative_path: str) -> bytes:
        """Downloads exact raw bytes for a specific file at the pinned commit."""
        clean_path = relative_path.replace("\\", "/").lstrip("/")
        raw_url = f"https://raw.githubusercontent.com/{self.repo}/{self.commit_sha}/{clean_path}"
        resp = self.session.get(raw_url, timeout=self.config.http_timeout_seconds)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to download {clean_path} from GitHub (HTTP {resp.status_code})")
        return resp.content

    def ingest_file(self, relative_path: str, raw_bytes: Optional[bytes] = None) -> Tuple[str, str, bool]:
        """Writes unchanged source JSON to Raw storage under historical/ preserving relative path.
        
        Returns:
            (target_path: str, sha256_hash: str, is_new: bool)
        """
        if raw_bytes is None:
            raw_bytes = self.download_file_bytes(relative_path)

        # Validate that content is valid JSON without modifying it
        try:
            json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"Downloaded file {relative_path} is not valid JSON: {e}")

        target_path = self.storage_manager.get_historical_target_path(relative_path)
        return self.storage_manager.write_raw_file(target_path, raw_bytes)

    def run_historical_ingestion(
        self,
        max_files: Optional[int] = None
    ) -> GitHubIngestionSummary:
        """Executes full or limited historical raw ingestion.
        
        Args:
            max_files: Optional cap on files to process (useful for quick testing/runs).
        """
        summary = GitHubIngestionSummary()
        files = self.discover_files()
        if max_files:
            files = files[:max_files]

        summary.total_discovered = len(files)

        for rel_path in files:
            try:
                raw_bytes = self.download_file_bytes(rel_path)
                target_path, sha_hash, is_new = self.ingest_file(rel_path, raw_bytes)
                if is_new:
                    summary.newly_ingested += 1
                else:
                    summary.skipped_existing += 1
                summary.files_processed.append(target_path)
            except Exception as e:
                err_msg = f"Failed to ingest {rel_path}: {e}"
                logger.error(err_msg)
                summary.failed += 1
                summary.errors.append(err_msg)

        logger.info(
            "Historical ingestion complete. Total: %d, Landed: %d, Skipped: %d, Failed: %d",
            summary.total_discovered, summary.newly_ingested, summary.skipped_existing, summary.failed
        )
        return summary
