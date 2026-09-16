"""Configuration settings for Valorant analytics raw and landing layers."""

import os
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class IngestionConfig:
    """Configuration class for Databricks Unity Catalog, Volumes, and HenrikDev API."""

    catalog: str = os.getenv("DATABRICKS_CATALOG", "valorant")
    raw_schema: str = os.getenv("DATABRICKS_RAW_SCHEMA", "raw")
    landing_schema: str = os.getenv("DATABRICKS_LANDING_SCHEMA", "landing")
    volume: str = os.getenv("DATABRICKS_VOLUME", "data")
    control_table: str = os.getenv("CONTROL_TABLE_NAME", "ingestion_control")

    # Backward compatibility alias for landing_schema
    schema: Optional[str] = None

    api_base_url: str = os.getenv("HENRIK_API_BASE_URL", "https://api.henrikdev.xyz")
    api_version: str = os.getenv("HENRIK_API_VERSION", "v3")
    secret_scope: str = os.getenv("DATABRICKS_SECRET_SCOPE", "valorant_secrets")
    secret_key: str = os.getenv("DATABRICKS_SECRET_KEY", "henrik_api_key")

    http_timeout_seconds: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
    max_retries: int = int(os.getenv("HTTP_MAX_RETRIES", "3"))

    base_volume_path_override: Optional[str] = os.getenv("BASE_VOLUME_PATH_OVERRIDE", None)

    @property
    def target_landing_schema(self) -> str:
        return self.schema if self.schema is not None else self.landing_schema

    @property
    def raw_root(self) -> str:
        """Returns root path to the Raw Volume: /Volumes/<catalog>/raw/<volume>"""
        if self.base_volume_path_override:
            clean = self.base_volume_path_override.rstrip("/").rstrip("\\")
            return f"{clean}/raw"
        return f"/Volumes/{self.catalog}/{self.raw_schema}/{self.volume}"

    @property
    def raw_historical_root(self) -> str:
        return f"{self.raw_root}/historical"

    @property
    def raw_api_root(self) -> str:
        return f"{self.raw_root}/api"

    @property
    def landing_root(self) -> str:
        """Returns root path to the Landing Volume: /Volumes/<catalog>/landing/<volume>"""
        if self.base_volume_path_override:
            return self.base_volume_path_override.rstrip("/").rstrip("\\")
        return f"/Volumes/{self.catalog}/{self.target_landing_schema}/{self.volume}"

    @property
    def landing_matches_root(self) -> str:
        return f"{self.landing_root}/matches"

    @property
    def volume_root(self) -> str:
        """Backward compatibility for landing_root."""
        return self.landing_root

    @property
    def full_control_table_name(self) -> str:
        """Returns the fully qualified three-level Delta table identifier."""
        return f"{self.catalog}.{self.target_landing_schema}.{self.control_table}"

    def build_match_landing_path(self, match_id: str, match_date: date) -> str:
        """Constructs partitioned target path: .../matches/YYYY/MM/DD/<match_id>.json."""
        year_str = f"{match_date.year:04d}"
        month_str = f"{match_date.month:02d}"
        day_str = f"{match_date.day:02d}"
        return f"{self.landing_matches_root}/{year_str}/{month_str}/{day_str}/{match_id}.json"
