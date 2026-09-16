"""Delta Lake ingestion_control table manager with provenance tracking, idempotency, and reconciliation."""

from datetime import date, datetime, timezone
import json
from typing import Any, Dict, List, Optional, Set
from release_2.configs.settings import IngestionConfig
from release_2.src.common.logger import get_logger
from release_2.src.common.databricks_utils import get_spark_session

logger = get_logger(__name__)


class IngestionControlManager:
    """Manages tracking of match ingestion state in the Delta control table."""

    def __init__(self, config: IngestionConfig, spark: Optional[Any] = None):
        self.config = config
        self.spark = spark if spark is not None else get_spark_session()
        self.table_name = config.full_control_table_name
        self._local_records: Dict[str, Dict[str, Any]] = {}

        if self.spark:
            self.ensure_table_exists()
        else:
            logger.info("SparkSession not detected. IngestionControlManager operating in local/mock mode.")

    def ensure_table_exists(self) -> None:
        """Creates the Delta ingestion_control table if it does not already exist."""
        if not self.spark:
            return

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            match_id STRING NOT NULL,
            match_date DATE,
            landing_path STRING,
            ingestion_timestamp TIMESTAMP,
            ingestion_status STRING,
            source STRING,
            load_type STRING,
            source_file STRING,
            provenance STRING,
            error_message STRING
        )
        USING DELTA
        COMMENT 'Control table tracking match ingestion state, provenance, and idempotency'
        """
        try:
            logger.info("Ensuring ingestion control Delta table exists: %s", self.table_name)
            self.spark.sql(ddl)
        except Exception as e:
            logger.error("Failed to initialize ingestion_control Delta table: %s", e)
            raise

    def get_successful_match_ids(
        self,
        candidate_match_ids: List[str],
        verify_file_exists_fn: Optional[Any] = None
    ) -> Set[str]:
        """Queries control table to find which matches are already successfully ingested."""
        if not candidate_match_ids:
            return set()

        successful_ids: Set[str] = set()

        if self.spark:
            try:
                ids_formatted = ", ".join([f"'{m}'" for m in candidate_match_ids])
                query = f"""
                SELECT match_id, landing_path, ingestion_status
                FROM {self.table_name}
                WHERE match_id IN ({ids_formatted})
                """
                df = self.spark.sql(query)
                rows = df.collect()

                for row in rows:
                    m_id = row["match_id"]
                    status = row["ingestion_status"]
                    path = row["landing_path"]

                    if status == "SUCCESS":
                        if verify_file_exists_fn and path and not verify_file_exists_fn(path):
                            logger.warning(
                                "Reconciliation alert: Match %s marked SUCCESS but file missing at %s. Marking for re-ingestion.",
                                m_id, path
                            )
                            continue
                        successful_ids.add(m_id)

                return successful_ids
            except Exception as e:
                logger.error("Error querying ingestion_control table: %s", e)
                raise
        else:
            # Local fallback mode
            for m_id in candidate_match_ids:
                record = self._local_records.get(m_id)
                if record and record.get("ingestion_status") == "SUCCESS":
                    path = record.get("landing_path")
                    if verify_file_exists_fn and path and not verify_file_exists_fn(path):
                        logger.warning(
                            "Reconciliation alert: Match %s marked SUCCESS but file missing at %s. Marking for re-ingestion.",
                            m_id, path
                        )
                        continue
                    successful_ids.add(m_id)
            return successful_ids

    def record_status(
        self,
        match_id: str,
        match_date: Optional[date],
        landing_path: Optional[str],
        status: str,
        source: str = "api",
        load_type: str = "incremental",
        source_file: Optional[str] = None,
        provenance: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> None:
        """Upserts a record into the ingestion_control table using Delta MERGE."""
        now = datetime.now(timezone.utc)
        prov_str = json.dumps(provenance) if provenance else None

        if self.spark:
            try:
                date_str = f"DATE('{match_date.isoformat()}')" if match_date else "NULL"
                path_str = f"'{landing_path}'" if landing_path else "NULL"
                src_file_str = f"'{source_file}'" if source_file else "NULL"
                escaped_prov = prov_str.replace("'", "''") if prov_str else None
                prov_sql = f"'{escaped_prov}'" if escaped_prov else "NULL"
                escaped_err = error_message.replace("'", "''") if error_message else None
                err_str = f"'{escaped_err}'" if escaped_err else "NULL"
                ts_str = f"'{now.strftime('%Y-%m-%d %H:%M:%S')}'"

                merge_sql = f"""
                MERGE INTO {self.table_name} AS target
                USING (
                    SELECT 
                        '{match_id}' AS match_id,
                        {date_str} AS match_date,
                        {path_str} AS landing_path,
                        CAST({ts_str} AS TIMESTAMP) AS ingestion_timestamp,
                        '{status}' AS ingestion_status,
                        '{source}' AS source,
                        '{load_type}' AS load_type,
                        {src_file_str} AS source_file,
                        {prov_sql} AS provenance,
                        {err_str} AS error_message
                ) AS source
                ON target.match_id = source.match_id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.match_date = source.match_date,
                        target.landing_path = source.landing_path,
                        target.ingestion_timestamp = source.ingestion_timestamp,
                        target.ingestion_status = source.ingestion_status,
                        target.source = source.source,
                        target.load_type = source.load_type,
                        target.source_file = source.source_file,
                        target.provenance = source.provenance,
                        target.error_message = source.error_message
                WHEN NOT MATCHED THEN
                    INSERT (
                        match_id, match_date, landing_path,
                        ingestion_timestamp, ingestion_status, source,
                        load_type, source_file, provenance, error_message
                    )
                    VALUES (
                        source.match_id, source.match_date, source.landing_path,
                        source.ingestion_timestamp, source.ingestion_status, source.source,
                        source.load_type, source.source_file, source.provenance, source.error_message
                    )
                """
                self.spark.sql(merge_sql)
                logger.info("Recorded ingestion status %s for match %s in Delta table", status, match_id)
            except Exception as e:
                logger.error("Failed to record status in Delta table for match %s: %s", match_id, e)
                raise
        else:
            self._local_records[match_id] = {
                "match_id": match_id,
                "match_date": match_date,
                "landing_path": landing_path,
                "ingestion_timestamp": now,
                "ingestion_status": status,
                "source": source,
                "load_type": load_type,
                "source_file": source_file,
                "provenance": prov_str,
                "error_message": error_message,
            }
            logger.info("Recorded [LOCAL] ingestion status %s for match %s", status, match_id)
