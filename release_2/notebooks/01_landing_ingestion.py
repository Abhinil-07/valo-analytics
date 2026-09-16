# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Phase 1: Valorant Match Landing Layer Ingestion
# MAGIC 
# MAGIC This notebook orchestrates the ingestion of raw Valorant match payloads from the HenrikDev API into a Databricks Unity Catalog Volume:
# MAGIC - Path: `/Volumes/<catalog>/<schema>/<volume>/matches/YYYY/MM/DD/<match_id>.json`
# MAGIC - Idempotency & Ingestion Tracking: `<catalog>.<schema>.ingestion_control` Delta table.

# COMMAND ----------
import sys
import os

# Ensure repository root is on sys.path when running in Databricks Repos / Workflows
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from configs.settings import IngestionConfig
from src.common.logger import get_logger
from src.common.databricks_utils import get_databricks_secret, get_spark_session
from src.landing import (
    HenrikValApiClient,
    LandingOrchestrator,
    MatchValidator,
    VolumeWriter,
    IngestionControlManager
)

logger = get_logger("databricks_landing_runner")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Initialize Databricks Widgets for Runtime Parameters

# COMMAND ----------
dbutils = None
try:
    import IPython
    ipython = IPython.get_ipython()
    if ipython and "dbutils" in ipython.user_ns:
        dbutils = ipython.user_ns["dbutils"]
except Exception:
    pass

if dbutils:
    dbutils.widgets.text("catalog", "main", "1. Unity Catalog")
    dbutils.widgets.text("schema", "valorant", "2. Schema / DB")
    dbutils.widgets.text("volume", "landing", "3. Volume Name")
    dbutils.widgets.text("region", "na", "4. Region (na, eu, ap, kr)")
    dbutils.widgets.text("player_name", "TenZ", "5. Player Name")
    dbutils.widgets.text("tag", "0001", "6. Player Tag")
    dbutils.widgets.dropdown("queue", "competitive", ["competitive", "unrated", "all"], "7. Queue Filter")
    dbutils.widgets.text("secret_scope", "valorant_secrets", "8. Secret Scope")
    dbutils.widgets.text("secret_key", "henrik_api_key", "9. Secret Key")

    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    volume = dbutils.widgets.get("volume")
    region = dbutils.widgets.get("region")
    player_name = dbutils.widgets.get("player_name")
    tag = dbutils.widgets.get("tag")
    queue = dbutils.widgets.get("queue") if dbutils.widgets.get("queue") != "all" else None
    secret_scope = dbutils.widgets.get("secret_scope")
    secret_key = dbutils.widgets.get("secret_key")
else:
    # Default fallbacks when running outside interactive widget context
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "valorant")
    volume = os.getenv("DATABRICKS_VOLUME", "landing")
    region = os.getenv("VAL_REGION", "na")
    player_name = os.getenv("VAL_PLAYER_NAME", "TenZ")
    tag = os.getenv("VAL_TAG", "0001")
    queue = os.getenv("VAL_QUEUE", "competitive")
    secret_scope = os.getenv("DATABRICKS_SECRET_SCOPE", "valorant_secrets")
    secret_key = os.getenv("DATABRICKS_SECRET_KEY", "henrik_api_key")

logger.info(
    "Running landing ingestion for %s#%s (%s) -> Volume: /Volumes/%s/%s/%s",
    player_name, tag, region, catalog, schema, volume
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Configure Components & Secure API Client

# COMMAND ----------
config = IngestionConfig(
    catalog=catalog,
    schema=schema,
    volume=volume,
    secret_scope=secret_scope,
    secret_key=secret_key
)

api_key = get_databricks_secret(scope=config.secret_scope, key=config.secret_key)
if not api_key:
    raise RuntimeError(
        f"API Key not found in secret scope '{config.secret_scope}', key '{config.secret_key}'. "
        "Please create the secret using the Databricks CLI or set the HENRIK_API_KEY environment variable."
    )

spark = get_spark_session()
api_client = HenrikValApiClient(api_key=api_key, base_url=config.api_base_url, api_version=config.api_version)
validator = MatchValidator()
volume_writer = VolumeWriter(config)
control_manager = IngestionControlManager(config=config, spark=spark)

orchestrator = LandingOrchestrator(
    config=config,
    api_client=api_client,
    validator=validator,
    volume_writer=volume_writer,
    control_manager=control_manager
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Execute Ingestion Pipeline

# COMMAND ----------
summary = orchestrator.run_player_ingestion(
    region=region,
    name=player_name,
    tag=tag,
    queue=queue,
    size=10
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Display Ingestion Results & Audit Summary

# COMMAND ----------
print("=" * 60)
print("LANDING INGESTION SUMMARY")
print("=" * 60)
print(f"Total Matches Received: {summary.total_received}")
print(f"Valid Match Payloads:  {summary.total_valid}")
print(f"Newly Landed Matches:  {summary.ingested}")
print(f"Skipped (Idempotent):  {summary.skipped}")
print(f"Failed Matches:        {summary.failed}")
if summary.successful_match_ids:
    print(f"Landed Match IDs:      {', '.join(summary.successful_match_ids)}")
if summary.skipped_match_ids:
    print(f"Skipped Match IDs:     {', '.join(summary.skipped_match_ids)}")
if summary.errors:
    print("Errors encountered:")
    for err in summary.errors:
        print(f" - {err}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Query Delta Control Table for Verification

# COMMAND ----------
if spark:
    display(spark.sql(f"SELECT * FROM {config.full_control_table_name} ORDER BY ingestion_timestamp DESC LIMIT 20"))
