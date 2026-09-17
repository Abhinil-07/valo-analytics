# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_match`
# MAGIC 
# MAGIC - **Grain:** 1 row per match
# MAGIC - **Primary Key:** `match_id`
# MAGIC - **Source:** `data[].metadata` & root `is_available`
# MAGIC - **Target:** `<catalog>.<schema>.bronze_match`

# COMMAND ----------
import os
import glob
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, BooleanType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "bronze"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/data/matches"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_match"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {LANDING_VOLUME_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    match_id STRING NOT NULL,
    is_available BOOLEAN,
    map STRING,
    game_version STRING,
    game_length INT,
    game_start BIGINT,
    game_start_patched STRING,
    rounds_played INT,
    mode STRING,
    mode_id STRING,
    queue STRING,
    season_id STRING,
    platform STRING,
    premier_tournament_id STRING,
    premier_matchup_id STRING,
    region STRING,
    cluster STRING
)
USING DELTA
COMMENT 'Source-aligned match metadata: 1 row per match'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Identify Pending Matches from Ingestion Control (Incremental)

# COMMAND ----------
CONTROL_TABLE = f"{CATALOG}.landing.ingestion_control"

# Query matches already processed in this Bronze table
try:
    existing_bronze_df = spark.sql(f"SELECT DISTINCT match_id FROM {TARGET_TABLE}")
    already_processed_ids = {row["match_id"] for row in existing_bronze_df.collect()}
except Exception:
    already_processed_ids = set()

# Query successful landing files from ingestion_control
control_df = spark.sql(f"""
    SELECT match_id, landing_path 
    FROM {CONTROL_TABLE} 
    WHERE ingestion_status = 'SUCCESS'
""")
landing_records = control_df.collect()

# Filter to pending files only
pending_matches = [
    (row["match_id"], row["landing_path"])
    for row in landing_records
    if row["match_id"] not in already_processed_ids
]

print(f"Total Landing Matches: {len(landing_records)}")
print(f"Already in Bronze:     {len(already_processed_ids)}")
print(f"Pending to Ingest:     {len(pending_matches)}")

if not pending_matches:
    print(f"No pending matches for {TARGET_TABLE}. Bronze is up to date!")
    # Exit early without redundant work
    dbutils.notebook.exit("Success: 0 new matches")

matches_data = []
for mid, file_path in pending_matches:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)
            if isinstance(content, dict):
                if "data" in content and isinstance(content["data"], list):
                    matches_data.extend(content["data"])
                elif "metadata" in content:
                    matches_data.append(content)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

# Deduplicate by matchid
unique_matches = {}
for m in matches_data:
    mid = (m.get("metadata") or {}).get("matchid")
    if mid and mid not in unique_matches:
        unique_matches[mid] = m

print(f"Loaded {len(unique_matches)} pending match object(s) to process.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract `bronze_match` Entity

# COMMAND ----------
match_rows = []
for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue
    
    premier_info = meta.get("premier_info") or {}
    match_rows.append({
        "match_id": match_id,
        "is_available": m.get("is_available"),
        "map": meta.get("map"),
        "game_version": meta.get("game_version"),
        "game_length": meta.get("game_length"),
        "game_start": meta.get("game_start"),
        "game_start_patched": meta.get("game_start_patched"),
        "rounds_played": meta.get("rounds_played"),
        "mode": meta.get("mode"),
        "mode_id": meta.get("mode_id"),
        "queue": meta.get("queue"),
        "season_id": meta.get("season_id"),
        "platform": meta.get("platform"),
        "premier_tournament_id": premier_info.get("tournament_id"),
        "premier_matchup_id": premier_info.get("matchup_id"),
        "region": meta.get("region"),
        "cluster": meta.get("cluster")
    })

schema_match = StructType([
    StructField("match_id", StringType(), False),
    StructField("is_available", BooleanType(), True),
    StructField("map", StringType(), True),
    StructField("game_version", StringType(), True),
    StructField("game_length", IntegerType(), True),
    StructField("game_start", LongType(), True),
    StructField("game_start_patched", StringType(), True),
    StructField("rounds_played", IntegerType(), True),
    StructField("mode", StringType(), True),
    StructField("mode_id", StringType(), True),
    StructField("queue", StringType(), True),
    StructField("season_id", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("premier_tournament_id", StringType(), True),
    StructField("premier_matchup_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("cluster", StringType(), True)
])

df_match = spark.createDataFrame(match_rows, schema=schema_match)
df_match.createOrReplaceTempView("tv_bronze_match")
print(f"Extracted {len(match_rows)} rows for bronze_match.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Delta MERGE (Idempotent Upsert)

# COMMAND ----------
spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_match AS source
ON target.match_id = source.match_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")
print(f"Successfully merged into {TARGET_TABLE}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Data Quality Assertions & Sample View

# COMMAND ----------
stats = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT match_id) AS distinct_matches,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:       {stats['total_rows']}")
print(f"Distinct Matches: {stats['distinct_matches']}")
print(f"Null Matches:     {stats['null_matches']}")

assert stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found!"
assert stats["total_rows"] == stats["distinct_matches"], "DATA QUALITY ERROR: duplicate match_id found!"

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 10"))
