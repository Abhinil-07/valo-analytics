# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_round`
# MAGIC 
# MAGIC - **Grain:** 1 row per round per match
# MAGIC - **Composite Key:** `match_id` + `round_number`
# MAGIC - **Source:** `data[].rounds[]` (round_number derived from 0-based array index + 1)
# MAGIC - **Target:** `<catalog>.<schema>.bronze_round`

# COMMAND ----------
import os
import glob
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "bronze"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/data/matches"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_round"

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
    round_number INT NOT NULL,
    winning_team STRING,
    end_type STRING,
    bomb_planted BOOLEAN,
    bomb_defused BOOLEAN
)
USING DELTA
COMMENT 'Source-aligned round summaries: 1 row per round per match'
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
# MAGIC ### Step 3: Extract `bronze_round` Entities

# COMMAND ----------
round_rows = []
for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    rounds_list = m.get("rounds") or []
    # Derive round_number: 0-based array index + 1
    for idx, r in enumerate(rounds_list):
        round_rows.append({
            "match_id": match_id,
            "round_number": idx + 1,
            "winning_team": r.get("winning_team"),
            "end_type": r.get("end_type"),
            "bomb_planted": r.get("bomb_planted"),
            "bomb_defused": r.get("bomb_defused")
        })

schema_round = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("winning_team", StringType(), True),
    StructField("end_type", StringType(), True),
    StructField("bomb_planted", BooleanType(), True),
    StructField("bomb_defused", BooleanType(), True)
])

df_round = spark.createDataFrame(round_rows, schema=schema_round)
df_round.createOrReplaceTempView("tv_bronze_round")
print(f"Extracted {len(round_rows)} rows for bronze_round.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Delta MERGE (Idempotent Upsert)

# COMMAND ----------
spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_round AS source
ON target.match_id = source.match_id 
AND target.round_number = source.round_number
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
    COUNT(DISTINCT match_id, round_number) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN round_number < 1 OR round_number IS NULL THEN 1 END) AS invalid_rounds
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:      {stats['total_rows']}")
print(f"Distinct Keys:   {stats['distinct_keys']}")
print(f"Null Matches:    {stats['null_matches']}")
print(f"Invalid Rounds:  {stats['invalid_rounds']}")

assert stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found!"
assert stats["invalid_rounds"] == 0, "DATA QUALITY ERROR: invalid round_number found!"
assert stats["total_rows"] == stats["distinct_keys"], "DATA QUALITY ERROR: duplicate (match_id, round_number) found!"

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 15"))
