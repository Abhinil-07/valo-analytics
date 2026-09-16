# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_team`
# MAGIC 
# MAGIC - **Grain:** 1 row per team per match
# MAGIC - **Composite Key:** `match_id` + `team_side`
# MAGIC - **Source:** `data[].teams.red` & `data[].teams.blue`
# MAGIC - **Target:** `<catalog>.<schema>.bronze_team`

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
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_team"

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
    team_side STRING NOT NULL,
    has_won BOOLEAN,
    rounds_won INT,
    rounds_lost INT
)
USING DELTA
COMMENT 'Source-aligned team performance: 1 row per team per match'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Load Match JSONs from Landing Volume

# COMMAND ----------
json_files = glob.glob(f"{LANDING_VOLUME_PATH}/*/*/*/*.json")
if not json_files:
    json_files = glob.glob(f"{LANDING_VOLUME_PATH}/**/*.json", recursive=True)

print(f"Found {len(json_files)} landing JSON file(s).")

matches_data = []
for file_path in json_files:
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

print(f"Loaded {len(unique_matches)} unique match object(s).")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract `bronze_team` Entities

# COMMAND ----------
team_rows = []
for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    teams_dict = m.get("teams") or {}
    for side in ["red", "blue"]:
        t_info = teams_dict.get(side) or {}
        team_rows.append({
            "match_id": match_id,
            "team_side": side.capitalize(),
            "has_won": t_info.get("has_won"),
            "rounds_won": t_info.get("rounds_won"),
            "rounds_lost": t_info.get("rounds_lost")
        })

schema_team = StructType([
    StructField("match_id", StringType(), False),
    StructField("team_side", StringType(), False),
    StructField("has_won", BooleanType(), True),
    StructField("rounds_won", IntegerType(), True),
    StructField("rounds_lost", IntegerType(), True)
])

df_team = spark.createDataFrame(team_rows, schema=schema_team)
df_team.createOrReplaceTempView("tv_bronze_team")
print(f"Extracted {len(team_rows)} rows for bronze_team.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Delta MERGE (Idempotent Upsert)

# COMMAND ----------
spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_team AS source
ON target.match_id = source.match_id 
AND target.team_side = source.team_side
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
    COUNT(DISTINCT match_id, team_side) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN team_side NOT IN ('Red', 'Blue') THEN 1 END) AS invalid_sides
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:     {stats['total_rows']}")
print(f"Distinct Keys:  {stats['distinct_keys']}")
print(f"Null Matches:   {stats['null_matches']}")
print(f"Invalid Sides:  {stats['invalid_sides']}")

assert stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found!"
assert stats["invalid_sides"] == 0, "DATA QUALITY ERROR: invalid team_side found!"
assert stats["total_rows"] == stats["distinct_keys"], "DATA QUALITY ERROR: duplicate (match_id, team_side) found!"

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 10"))
