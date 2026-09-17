# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_defuse_event`
# MAGIC 
# MAGIC - **Grain:** 1 row per spike defuse event per match per round
# MAGIC - **Composite Key:** `match_id` + `round_number` + `defuse_event_sequence`
# MAGIC - **Source:** `rounds[].defuse_events`
# MAGIC - **Target:** `<catalog>.<schema>.bronze_defuse_event`
# MAGIC - **Round Index Rule:** `round_number = rounds[] array position + 1`
# MAGIC - **Sequence Rule:** `defuse_event_sequence = 0-based defuse event sequence within round`

# COMMAND ----------
import os
import glob
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "bronze"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/data/matches"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_defuse_event"

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
    defuse_event_sequence INT NOT NULL,
    player_puuid STRING,
    player_display_name STRING,
    player_team STRING,
    defuse_site STRING,
    defuse_time_in_round INT,
    defuse_location STRUCT<x: DOUBLE, y: DOUBLE>,
    player_locations_on_defuse ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>>
)
USING DELTA
COMMENT 'Source-aligned spike defuse events from rounds[].defuse_events'
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
for fpath in json_files:
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            content = json.load(f)
            if isinstance(content, dict):
                if "data" in content and isinstance(content["data"], list):
                    matches_data.extend(content["data"])
                elif "metadata" in content:
                    matches_data.append(content)
    except Exception as e:
        print(f"Error reading {fpath}: {e}")

unique_matches = {}
for m in matches_data:
    mid = (m.get("metadata") or {}).get("matchid")
    if mid and mid not in unique_matches:
        unique_matches[mid] = m

print(f"Loaded {len(unique_matches)} unique match(es) for bronze_defuse_event.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract `bronze_defuse_event` Entities

# COMMAND ----------
defuse_rows = []

for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    rounds_list = m.get("rounds") or []
    for r_idx, r in enumerate(rounds_list):
        round_number = r_idx + 1
        de_raw = r.get("defuse_events")
        if not de_raw:
            continue

        events_list = de_raw if isinstance(de_raw, list) else [de_raw]

        for d_seq, de in enumerate(events_list):
            if not isinstance(de, dict):
                continue

            defused_by = de.get("defused_by")
            if not defused_by and not de.get("defuse_site") and de.get("defuse_time_in_round") is None:
                continue

            d_puuid = defused_by.get("puuid") if isinstance(defused_by, dict) else None
            d_name = defused_by.get("display_name") if isinstance(defused_by, dict) else None
            d_team = defused_by.get("team") if isinstance(defused_by, dict) else None

            # Location
            loc_raw = de.get("defuse_location") or {}
            defuse_loc = {
                "x": float(loc_raw["x"]) if "x" in loc_raw and loc_raw["x"] is not None else None,
                "y": float(loc_raw["y"]) if "y" in loc_raw and loc_raw["y"] is not None else None
            } if loc_raw else None

            # Player locations on defuse
            plocs_raw = de.get("player_locations_on_defuse") or []
            plocs = []
            for pl in plocs_raw:
                if isinstance(pl, dict):
                    loc = pl.get("location") or {}
                    plocs.append({
                        "player_puuid": pl.get("player_puuid"),
                        "player_display_name": pl.get("player_display_name"),
                        "player_team": pl.get("player_team"),
                        "location": {
                            "x": float(loc["x"]) if "x" in loc and loc["x"] is not None else None,
                            "y": float(loc["y"]) if "y" in loc and loc["y"] is not None else None
                        },
                        "view_radians": float(pl["view_radians"]) if "view_radians" in pl and pl["view_radians"] is not None else None
                    })

            defuse_rows.append({
                "match_id": match_id,
                "round_number": round_number,
                "defuse_event_sequence": d_seq,
                "player_puuid": d_puuid,
                "player_display_name": d_name,
                "player_team": d_team,
                "defuse_site": de.get("defuse_site"),
                "defuse_time_in_round": de.get("defuse_time_in_round"),
                "defuse_location": defuse_loc,
                "player_locations_on_defuse": plocs
            })

print(f"Extracted {len(defuse_rows)} rows for bronze_defuse_event.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Create DataFrame & Delta MERGE (Idempotent Upsert)

# COMMAND ----------
loc_struct = StructType([
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True)
])

player_loc_struct = StructType([
    StructField("player_puuid", StringType(), True),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("location", loc_struct, True),
    StructField("view_radians", DoubleType(), True)
])

schema_defuse = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("defuse_event_sequence", IntegerType(), False),
    StructField("player_puuid", StringType(), True),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("defuse_site", StringType(), True),
    StructField("defuse_time_in_round", IntegerType(), True),
    StructField("defuse_location", loc_struct, True),
    StructField("player_locations_on_defuse", ArrayType(player_loc_struct), True)
])

df_defuse = spark.createDataFrame(defuse_rows, schema=schema_defuse)
df_defuse.createOrReplaceTempView("tv_bronze_defuse_event")

spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_defuse_event AS source
ON target.match_id = source.match_id
AND target.round_number = source.round_number
AND target.defuse_event_sequence = source.defuse_event_sequence
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")
print(f"Successfully merged into {TARGET_TABLE}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Data Quality & Reconciliation Assertions

# COMMAND ----------
stats = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT match_id, round_number, defuse_event_sequence) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN round_number < 1 OR round_number IS NULL THEN 1 END) AS invalid_rounds,
    COUNT(CASE WHEN defuse_event_sequence IS NULL THEN 1 END) AS null_sequences
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:      {stats['total_rows']}")
print(f"Distinct Keys:   {stats['distinct_keys']}")
print(f"Null Matches:    {stats['null_matches']}")
print(f"Invalid Rounds:  {stats['invalid_rounds']}")
print(f"Null Sequences:  {stats['null_sequences']}")

assert stats["null_matches"] == 0, "DQ ERROR: null match_id found!"
assert stats["invalid_rounds"] == 0, "DQ ERROR: invalid round_number found!"
assert stats["null_sequences"] == 0, "DQ ERROR: null defuse_event_sequence found!"
assert stats["total_rows"] == stats["distinct_keys"], "DQ ERROR: duplicate composite key found!"

# Source count reconciliation
expected_defuses = 0
for m in unique_matches.values():
    for r in m.get("rounds", []):
        de = r.get("defuse_events")
        if de:
            evts = de if isinstance(de, list) else [de]
            for e in evts:
                if isinstance(e, dict) and (e.get("defused_by") or e.get("defuse_site") or e.get("defuse_time_in_round") is not None):
                    expected_defuses += 1

assert stats["total_rows"] == expected_defuses, f"Source reconciliation failed! Expected {expected_defuses}, found {stats['total_rows']}"
print(f"Reconciliation SUCCESS: Exact match with source defuse events count ({expected_defuses} rows).")

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 15"))
