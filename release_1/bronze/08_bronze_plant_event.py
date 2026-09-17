# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_plant_event`
# MAGIC 
# MAGIC - **Grain:** 1 row per spike plant event per match per round
# MAGIC - **Composite Key:** `match_id` + `round_number` + `plant_event_sequence`
# MAGIC - **Source:** `rounds[].plant_events`
# MAGIC - **Target:** `<catalog>.<schema>.bronze_plant_event`
# MAGIC - **Round Index Rule:** `round_number = rounds[] array position + 1`
# MAGIC - **Sequence Rule:** `plant_event_sequence = 0-based plant event sequence within round`

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
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_plant_event"

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
    plant_event_sequence INT NOT NULL,
    player_puuid STRING,
    player_display_name STRING,
    player_team STRING,
    plant_site STRING,
    plant_time_in_round INT,
    plant_location STRUCT<x: DOUBLE, y: DOUBLE>,
    player_locations_on_plant ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>>
)
USING DELTA
COMMENT 'Source-aligned spike plant events from rounds[].plant_events'
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
# MAGIC ### Step 3: Extract `bronze_plant_event` Entities

# COMMAND ----------
plant_rows = []

for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    rounds_list = m.get("rounds") or []
    for r_idx, r in enumerate(rounds_list):
        round_number = r_idx + 1
        pe_raw = r.get("plant_events")
        if not pe_raw:
            continue

        # Normalise list vs single dict
        events_list = pe_raw if isinstance(pe_raw, list) else [pe_raw]

        for p_seq, pe in enumerate(events_list):
            if not isinstance(pe, dict):
                continue

            planted_by = pe.get("planted_by")
            # If planted_by is null or all fields are null, no plant occurred in this event
            if not planted_by and not pe.get("plant_site") and pe.get("plant_time_in_round") is None:
                continue

            p_puuid = planted_by.get("puuid") if isinstance(planted_by, dict) else None
            p_name = planted_by.get("display_name") if isinstance(planted_by, dict) else None
            p_team = planted_by.get("team") if isinstance(planted_by, dict) else None

            # Location
            loc_raw = pe.get("plant_location") or {}
            plant_loc = {
                "x": float(loc_raw["x"]) if "x" in loc_raw and loc_raw["x"] is not None else None,
                "y": float(loc_raw["y"]) if "y" in loc_raw and loc_raw["y"] is not None else None
            } if loc_raw else None

            # Player locations on plant
            plocs_raw = pe.get("player_locations_on_plant") or []
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

            plant_rows.append({
                "match_id": match_id,
                "round_number": round_number,
                "plant_event_sequence": p_seq,
                "player_puuid": p_puuid,
                "player_display_name": p_name,
                "player_team": p_team,
                "plant_site": pe.get("plant_site"),
                "plant_time_in_round": pe.get("plant_time_in_round"),
                "plant_location": plant_loc,
                "player_locations_on_plant": plocs
            })

print(f"Extracted {len(plant_rows)} rows for bronze_plant_event.")

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

schema_plant = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("plant_event_sequence", IntegerType(), False),
    StructField("player_puuid", StringType(), True),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("plant_site", StringType(), True),
    StructField("plant_time_in_round", IntegerType(), True),
    StructField("plant_location", loc_struct, True),
    StructField("player_locations_on_plant", ArrayType(player_loc_struct), True)
])

df_plant = spark.createDataFrame(plant_rows, schema=schema_plant)
df_plant.createOrReplaceTempView("tv_bronze_plant_event")

spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_plant_event AS source
ON target.match_id = source.match_id
AND target.round_number = source.round_number
AND target.plant_event_sequence = source.plant_event_sequence
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
    COUNT(DISTINCT match_id, round_number, plant_event_sequence) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN round_number < 1 OR round_number IS NULL THEN 1 END) AS invalid_rounds,
    COUNT(CASE WHEN plant_event_sequence IS NULL THEN 1 END) AS null_sequences
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:      {stats['total_rows']}")
print(f"Distinct Keys:   {stats['distinct_keys']}")
print(f"Null Matches:    {stats['null_matches']}")
print(f"Invalid Rounds:  {stats['invalid_rounds']}")
print(f"Null Sequences:  {stats['null_sequences']}")

assert stats["null_matches"] == 0, "DQ ERROR: null match_id found!"
assert stats["invalid_rounds"] == 0, "DQ ERROR: invalid round_number found!"
assert stats["null_sequences"] == 0, "DQ ERROR: null plant_event_sequence found!"
assert stats["total_rows"] == stats["distinct_keys"], "DQ ERROR: duplicate composite key found!"

# Source count reconciliation
expected_plants = 0
for m in unique_matches.values():
    for r in m.get("rounds", []):
        pe = r.get("plant_events")
        if pe:
            evts = pe if isinstance(pe, list) else [pe]
            for e in evts:
                if isinstance(e, dict) and (e.get("planted_by") or e.get("plant_site") or e.get("plant_time_in_round") is not None):
                    expected_plants += 1

assert stats["total_rows"] == expected_plants, f"Source reconciliation failed! Expected {expected_plants}, found {stats['total_rows']}"
print(f"Reconciliation SUCCESS: Exact match with source plant events count ({expected_plants} rows).")

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 15"))
