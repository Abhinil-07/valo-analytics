# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_kill_event`
# MAGIC 
# MAGIC - **Grain:** 1 row per kill event
# MAGIC - **Composite Key:** `match_id` + `kill_event_sequence`
# MAGIC - **Source:** top-level `kills[]`
# MAGIC - **Target:** `<catalog>.<schema>.bronze_kill_event`
# MAGIC - **Sequence Rule:** `kill_event_sequence = kills[] 0-based array position`

# COMMAND ----------
import os
import glob
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    BooleanType, DoubleType, ArrayType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "bronze"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/data/matches"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_kill_event"

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
    kill_event_sequence INT NOT NULL,
    round_number INT,
    kill_time_in_round INT,
    kill_time_in_match INT,
    killer_puuid STRING,
    killer_display_name STRING,
    killer_team STRING,
    victim_puuid STRING,
    victim_display_name STRING,
    victim_team STRING,
    victim_death_location STRUCT<x: DOUBLE, y: DOUBLE>,
    damage_weapon_id STRING,
    damage_weapon_name STRING,
    secondary_fire_mode BOOLEAN,
    assist_puuids ARRAY<STRING>,
    assistants ARRAY<STRUCT<assistant_puuid: STRING, assistant_display_name: STRING, assistant_team: STRING>>,
    player_locations ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>>
)
USING DELTA
COMMENT 'Source-aligned kill events from canonical kills[] array'
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

print(f"Loaded {len(unique_matches)} unique match(es) for bronze_kill_event.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract `bronze_kill_event` Entities

# COMMAND ----------
kill_rows = []

for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    kills_list = m.get("kills") or []
    for k_seq, k in enumerate(kills_list):
        # Derive round_number from 0-based round field (+ 1)
        r_field = k.get("round")
        round_number = (int(r_field) + 1) if r_field is not None else None

        # assistants and assist_puuids
        assistants_raw = k.get("assistants") or []
        assist_puuids = [
            a.get("assistant_puuid")
            for a in assistants_raw
            if isinstance(a, dict) and a.get("assistant_puuid")
        ]
        assistants_structs = [
            {
                "assistant_puuid": a.get("assistant_puuid"),
                "assistant_display_name": a.get("assistant_display_name"),
                "assistant_team": a.get("assistant_team")
            }
            for a in assistants_raw if isinstance(a, dict)
        ]

        # player locations on kill
        player_locs_raw = k.get("player_locations_on_kill") or []
        player_locs = []
        for pl in player_locs_raw:
            if isinstance(pl, dict):
                loc = pl.get("location") or {}
                player_locs.append({
                    "player_puuid": pl.get("player_puuid"),
                    "player_display_name": pl.get("player_display_name"),
                    "player_team": pl.get("player_team"),
                    "location": {
                        "x": float(loc["x"]) if "x" in loc and loc["x"] is not None else None,
                        "y": float(loc["y"]) if "y" in loc and loc["y"] is not None else None
                    },
                    "view_radians": float(pl["view_radians"]) if "view_radians" in pl and pl["view_radians"] is not None else None
                })

        # victim death location
        v_loc = k.get("victim_death_location") or {}
        death_loc = {
            "x": float(v_loc["x"]) if "x" in v_loc and v_loc["x"] is not None else None,
            "y": float(v_loc["y"]) if "y" in v_loc and v_loc["y"] is not None else None
        } if v_loc else None

        kill_rows.append({
            "match_id": match_id,
            "kill_event_sequence": k_seq,
            "round_number": round_number,
            "kill_time_in_round": k.get("kill_time_in_round"),
            "kill_time_in_match": k.get("kill_time_in_match"),
            "killer_puuid": k.get("killer_puuid"),
            "killer_display_name": k.get("killer_display_name"),
            "killer_team": k.get("killer_team"),
            "victim_puuid": k.get("victim_puuid"),
            "victim_display_name": k.get("victim_display_name"),
            "victim_team": k.get("victim_team"),
            "victim_death_location": death_loc,
            "damage_weapon_id": k.get("damage_weapon_id"),
            "damage_weapon_name": k.get("damage_weapon_name"),
            "secondary_fire_mode": k.get("secondary_fire_mode"),
            "assist_puuids": assist_puuids,
            "assistants": assistants_structs,
            "player_locations": player_locs
        })

print(f"Extracted {len(kill_rows)} rows for bronze_kill_event.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Create DataFrame & Delta MERGE (Idempotent Upsert)

# COMMAND ----------
loc_struct = StructType([
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True)
])

assistant_struct = StructType([
    StructField("assistant_puuid", StringType(), True),
    StructField("assistant_display_name", StringType(), True),
    StructField("assistant_team", StringType(), True)
])

player_loc_struct = StructType([
    StructField("player_puuid", StringType(), True),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("location", loc_struct, True),
    StructField("view_radians", DoubleType(), True)
])

schema_kill = StructType([
    StructField("match_id", StringType(), False),
    StructField("kill_event_sequence", IntegerType(), False),
    StructField("round_number", IntegerType(), True),
    StructField("kill_time_in_round", IntegerType(), True),
    StructField("kill_time_in_match", IntegerType(), True),
    StructField("killer_puuid", StringType(), True),
    StructField("killer_display_name", StringType(), True),
    StructField("killer_team", StringType(), True),
    StructField("victim_puuid", StringType(), True),
    StructField("victim_display_name", StringType(), True),
    StructField("victim_team", StringType(), True),
    StructField("victim_death_location", loc_struct, True),
    StructField("damage_weapon_id", StringType(), True),
    StructField("damage_weapon_name", StringType(), True),
    StructField("secondary_fire_mode", BooleanType(), True),
    StructField("assist_puuids", ArrayType(StringType()), True),
    StructField("assistants", ArrayType(assistant_struct), True),
    StructField("player_locations", ArrayType(player_loc_struct), True)
])

df_kill = spark.createDataFrame(kill_rows, schema=schema_kill)
df_kill.createOrReplaceTempView("tv_bronze_kill_event")

spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_kill_event AS source
ON target.match_id = source.match_id
AND target.kill_event_sequence = source.kill_event_sequence
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
    COUNT(DISTINCT match_id, kill_event_sequence) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN kill_event_sequence IS NULL THEN 1 END) AS null_sequences
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:      {stats['total_rows']}")
print(f"Distinct Keys:   {stats['distinct_keys']}")
print(f"Null Matches:    {stats['null_matches']}")
print(f"Null Sequences:  {stats['null_sequences']}")

assert stats["null_matches"] == 0, "DQ ERROR: null match_id found!"
assert stats["null_sequences"] == 0, "DQ ERROR: null kill_event_sequence found!"
assert stats["total_rows"] == stats["distinct_keys"], "DQ ERROR: duplicate composite key found!"

# Source count reconciliation
expected_kills_count = sum(len(m.get("kills", [])) for m in unique_matches.values())
assert stats["total_rows"] == expected_kills_count, f"Source reconciliation failed! Expected {expected_kills_count}, found {stats['total_rows']}"
print(f"Reconciliation SUCCESS: Exact match with source kills count ({expected_kills_count} rows).")

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 15"))
