# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_round_player_stats`
# MAGIC 
# MAGIC - **Grain:** 1 row per player per round per match
# MAGIC - **Composite Key:** `match_id` + `round_number` + `player_puuid`
# MAGIC - **Source:** `rounds[].player_stats[]`
# MAGIC - **Target:** `<catalog>.<schema>.bronze_round_player_stats`
# MAGIC - **Round Index Rule:** `round_number = rounds[] array position + 1`

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
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_round_player_stats"

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
    player_puuid STRING NOT NULL,
    player_display_name STRING,
    player_team STRING,
    kills INT,
    deaths INT,
    assists INT,
    score INT,
    damage INT,
    bodyshots INT,
    headshots INT,
    legshots INT,
    loadout_value INT,
    remaining_credits INT,
    spent_credits INT,
    weapon_id STRING,
    weapon_name STRING,
    armor_id STRING,
    armor_name STRING,
    c_casts INT,
    q_casts INT,
    e_casts INT,
    x_casts INT,
    was_afk BOOLEAN,
    was_penalized BOOLEAN,
    stayed_in_spawn BOOLEAN,
    was_alive BOOLEAN
)
USING DELTA
COMMENT 'Source-aligned round-level player statistics: 1 row per player per round per match'
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

# Deduplicate by matchid
unique_matches = {}
for m in matches_data:
    mid = (m.get("metadata") or {}).get("matchid")
    if mid and mid not in unique_matches:
        unique_matches[mid] = m

print(f"Loaded {len(unique_matches)} unique match(es) for bronze_round_player_stats.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract `bronze_round_player_stats` Entities

# COMMAND ----------
pstat_rows = []

for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    rounds_list = m.get("rounds") or []
    for r_idx, r in enumerate(rounds_list):
        round_number = r_idx + 1
        pstats = r.get("player_stats") or []

        for ps in pstats:
            puuid = ps.get("player_puuid")
            if not puuid:
                continue

            econ = ps.get("economy") or {}
            weapon = econ.get("weapon") or {}
            armor = econ.get("armor") or {}
            casts = ps.get("ability_casts") or {}

            pstat_rows.append({
                "match_id": match_id,
                "round_number": round_number,
                "player_puuid": puuid,
                "player_display_name": ps.get("player_display_name"),
                "player_team": ps.get("player_team"),
                "kills": ps.get("kills"),
                "deaths": ps.get("deaths"),
                "assists": ps.get("assists"),
                "score": ps.get("score"),
                "damage": ps.get("damage"),
                "bodyshots": ps.get("bodyshots"),
                "headshots": ps.get("headshots"),
                "legshots": ps.get("legshots"),
                "loadout_value": econ.get("loadout_value"),
                "remaining_credits": econ.get("remaining"),
                "spent_credits": econ.get("spent"),
                "weapon_id": weapon.get("id"),
                "weapon_name": weapon.get("name"),
                "armor_id": armor.get("id"),
                "armor_name": armor.get("name"),
                "c_casts": casts.get("c_casts"),
                "q_casts": casts.get("q_casts"),
                "e_casts": casts.get("e_casts"),
                "x_casts": casts.get("x_casts"),
                "was_afk": ps.get("was_afk"),
                "was_penalized": ps.get("was_penalized"),
                "stayed_in_spawn": ps.get("stayed_in_spawn"),
                "was_alive": ps.get("was_alive")
            })

print(f"Extracted {len(pstat_rows)} rows for bronze_round_player_stats.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Create DataFrame & Delta MERGE (Idempotent Upsert)

# COMMAND ----------
schema_pstats = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("player_puuid", StringType(), False),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("score", IntegerType(), True),
    StructField("damage", IntegerType(), True),
    StructField("bodyshots", IntegerType(), True),
    StructField("headshots", IntegerType(), True),
    StructField("legshots", IntegerType(), True),
    StructField("loadout_value", IntegerType(), True),
    StructField("remaining_credits", IntegerType(), True),
    StructField("spent_credits", IntegerType(), True),
    StructField("weapon_id", StringType(), True),
    StructField("weapon_name", StringType(), True),
    StructField("armor_id", StringType(), True),
    StructField("armor_name", StringType(), True),
    StructField("c_casts", IntegerType(), True),
    StructField("q_casts", IntegerType(), True),
    StructField("e_casts", IntegerType(), True),
    StructField("x_casts", IntegerType(), True),
    StructField("was_afk", BooleanType(), True),
    StructField("was_penalized", BooleanType(), True),
    StructField("stayed_in_spawn", BooleanType(), True),
    StructField("was_alive", BooleanType(), True)
])

df_pstats = spark.createDataFrame(pstat_rows, schema=schema_pstats)
df_pstats.createOrReplaceTempView("tv_bronze_round_player_stats")

spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_round_player_stats AS source
ON target.match_id = source.match_id
AND target.round_number = source.round_number
AND target.player_puuid = source.player_puuid
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
    COUNT(DISTINCT match_id, round_number, player_puuid) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN round_number < 1 OR round_number IS NULL THEN 1 END) AS invalid_rounds,
    COUNT(CASE WHEN player_puuid IS NULL THEN 1 END) AS null_puuids
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:      {stats['total_rows']}")
print(f"Distinct Keys:   {stats['distinct_keys']}")
print(f"Null Matches:    {stats['null_matches']}")
print(f"Invalid Rounds:  {stats['invalid_rounds']}")
print(f"Null PUUIDs:     {stats['null_puuids']}")

assert stats["null_matches"] == 0, "DQ ERROR: null match_id found!"
assert stats["invalid_rounds"] == 0, "DQ ERROR: invalid round_number found!"
assert stats["null_puuids"] == 0, "DQ ERROR: null player_puuid found!"
assert stats["total_rows"] == stats["distinct_keys"], "DQ ERROR: duplicate composite key found!"

# Source count reconciliation
expected_pstat_count = sum(
    len(r.get("player_stats", []))
    for m in unique_matches.values()
    for r in m.get("rounds", [])
)
assert stats["total_rows"] == expected_pstat_count, f"Source reconciliation failed! Expected {expected_pstat_count}, found {stats['total_rows']}"
print(f"Reconciliation SUCCESS: Exact match with source player_stats count ({expected_pstat_count} rows).")

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 15"))
