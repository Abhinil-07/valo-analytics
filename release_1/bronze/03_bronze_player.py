# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_player`
# MAGIC 
# MAGIC - **Grain:** 1 row per player per match
# MAGIC - **Composite Key:** `match_id` + `player_puuid`
# MAGIC - **Source:** `data[].players.all_players[]` (authoritative player collection)
# MAGIC - **Target:** `<catalog>.<schema>.bronze_player`

# COMMAND ----------
import os
import glob
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "bronze"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/data/matches"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_player"

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
    player_puuid STRING NOT NULL,
    player_name STRING,
    player_tag STRING,
    team STRING,
    level INT,
    character STRING,
    current_tier INT,
    current_tier_patched STRING,
    player_card STRING,
    player_title STRING,
    party_id STRING,
    session_playtime_minutes INT,
    session_playtime_seconds INT,
    session_playtime_milliseconds INT,
    afk_rounds DOUBLE,
    friendly_fire_incoming DOUBLE,
    friendly_fire_outgoing DOUBLE,
    rounds_in_spawn DOUBLE,
    platform_type STRING,
    os_name STRING,
    os_version STRING,
    x_casts INT,
    e_casts INT,
    q_casts INT,
    c_casts INT,
    score INT,
    kills INT,
    deaths INT,
    assists INT,
    bodyshots INT,
    headshots INT,
    legshots INT,
    spent_overall INT,
    spent_average DOUBLE,
    loadout_value_overall INT,
    loadout_value_average DOUBLE,
    damage_made INT,
    damage_received INT
)
USING DELTA
COMMENT 'Source-aligned player performance: 1 row per player per match'
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
# MAGIC ### Step 3: Extract `bronze_player` Entities

# COMMAND ----------
player_rows = []
for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    all_players = (m.get("players") or {}).get("all_players") or []
    for p in all_players:
        puuid = p.get("puuid")
        if not puuid:
            continue

        session_pt = p.get("session_playtime") or {}
        behavior = p.get("behavior") or {}
        friendly_fire = behavior.get("friendly_fire") or {}
        platform = p.get("platform") or {}
        os_info = platform.get("os") or {}
        ability_casts = p.get("ability_casts") or {}
        stats = p.get("stats") or {}
        economy = p.get("economy") or {}
        spent = economy.get("spent") or {}
        loadout_val = economy.get("loadout_value") or {}

        def get_cast(key_singular: str, key_plural: str):
            val = ability_casts.get(key_singular)
            return val if val is not None else ability_casts.get(key_plural)

        player_rows.append({
            "match_id": match_id,
            "player_puuid": puuid,
            "player_name": p.get("name"),
            "player_tag": p.get("tag"),
            "team": p.get("team"),
            "level": p.get("level"),
            "character": p.get("character"),
            "current_tier": p.get("currenttier"),
            "current_tier_patched": p.get("currenttier_patched"),
            "player_card": p.get("player_card"),
            "player_title": p.get("player_title"),
            "party_id": p.get("party_id"),
            "session_playtime_minutes": session_pt.get("minutes"),
            "session_playtime_seconds": session_pt.get("seconds"),
            "session_playtime_milliseconds": session_pt.get("milliseconds"),
            "afk_rounds": float(behavior.get("afk_rounds")) if behavior.get("afk_rounds") is not None else None,
            "friendly_fire_incoming": float(friendly_fire.get("incoming")) if friendly_fire.get("incoming") is not None else None,
            "friendly_fire_outgoing": float(friendly_fire.get("outgoing")) if friendly_fire.get("outgoing") is not None else None,
            "rounds_in_spawn": float(behavior.get("rounds_in_spawn")) if behavior.get("rounds_in_spawn") is not None else None,
            "platform_type": platform.get("type"),
            "os_name": os_info.get("name"),
            "os_version": os_info.get("version"),
            "x_casts": get_cast("x_cast", "x_casts"),
            "e_casts": get_cast("e_cast", "e_casts"),
            "q_casts": get_cast("q_cast", "q_casts"),
            "c_casts": get_cast("c_cast", "c_casts"),
            "score": stats.get("score"),
            "kills": stats.get("kills"),
            "deaths": stats.get("deaths"),
            "assists": stats.get("assists"),
            "bodyshots": stats.get("bodyshots"),
            "headshots": stats.get("headshots"),
            "legshots": stats.get("legshots"),
            "spent_overall": spent.get("overall"),
            "spent_average": float(spent.get("average")) if spent.get("average") is not None else None,
            "loadout_value_overall": loadout_val.get("overall"),
            "loadout_value_average": float(loadout_val.get("average")) if loadout_val.get("average") is not None else None,
            "damage_made": p.get("damage_made"),
            "damage_received": p.get("damage_received")
        })

schema_player = StructType([
    StructField("match_id", StringType(), False),
    StructField("player_puuid", StringType(), False),
    StructField("player_name", StringType(), True),
    StructField("player_tag", StringType(), True),
    StructField("team", StringType(), True),
    StructField("level", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("current_tier", IntegerType(), True),
    StructField("current_tier_patched", StringType(), True),
    StructField("player_card", StringType(), True),
    StructField("player_title", StringType(), True),
    StructField("party_id", StringType(), True),
    StructField("session_playtime_minutes", IntegerType(), True),
    StructField("session_playtime_seconds", IntegerType(), True),
    StructField("session_playtime_milliseconds", IntegerType(), True),
    StructField("afk_rounds", DoubleType(), True),
    StructField("friendly_fire_incoming", DoubleType(), True),
    StructField("friendly_fire_outgoing", DoubleType(), True),
    StructField("rounds_in_spawn", DoubleType(), True),
    StructField("platform_type", StringType(), True),
    StructField("os_name", StringType(), True),
    StructField("os_version", StringType(), True),
    StructField("x_casts", IntegerType(), True),
    StructField("e_casts", IntegerType(), True),
    StructField("q_casts", IntegerType(), True),
    StructField("c_casts", IntegerType(), True),
    StructField("score", IntegerType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("bodyshots", IntegerType(), True),
    StructField("headshots", IntegerType(), True),
    StructField("legshots", IntegerType(), True),
    StructField("spent_overall", IntegerType(), True),
    StructField("spent_average", DoubleType(), True),
    StructField("loadout_value_overall", IntegerType(), True),
    StructField("loadout_value_average", DoubleType(), True),
    StructField("damage_made", IntegerType(), True),
    StructField("damage_received", IntegerType(), True)
])

df_player = spark.createDataFrame(player_rows, schema=schema_player)
df_player.createOrReplaceTempView("tv_bronze_player")
print(f"Extracted {len(player_rows)} rows for bronze_player.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Delta MERGE (Idempotent Upsert)

# COMMAND ----------
spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_player AS source
ON target.match_id = source.match_id 
AND target.player_puuid = source.player_puuid
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
    COUNT(DISTINCT match_id, player_puuid) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN player_puuid IS NULL THEN 1 END) AS null_puuids
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:     {stats['total_rows']}")
print(f"Distinct Keys:  {stats['distinct_keys']}")
print(f"Null Matches:   {stats['null_matches']}")
print(f"Null PUUIDs:    {stats['null_puuids']}")

assert stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found!"
assert stats["null_puuids"] == 0, "DATA QUALITY ERROR: null player_puuid found!"
assert stats["total_rows"] == stats["distinct_keys"], "DATA QUALITY ERROR: duplicate (match_id, player_puuid) found!"

display(spark.sql(f"SELECT match_id, player_name, player_tag, team, character, kills, deaths, assists, damage_made FROM {TARGET_TABLE} LIMIT 10"))
