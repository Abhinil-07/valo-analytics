# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Release 1: Bronze Layer Ingestion
# MAGIC 
# MAGIC Ingests raw match JSONs from Landing Volume into the first four Bronze Delta tables:
# MAGIC 1. **`bronze_match`** (Grain: 1 row per match, Key: `match_id`)
# MAGIC 2. **`bronze_team`** (Grain: 1 row per team per match, Composite Key: `match_id` + `team_side`)
# MAGIC 3. **`bronze_player`** (Grain: 1 row per player per match, Composite Key: `match_id` + `player_puuid`)
# MAGIC 4. **`bronze_round`** (Grain: 1 row per round per match, Composite Key: `match_id` + `round_number`)
# MAGIC 
# MAGIC All writes use **Delta MERGE** for strict idempotency.

# COMMAND ----------
import os
import glob
import json
from typing import Any, Dict, List, Optional
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, BooleanType, DoubleType
)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CATALOG = "valorant"
BRONZE_SCHEMA = "bronze"
LANDING_SCHEMA = "landing"
LANDING_VOLUME = "data"

# Volume path where landing JSONs reside
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/matches"

print(f"Catalog:        {CATALOG}")
print(f"Bronze Schema:  {BRONZE_SCHEMA}")
print(f"Landing Volume: {LANDING_VOLUME_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Bronze Schema & Delta Tables Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# 1. bronze_match
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_match (
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
COMMENT 'Raw source-aligned match metadata: 1 row per match'
""")

# 2. bronze_team
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_team (
    match_id STRING NOT NULL,
    team_side STRING NOT NULL,
    has_won BOOLEAN,
    rounds_won INT,
    rounds_lost INT
)
USING DELTA
COMMENT 'Raw source-aligned team performance: 1 row per team per match'
""")

# 3. bronze_player
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_player (
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
COMMENT 'Raw source-aligned player stats: 1 row per player per match'
""")

# 4. bronze_round
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_round (
    match_id STRING NOT NULL,
    round_number INT NOT NULL,
    winning_team STRING,
    end_type STRING,
    bomb_planted BOOLEAN,
    bomb_defused BOOLEAN
)
USING DELTA
COMMENT 'Raw source-aligned round summaries: 1 row per round per match'
""")

print("Successfully verified all 4 Bronze Delta table DDLs.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Load Landing Match JSONs

# COMMAND ----------
# Discover all match JSON files from landing volume (or fallback to local file if testing)
json_files = glob.glob(f"{LANDING_VOLUME_PATH}/*/*/*/*.json")
if not json_files:
    json_files = glob.glob(f"{LANDING_VOLUME_PATH}/**/*.json", recursive=True)

print(f"Discovered {len(json_files)} landing match file(s).")

matches_data = []
for file_path in json_files:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)
            # Handle both direct match object and envelope {data: [...]}
            if isinstance(content, dict):
                if "data" in content and isinstance(content["data"], list):
                    matches_data.extend(content["data"])
                elif "metadata" in content:
                    matches_data.append(content)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

# Deduplicate matches by matchid if multiple files contain the same match
unique_matches = {}
for m in matches_data:
    mid = (m.get("metadata") or {}).get("matchid")
    if mid and mid not in unique_matches:
        unique_matches[mid] = m

matches_to_process = list(unique_matches.values())
print(f"Loaded {len(matches_to_process)} unique match(es) for Bronze transformation.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract Entities According to Strict Grains

# COMMAND ----------
match_rows = []
team_rows = []
player_rows = []
round_rows = []

for m in matches_to_process:
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    # 1. bronze_match row
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

    # 2. bronze_team rows (Red and Blue)
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

    # 3. bronze_player rows (from all_players[])
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

        # Handle both x_cast and x_casts
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

    # 4. bronze_round rows (0-based index + 1)
    rounds_list = m.get("rounds") or []
    for idx, r in enumerate(rounds_list):
        round_rows.append({
            "match_id": match_id,
            "round_number": idx + 1,
            "winning_team": r.get("winning_team"),
            "end_type": r.get("end_type"),
            "bomb_planted": r.get("bomb_planted"),
            "bomb_defused": r.get("bomb_defused")
        })

print(f"Extracted: {len(match_rows)} match rows, {len(team_rows)} team rows, {len(player_rows)} player rows, {len(round_rows)} round rows.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Define Schemas & Create DataFrames

# COMMAND ----------
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

schema_team = StructType([
    StructField("match_id", StringType(), False),
    StructField("team_side", StringType(), False),
    StructField("has_won", BooleanType(), True),
    StructField("rounds_won", IntegerType(), True),
    StructField("rounds_lost", IntegerType(), True)
])

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

schema_round = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("winning_team", StringType(), True),
    StructField("end_type", StringType(), True),
    StructField("bomb_planted", BooleanType(), True),
    StructField("bomb_defused", BooleanType(), True)
])

df_match = spark.createDataFrame(match_rows, schema=schema_match)
df_team = spark.createDataFrame(team_rows, schema=schema_team)
df_player = spark.createDataFrame(player_rows, schema=schema_player)
df_round = spark.createDataFrame(round_rows, schema=schema_round)

df_match.createOrReplaceTempView("tv_bronze_match")
df_team.createOrReplaceTempView("tv_bronze_team")
df_player.createOrReplaceTempView("tv_bronze_player")
df_round.createOrReplaceTempView("tv_bronze_round")

print("Created temp views with explicit schemas.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Execute Idempotent Delta MERGE for All Four Tables

# COMMAND ----------
# 1. MERGE bronze_match
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_match AS target
USING tv_bronze_match AS source
ON target.match_id = source.match_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")
print("Merged bronze_match successfully.")

# 2. MERGE bronze_team
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_team AS target
USING tv_bronze_team AS source
ON target.match_id = source.match_id 
AND target.team_side = source.team_side
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")
print("Merged bronze_team successfully.")

# 3. MERGE bronze_player
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_player AS target
USING tv_bronze_player AS source
ON target.match_id = source.match_id 
AND target.player_puuid = source.player_puuid
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")
print("Merged bronze_player successfully.")

# 4. MERGE bronze_round
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_round AS target
USING tv_bronze_round AS source
ON target.match_id = source.match_id 
AND target.round_number = source.round_number
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")
print("Merged bronze_round successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Data Quality & Uniqueness Validation Checks

# COMMAND ----------
print("=" * 60)
print("DATA QUALITY AUDIT & ASSERTIONS")
print("=" * 60)

# Check 1: bronze_match (match_id NOT NULL and UNIQUE)
match_stats = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT match_id) AS distinct_matches,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches
FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_match
""").collect()[0]

print(f"bronze_match -> Total: {match_stats['total_rows']}, Distinct: {match_stats['distinct_matches']}, Nulls: {match_stats['null_matches']}")
assert match_stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found in bronze_match"
assert match_stats["total_rows"] == match_stats["distinct_matches"], "DATA QUALITY ERROR: duplicate match_id found in bronze_match"

# Check 2: bronze_team (match_id NOT NULL, team_side IN ('Red', 'Blue'), unique composite key)
team_stats = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT match_id, team_side) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN team_side NOT IN ('Red', 'Blue') THEN 1 END) AS invalid_sides
FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_team
""").collect()[0]

print(f"bronze_team  -> Total: {team_stats['total_rows']}, Distinct: {team_stats['distinct_keys']}, Nulls: {team_stats['null_matches']}, Invalid sides: {team_stats['invalid_sides']}")
assert team_stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found in bronze_team"
assert team_stats["invalid_sides"] == 0, "DATA QUALITY ERROR: invalid team_side found in bronze_team"
assert team_stats["total_rows"] == team_stats["distinct_keys"], "DATA QUALITY ERROR: duplicate (match_id, team_side) found in bronze_team"

# Check 3: bronze_player (match_id NOT NULL, player_puuid NOT NULL, unique composite key)
player_stats = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT match_id, player_puuid) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN player_puuid IS NULL THEN 1 END) AS null_puuids
FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_player
""").collect()[0]

print(f"bronze_player -> Total: {player_stats['total_rows']}, Distinct: {player_stats['distinct_keys']}, Nulls: {player_stats['null_matches']}, Null PUUIDs: {player_stats['null_puuids']}")
assert player_stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found in bronze_player"
assert player_stats["null_puuids"] == 0, "DATA QUALITY ERROR: null player_puuid found in bronze_player"
assert player_stats["total_rows"] == player_stats["distinct_keys"], "DATA QUALITY ERROR: duplicate (match_id, player_puuid) in bronze_player"

# Check 4: bronze_round (match_id NOT NULL, round_number >= 1, unique composite key)
round_stats = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT match_id, round_number) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN round_number < 1 OR round_number IS NULL THEN 1 END) AS invalid_rounds
FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_round
""").collect()[0]

print(f"bronze_round  -> Total: {round_stats['total_rows']}, Distinct: {round_stats['distinct_keys']}, Nulls: {round_stats['null_matches']}, Invalid rounds: {round_stats['invalid_rounds']}")
assert round_stats["null_matches"] == 0, "DATA QUALITY ERROR: null match_id found in bronze_round"
assert round_stats["invalid_rounds"] == 0, "DATA QUALITY ERROR: invalid round_number found in bronze_round"
assert round_stats["total_rows"] == round_stats["distinct_keys"], "DATA QUALITY ERROR: duplicate (match_id, round_number) in bronze_round"

print("\nALL DATA QUALITY ASSERTIONS PASSED SUCCESSFULLY!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 7: Display Sample Rows from Each Table

# COMMAND ----------
print("SAMPLE: bronze_match")
display(spark.sql(f"SELECT match_id, map, game_version, rounds_played, mode, region FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_match LIMIT 5"))

print("SAMPLE: bronze_team")
display(spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_team LIMIT 6"))

print("SAMPLE: bronze_player")
display(spark.sql(f"SELECT match_id, player_name, player_tag, team, character, kills, deaths, assists, damage_made FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_player LIMIT 10"))

print("SAMPLE: bronze_round")
display(spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_round LIMIT 10"))
