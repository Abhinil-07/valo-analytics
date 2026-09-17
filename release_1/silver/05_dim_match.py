# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Dimension: `dim_match`
# MAGIC 
# MAGIC - **Catalog:** `valorant`
# MAGIC - **Schema:** `silver`
# MAGIC - **Target Table:** `valorant.silver.dim_match`
# MAGIC - **Grain:** 1 row per unique match
# MAGIC - **Primary Key:** `match_id`
# MAGIC - **Purpose:** Canonical match dimension providing clean dates, durations, and team perspective
# MAGIC   outcomes (`our_team_side`, `our_team_rounds_won`, `opponent_rounds_won`, `match_outcome`)
# MAGIC   driven dynamically by `dim_team_roster` (SRS Section 2, 24 & 27).
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id`)

# COMMAND ----------
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration
CATALOG = "valorant"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

SOURCE_MATCH = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_match"
SOURCE_TEAM = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_team"
SOURCE_PLAYER = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_player"
SOURCE_ROSTER = f"{CATALOG}.{SILVER_SCHEMA}.dim_team_roster"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_match"

# Fallback Roster PUUIDs if dim_team_roster table is not yet populated
DEFAULT_ROSTER_PUUIDS = [
    "c38ffa2e-ce9d-5d95-9399-3a89c6af6b16",  # Agamemnon (Captain)
    "59bae8f3-025c-5dcc-9a1c-c903279e4145",  # systemctl
    "f91099e8-a14b-5913-b66b-13717562a6eb",  # NoSheat
    "9ac37245-e47a-5977-9785-7c2590e2dcda",  # SC4R
    "5775df6c-0f15-5e6c-8f00-3398dc77d351",  # Garamhe
    "663569fe-cd21-52b8-abc0-f9596fc7d1bf"   # Ned
]

print(f"Source Match:  {SOURCE_MATCH}")
print(f"Source Team:   {SOURCE_TEAM}")
print(f"Source Player: {SOURCE_PLAYER}")
print(f"Source Roster: {SOURCE_ROSTER}")
print(f"Target Table:  {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Silver Schema and `dim_match` Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    match_id STRING NOT NULL,
    match_date DATE NOT NULL,
    match_start_timestamp TIMESTAMP NOT NULL,
    map_name STRING NOT NULL,
    game_duration_seconds INT,
    game_duration_minutes DOUBLE,
    rounds_played INT NOT NULL,
    is_overtime BOOLEAN NOT NULL,
    our_team_color STRING,
    our_team_rounds_won INT,
    opponent_rounds_won INT,
    round_differential INT,
    match_outcome STRING,
    is_our_team_win BOOLEAN,
    server_cluster STRING,
    game_version STRING,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Canonical match dimension with match duration, score, and automated our-team outcome'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Match Metadata and Resolve Team Perspective
# MAGIC 
# MAGIC 1. Match start epoch seconds -> UTC timestamp and calendar Date.
# MAGIC 2. Cross-reference `bronze_player` with roster to identify which team color (`Red` or `Blue`) our squad was on.
# MAGIC 3. Join with `bronze_team` to get rounds won/lost from our team's perspective.

# COMMAND ----------
# 1. Base Match Metadata (game_start is already in unix seconds)
match_df = spark.table(SOURCE_MATCH).select(
    F.col("match_id"),
    F.to_timestamp(F.from_unixtime(F.col("game_start"))).alias("match_start_timestamp"),
    F.to_date(F.to_timestamp(F.from_unixtime(F.col("game_start")))).alias("match_date"),
    F.col("map").alias("map_name"),
    F.col("game_length").alias("game_duration_seconds"),
    F.round(F.col("game_length") / 60.0, 1).alias("game_duration_minutes"),
    F.col("rounds_played"),
    (F.col("rounds_played") > 24).alias("is_overtime"),
    F.col("cluster").alias("server_cluster"),
    F.col("game_version")
)

# 2. Derive our_team_side from roster
try:
    roster_df = spark.table(SOURCE_ROSTER).filter(F.col("is_active_roster") == True).select(
        "player_puuid", "is_team_owner"
    )
    if roster_df.count() == 0:
        raise ValueError("Roster table is empty")
except Exception as e:
    print(f"Using default roster fallback: {e}")
    roster_df = spark.createDataFrame(
        [(p, p == "c38ffa2e-ce9d-5d95-9399-3a89c6af6b16") for p in DEFAULT_ROSTER_PUUIDS],
        ["player_puuid", "is_team_owner"]
    )

player_df = spark.table(SOURCE_PLAYER).select("match_id", "player_puuid", "team")

# Join players in match with roster to find which side our members were on
roster_matches_df = player_df.join(
    roster_df,
    on="player_puuid",
    how="inner"
)

# Window: Prefer captain (Agamemnon), else count of roster members on that side
roster_side_window = Window.partitionBy("match_id").orderBy(
    F.col("is_team_owner").desc(),
    F.col("player_puuid")
)

our_side_df = roster_matches_df.withColumn(
    "priority_rank", F.row_number().over(roster_side_window)
).filter(
    F.col("priority_rank") == 1
).select(
    F.col("match_id"),
    F.initcap(F.col("team")).alias("our_team_color")
)

# 3. Pivot team scores (Red and Blue) from bronze_team
team_df = spark.table(SOURCE_TEAM).select("match_id", "team_side", "rounds_won")

red_team_df = team_df.filter(F.lower(F.col("team_side")) == "red").select(
    F.col("match_id"),
    F.col("rounds_won").alias("red_rounds_won")
)

blue_team_df = team_df.filter(F.lower(F.col("team_side")) == "blue").select(
    F.col("match_id"),
    F.col("rounds_won").alias("blue_rounds_won")
)

# 4. Assemble Candidate Records
staged_dim_match_df = match_df.join(
    our_side_df, on="match_id", how="left"
).join(
    red_team_df, on="match_id", how="left"
).join(
    blue_team_df, on="match_id", how="left"
).withColumn(
    "our_team_rounds_won",
    F.when(F.col("our_team_color") == "Red", F.col("red_rounds_won"))
     .when(F.col("our_team_color") == "Blue", F.col("blue_rounds_won"))
     .otherwise(None)
).withColumn(
    "opponent_rounds_won",
    F.when(F.col("our_team_color") == "Red", F.col("blue_rounds_won"))
     .when(F.col("our_team_color") == "Blue", F.col("red_rounds_won"))
     .otherwise(None)
).withColumn(
    "round_differential",
    F.col("our_team_rounds_won") - F.col("opponent_rounds_won")
).withColumn(
    "match_outcome",
    F.when(F.col("our_team_rounds_won") > F.col("opponent_rounds_won"), "VICTORY")
     .when(F.col("our_team_rounds_won") < F.col("opponent_rounds_won"), "DEFEAT")
     .when(F.col("our_team_rounds_won") == F.col("opponent_rounds_won"), "DRAW")
     .otherwise("UNKNOWN")
).withColumn(
    "is_our_team_win",
    F.when(F.col("match_outcome") == "VICTORY", True)
     .when(F.col("match_outcome") == "DEFEAT", False)
     .otherwise(None)
).select(
    "match_id",
    "match_date",
    "match_start_timestamp",
    "map_name",
    "game_duration_seconds",
    "game_duration_minutes",
    "rounds_played",
    "is_overtime",
    "our_team_color",
    "our_team_rounds_won",
    "opponent_rounds_won",
    "round_differential",
    "match_outcome",
    "is_our_team_win",
    "server_cluster",
    "game_version",
    F.current_timestamp().alias("updated_at")
)

staged_dim_match_df.createOrReplaceTempView("staged_dim_match")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `dim_match`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_dim_match AS source
ON target.match_id = source.match_id
WHEN MATCHED THEN
  UPDATE SET
    target.match_date = source.match_date,
    target.match_start_timestamp = source.match_start_timestamp,
    target.map_name = source.map_name,
    target.game_duration_seconds = source.game_duration_seconds,
    target.game_duration_minutes = source.game_duration_minutes,
    target.rounds_played = source.rounds_played,
    target.is_overtime = source.is_overtime,
    target.our_team_color = source.our_team_color,
    target.our_team_rounds_won = source.our_team_rounds_won,
    target.opponent_rounds_won = source.opponent_rounds_won,
    target.round_differential = source.round_differential,
    target.match_outcome = source.match_outcome,
    target.is_our_team_win = source.is_our_team_win,
    target.server_cluster = source.server_cluster,
    target.game_version = source.game_version,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id,
    match_date,
    match_start_timestamp,
    map_name,
    game_duration_seconds,
    game_duration_minutes,
    rounds_played,
    is_overtime,
    our_team_color,
    our_team_rounds_won,
    opponent_rounds_won,
    round_differential,
    match_outcome,
    is_our_team_win,
    server_cluster,
    game_version,
    updated_at
  )
  VALUES (
    source.match_id,
    source.match_date,
    source.match_start_timestamp,
    source.map_name,
    source.game_duration_seconds,
    source.game_duration_minutes,
    source.rounds_played,
    source.is_overtime,
    source.our_team_color,
    source.our_team_rounds_won,
    source.opponent_rounds_won,
    source.round_differential,
    source.match_outcome,
    source.is_our_team_win,
    source.server_cluster,
    source.game_version,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
spark.sql(merge_sql).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification & Summary

# COMMAND ----------
total_matches = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
victories = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE match_outcome = 'VICTORY'").collect()[0][0]
defeats = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE match_outcome = 'DEFEAT'").collect()[0][0]

print(f"=== dim_match Summary ===")
print(f"Total matches registered: {total_matches}")
print(f"Victories: {victories} | Defeats: {defeats}")

display(spark.sql(f"""
    SELECT 
        match_id, 
        match_date, 
        map_name, 
        our_team_color,
        CONCAT(our_team_rounds_won, ' - ', opponent_rounds_won) AS score, 
        match_outcome, 
        round_differential
    FROM {TARGET_TABLE}
    ORDER BY match_start_timestamp DESC
"""))
