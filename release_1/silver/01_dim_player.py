# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Dimension: `dim_player`
# MAGIC 
# MAGIC - **Catalog:** `valorant`
# MAGIC - **Schema:** `silver`
# MAGIC - **Target Table:** `valorant.silver.dim_player`
# MAGIC - **Grain:** 1 row per unique player (`player_puuid`)
# MAGIC - **Primary Key:** `player_puuid`
# MAGIC - **Purpose:** Master player directory providing stable player identity (SRS Section 25 & 28),
# MAGIC   maintaining most recent display names, account levels, and dynamically joining with `dim_team_roster`
# MAGIC   to set `is_core_team`.
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `player_puuid`)

# COMMAND ----------
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration
CATALOG = "valorant"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

SOURCE_PLAYER = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_player"
SOURCE_MATCH = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_match"
SOURCE_ROSTER = f"{CATALOG}.{SILVER_SCHEMA}.dim_team_roster"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_player"

print(f"Source Player: {SOURCE_PLAYER}")
print(f"Source Match:  {SOURCE_MATCH}")
print(f"Source Roster: {SOURCE_ROSTER}")
print(f"Target Table:  {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Silver Schema and `dim_player` Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    player_puuid STRING NOT NULL,
    current_display_name STRING,
    player_name STRING,
    player_tag STRING,
    account_level INT,
    platform_type STRING,
    is_core_team BOOLEAN NOT NULL,
    first_seen_timestamp TIMESTAMP,
    last_seen_timestamp TIMESTAMP,
    total_matches_recorded INT,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Master player dimension with stable identity, latest aliases, and core roster flag'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Candidates and Derive Attributes
# MAGIC 
# MAGIC 1. Joins `bronze_player` with `bronze_match` to retrieve match timestamps.
# MAGIC 2. Joins with `dim_team_roster` to dynamically assign `is_core_team`.
# MAGIC 3. Finds the latest known display name and aggregates match count.

# COMMAND ----------
# Fetch active core roster PUUIDs dynamically from dim_team_roster
try:
    roster_df = spark.table(SOURCE_ROSTER).filter(F.col("is_active_roster") == True)
    core_puuids = [row["player_puuid"] for row in roster_df.select("player_puuid").collect()]
except Exception as e:
    print(f"Warning: Could not read {SOURCE_ROSTER} directly ({e}). Using empty roster fallback.")
    core_puuids = []

print(f"Active Core Roster PUUIDs loaded: {len(core_puuids)}")

bronze_player_df = spark.table(SOURCE_PLAYER)
bronze_match_df = spark.table(SOURCE_MATCH).select(
    "match_id",
    F.to_timestamp(F.from_unixtime(F.col("game_start"))).alias("match_start_ts")
)

player_matches_df = bronze_player_df.join(
    bronze_match_df,
    on="match_id",
    how="inner"
).filter(F.col("player_puuid").isNotNull())

latest_appearance_window = Window.partitionBy("player_puuid").orderBy(
    F.col("match_start_ts").desc_nulls_last()
)
player_agg_window = Window.partitionBy("player_puuid")

staged_players_df = player_matches_df.withColumn(
    "row_num", F.row_number().over(latest_appearance_window)
).withColumn(
    "first_seen", F.min("match_start_ts").over(player_agg_window)
).withColumn(
    "last_seen", F.max("match_start_ts").over(player_agg_window)
).withColumn(
    "match_cnt", F.count("match_id").over(player_agg_window)
).filter(
    F.col("row_num") == 1
).select(
    F.col("player_puuid"),
    F.concat_ws("#", F.col("player_name"), F.col("player_tag")).alias("current_display_name"),
    F.col("player_name"),
    F.col("player_tag"),
    F.col("level").alias("account_level"),
    F.col("platform_type"),
    F.col("player_puuid").isin(core_puuids).alias("is_core_team"),
    F.col("first_seen").alias("first_seen_timestamp"),
    F.col("last_seen").alias("last_seen_timestamp"),
    F.col("match_cnt").alias("total_matches_recorded"),
    F.current_timestamp().alias("updated_at")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `dim_player`

# COMMAND ----------
staged_players_df.createOrReplaceTempView("staged_dim_player")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_dim_player AS source
ON target.player_puuid = source.player_puuid
WHEN MATCHED AND (
    source.last_seen_timestamp > target.last_seen_timestamp
    OR target.is_core_team != source.is_core_team
    OR target.last_seen_timestamp < '2020-01-01'
    OR target.last_seen_timestamp IS NULL
) THEN
  UPDATE SET
    target.current_display_name = source.current_display_name,
    target.player_name = source.player_name,
    target.player_tag = source.player_tag,
    target.account_level = source.account_level,
    target.platform_type = source.platform_type,
    target.is_core_team = source.is_core_team,
    target.first_seen_timestamp = CASE 
      WHEN target.first_seen_timestamp < '2020-01-01' THEN source.first_seen_timestamp
      ELSE LEAST(COALESCE(target.first_seen_timestamp, source.first_seen_timestamp), source.first_seen_timestamp)
    END,
    target.last_seen_timestamp = CASE 
      WHEN target.last_seen_timestamp < '2020-01-01' THEN source.last_seen_timestamp
      ELSE GREATEST(COALESCE(target.last_seen_timestamp, source.last_seen_timestamp), source.last_seen_timestamp)
    END,
    target.total_matches_recorded = source.total_matches_recorded,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    player_puuid,
    current_display_name,
    player_name,
    player_tag,
    account_level,
    platform_type,
    is_core_team,
    first_seen_timestamp,
    last_seen_timestamp,
    total_matches_recorded,
    updated_at
  )
  VALUES (
    source.player_puuid,
    source.current_display_name,
    source.player_name,
    source.player_tag,
    source.account_level,
    source.platform_type,
    source.is_core_team,
    source.first_seen_timestamp,
    source.last_seen_timestamp,
    source.total_matches_recorded,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification & Summary

# COMMAND ----------
total_players = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
core_team_players = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE is_core_team = true").collect()[0][0]

print("=== dim_player Summary ===")
print(f"Total unique players in directory: {total_players}")
print(f"Core roster players: {core_team_players}")

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY is_core_team DESC, total_matches_recorded DESC LIMIT 10"))
