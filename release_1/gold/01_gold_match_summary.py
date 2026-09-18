# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_match_summary`
# MAGIC 
# MAGIC - **Grain:** 1 row per match (211+ rows)
# MAGIC - **Primary Key:** `match_id`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.silver.dim_match` (Match duration, scores, server cluster, dates)
# MAGIC   - `valorant.silver.dim_map` (Map splash art and minimap radar URLs)
# MAGIC   - `valorant.silver.fact_round` (Attack/Defense round counts and Thrifty conversions)
# MAGIC   - `valorant.silver.fact_match_player` (Team combat aggregates and Match MVP resolution)
# MAGIC - **Target:** `valorant.gold.gold_match_summary`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Executive Overview:** Instant high-level win/loss summary cards.
# MAGIC - **Match History Table:** Complete match overview with scores, duration, MVP, and map art.
# MAGIC - **Attack vs Defense Diagnostics:** Pre-aggregated attack and defense win rates per match.

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType, DateType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "gold"
SOURCE_DIM_MATCH         = f"{CATALOG}.silver.dim_match"
SOURCE_DIM_MAP           = f"{CATALOG}.silver.dim_map"
SOURCE_FACT_ROUND        = f"{CATALOG}.silver.fact_round"
SOURCE_FACT_MATCH_PLAYER = f"{CATALOG}.silver.fact_match_player"
TARGET_TABLE             = f"{CATALOG}.{SCHEMA}.gold_match_summary"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_DIM_MATCH}, {SOURCE_FACT_ROUND}, {SOURCE_FACT_MATCH_PLAYER}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Gold Catalog, Schema, and Table Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    match_id STRING NOT NULL,
    match_date DATE NOT NULL,
    match_start_timestamp TIMESTAMP NOT NULL,
    map_name STRING NOT NULL,
    map_splash_url STRING,
    game_duration_seconds INT,
    game_duration_minutes DOUBLE,
    rounds_played INT NOT NULL,
    is_overtime BOOLEAN NOT NULL,
    our_team_color STRING,
    our_team_rounds_won INT,
    opponent_rounds_won INT,
    score_display STRING,
    round_differential INT,
    match_outcome STRING,
    is_our_team_win BOOLEAN,
    attack_rounds_played INT,
    attack_rounds_won INT,
    attack_win_pct DOUBLE,
    defense_rounds_played INT,
    defense_rounds_won INT,
    defense_win_pct DOUBLE,
    team_kills INT,
    team_deaths INT,
    team_assists INT,
    team_kd_ratio DOUBLE,
    team_damage_dealt INT,
    team_damage_received INT,
    team_damage_differential INT,
    thrifty_rounds_won INT,
    match_mvp_player STRING,
    match_mvp_agent STRING,
    match_mvp_acs DOUBLE,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Gold executive match summary: scores, Attack/Defense splits, team combat totals, and MVP'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Aggregate, and Derive Business KPIs

# COMMAND ----------
# 1. Base Match Metadata
base_match_df = spark.table(SOURCE_DIM_MATCH).select(
    F.col("match_id"),
    F.col("match_date"),
    F.col("match_start_timestamp"),
    F.col("map_name"),
    F.col("game_duration_seconds"),
    F.col("game_duration_minutes"),
    F.col("rounds_played"),
    F.col("is_overtime"),
    F.col("our_team_color"),
    F.col("our_team_rounds_won"),
    F.col("opponent_rounds_won"),
    F.col("round_differential"),
    F.col("match_outcome"),
    F.col("is_our_team_win")
)

# 2. Map Splash Artwork from dim_map
map_df = spark.table(SOURCE_DIM_MAP).select(
    F.col("map_name"),
    F.col("map_splash_url")
)

# 3. Round-Level Attack/Defense & Thrifty Aggregations from fact_round
round_stats_df = spark.table(SOURCE_FACT_ROUND).groupBy("match_id").agg(
    F.count(F.when(F.col("our_team_side") == "Attack", 1)).alias("attack_rounds_played"),
    F.count(F.when((F.col("our_team_side") == "Attack") & (F.col("is_our_team_win") == True), 1)).alias("attack_rounds_won"),
    F.count(F.when(F.col("our_team_side") == "Defense", 1)).alias("defense_rounds_played"),
    F.count(F.when((F.col("our_team_side") == "Defense") & (F.col("is_our_team_win") == True), 1)).alias("defense_rounds_won"),
    F.count(F.when((F.col("is_thrifty") == True) & (F.col("is_our_team_win") == True), 1)).alias("thrifty_rounds_won")
)

# 4. Team Combat Totals from fact_match_player (filtered to our squad)
our_player_stats_df = spark.table(SOURCE_FACT_MATCH_PLAYER).filter(F.col("is_our_team") == True)

team_combat_df = our_player_stats_df.groupBy("match_id").agg(
    F.sum("kills").alias("team_kills"),
    F.sum("deaths").alias("team_deaths"),
    F.sum("assists").alias("team_assists"),
    F.sum("damage_made").alias("team_damage_dealt"),
    F.sum("damage_received").alias("team_damage_received"),
    F.sum("damage_differential").alias("team_damage_differential")
).withColumn(
    "team_kd_ratio",
    F.round(F.col("team_kills") / F.when(F.col("team_deaths") > 0, F.col("team_deaths")).otherwise(1), 2)
)

# 5. Resolve Match MVP (Player with highest ACS on our squad)
mvp_window = Window.partitionBy("match_id").orderBy(
    F.col("average_combat_score").desc_nulls_last(),
    F.col("kills").desc_nulls_last()
)

match_mvp_df = our_player_stats_df.withColumn(
    "mvp_rank", F.row_number().over(mvp_window)
).filter(
    F.col("mvp_rank") == 1
).select(
    F.col("match_id"),
    F.col("current_display_name").alias("match_mvp_player"),
    F.col("agent_name").alias("match_mvp_agent"),
    F.col("average_combat_score").alias("match_mvp_acs")
)

# 6. Assemble Gold Match Summary
staged_gold_match_df = base_match_df.join(
    map_df, on="map_name", how="left"
).join(
    round_stats_df, on="match_id", how="left"
).join(
    team_combat_df, on="match_id", how="left"
).join(
    match_mvp_df, on="match_id", how="left"
).withColumn(
    "score_display",
    F.concat_ws(" - ", F.coalesce(F.col("our_team_rounds_won"), F.lit(0)), F.coalesce(F.col("opponent_rounds_won"), F.lit(0)))
).withColumn(
    "attack_win_pct",
    F.when(F.col("attack_rounds_played") > 0,
           F.round((F.col("attack_rounds_won") / F.col("attack_rounds_played")) * 100.0, 1)
    ).otherwise(0.0)
).withColumn(
    "defense_win_pct",
    F.when(F.col("defense_rounds_played") > 0,
           F.round((F.col("defense_rounds_won") / F.col("defense_rounds_played")) * 100.0, 1)
    ).otherwise(0.0)
).select(
    "match_id",
    "match_date",
    "match_start_timestamp",
    "map_name",
    "map_splash_url",
    "game_duration_seconds",
    "game_duration_minutes",
    "rounds_played",
    "is_overtime",
    "our_team_color",
    "our_team_rounds_won",
    "opponent_rounds_won",
    "score_display",
    "round_differential",
    "match_outcome",
    "is_our_team_win",
    F.coalesce(F.col("attack_rounds_played"), F.lit(0)).alias("attack_rounds_played"),
    F.coalesce(F.col("attack_rounds_won"), F.lit(0)).alias("attack_rounds_won"),
    "attack_win_pct",
    F.coalesce(F.col("defense_rounds_played"), F.lit(0)).alias("defense_rounds_played"),
    F.coalesce(F.col("defense_rounds_won"), F.lit(0)).alias("defense_rounds_won"),
    "defense_win_pct",
    F.coalesce(F.col("team_kills"), F.lit(0)).alias("team_kills"),
    F.coalesce(F.col("team_deaths"), F.lit(0)).alias("team_deaths"),
    F.coalesce(F.col("team_assists"), F.lit(0)).alias("team_assists"),
    F.coalesce(F.col("team_kd_ratio"), F.lit(1.0)).alias("team_kd_ratio"),
    F.coalesce(F.col("team_damage_dealt"), F.lit(0)).alias("team_damage_dealt"),
    F.coalesce(F.col("team_damage_received"), F.lit(0)).alias("team_damage_received"),
    F.coalesce(F.col("team_damage_differential"), F.lit(0)).alias("team_damage_differential"),
    F.coalesce(F.col("thrifty_rounds_won"), F.lit(0)).alias("thrifty_rounds_won"),
    "match_mvp_player",
    "match_mvp_agent",
    "match_mvp_acs",
    F.current_timestamp().alias("updated_at")
)

staged_gold_match_df.createOrReplaceTempView("staged_gold_match_summary")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `gold_match_summary`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_match_summary AS source
ON target.match_id = source.match_id
WHEN MATCHED THEN
  UPDATE SET
    target.match_date = source.match_date,
    target.match_start_timestamp = source.match_start_timestamp,
    target.map_name = source.map_name,
    target.map_splash_url = source.map_splash_url,
    target.game_duration_seconds = source.game_duration_seconds,
    target.game_duration_minutes = source.game_duration_minutes,
    target.rounds_played = source.rounds_played,
    target.is_overtime = source.is_overtime,
    target.our_team_color = source.our_team_color,
    target.our_team_rounds_won = source.our_team_rounds_won,
    target.opponent_rounds_won = source.opponent_rounds_won,
    target.score_display = source.score_display,
    target.round_differential = source.round_differential,
    target.match_outcome = source.match_outcome,
    target.is_our_team_win = source.is_our_team_win,
    target.attack_rounds_played = source.attack_rounds_played,
    target.attack_rounds_won = source.attack_rounds_won,
    target.attack_win_pct = source.attack_win_pct,
    target.defense_rounds_played = source.defense_rounds_played,
    target.defense_rounds_won = source.defense_rounds_won,
    target.defense_win_pct = source.defense_win_pct,
    target.team_kills = source.team_kills,
    target.team_deaths = source.team_deaths,
    target.team_assists = source.team_assists,
    target.team_kd_ratio = source.team_kd_ratio,
    target.team_damage_dealt = source.team_damage_dealt,
    target.team_damage_received = source.team_damage_received,
    target.team_damage_differential = source.team_damage_differential,
    target.thrifty_rounds_won = source.thrifty_rounds_won,
    target.match_mvp_player = source.match_mvp_player,
    target.match_mvp_agent = source.match_mvp_agent,
    target.match_mvp_acs = source.match_mvp_acs,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id, match_date, match_start_timestamp, map_name, map_splash_url,
    game_duration_seconds, game_duration_minutes, rounds_played, is_overtime,
    our_team_color, our_team_rounds_won, opponent_rounds_won, score_display,
    round_differential, match_outcome, is_our_team_win, attack_rounds_played,
    attack_rounds_won, attack_win_pct, defense_rounds_played, defense_rounds_won,
    defense_win_pct, team_kills, team_deaths, team_assists, team_kd_ratio,
    team_damage_dealt, team_damage_received, team_damage_differential,
    thrifty_rounds_won, match_mvp_player, match_mvp_agent, match_mvp_acs, updated_at
  )
  VALUES (
    source.match_id, source.match_date, source.match_start_timestamp, source.map_name, source.map_splash_url,
    source.game_duration_seconds, source.game_duration_minutes, source.rounds_played, source.is_overtime,
    source.our_team_color, source.our_team_rounds_won, source.opponent_rounds_won, source.score_display,
    source.round_differential, source.match_outcome, source.is_our_team_win, source.attack_rounds_played,
    source.attack_rounds_won, source.attack_win_pct, source.defense_rounds_played, source.defense_rounds_won,
    source.defense_win_pct, source.team_kills, source.team_deaths, source.team_assists, source.team_kd_ratio,
    source.team_damage_dealt, source.team_damage_received, source.team_damage_differential,
    source.thrifty_rounds_won, source.match_mvp_player, source.match_mvp_agent, source.match_mvp_acs, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification and Executive Sample Display

# COMMAND ----------
total_gold_matches = spark.table(TARGET_TABLE).count()
total_wins = spark.table(TARGET_TABLE).filter(F.col("match_outcome") == "VICTORY").count()
total_losses = spark.table(TARGET_TABLE).filter(F.col("match_outcome") == "DEFEAT").count()
win_pct = round((total_wins / total_gold_matches) * 100, 1) if total_gold_matches > 0 else 0.0

print(f"Total Gold Matches: {total_gold_matches}")
print(f"Record: {total_wins} Wins / {total_losses} Losses ({win_pct}% Win Rate)")

# Display latest 10 matches formatted like Power BI Match History
spark.table(TARGET_TABLE).select(
    "match_date",
    "map_name",
    "score_display",
    "match_outcome",
    "attack_win_pct",
    "defense_win_pct",
    "team_kd_ratio",
    "match_mvp_player",
    "match_mvp_agent",
    "match_mvp_acs"
).orderBy(F.col("match_start_timestamp").desc()).show(10, truncate=False)
