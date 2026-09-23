# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_map_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per map
# MAGIC - **Primary Key:** `map_name`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.silver.dim_map` (Map splash URLs, bomb site counts, active pool status)
# MAGIC   - `valorant.gold.gold_match_summary` (Match results, scores, rounds, combat stats)
# MAGIC   - `valorant.silver.fact_round` (Starting side determination, 1st half round conversions)
# MAGIC - **Target:** `valorant.gold.gold_map_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `map_name`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Map Tier List & Ban/Pick Guidance:** Ranking maps by win percentage (`S-Tier`, `A-Tier`, `B-Tier`, `Perma-Ban`).
# MAGIC - **Starting Side Win Rates:** Granular comparison of winning when starting on **Attack** vs **Defense** (e.g. Ascent: Attack start 80% vs Defense start 20%).
# MAGIC - **Attack vs Defense Hold Rates:** Round conversion efficiency on each half.
# MAGIC - **Map Visual Cards:** Riot CDN high-resolution map splash artwork.

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "gold"
SOURCE_DIM_MAP    = f"{CATALOG}.silver.dim_map"
SOURCE_GOLD_MATCH = f"{CATALOG}.gold.gold_match_summary"
SOURCE_FACT_ROUND = f"{CATALOG}.silver.fact_round"
TARGET_TABLE      = f"{CATALOG}.{SCHEMA}.gold_map_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_DIM_MAP}, {SOURCE_GOLD_MATCH}, {SOURCE_FACT_ROUND}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    map_name STRING NOT NULL,
    map_splash_url STRING,
    bomb_site_count INT,
    is_active_pool BOOLEAN,
    matches_played INT NOT NULL,
    matches_won INT NOT NULL,
    matches_lost INT NOT NULL,
    matches_drawn INT NOT NULL,
    map_win_pct DOUBLE NOT NULL,
    play_frequency_pct DOUBLE NOT NULL,
    rounds_played INT NOT NULL,
    rounds_won INT NOT NULL,
    rounds_lost INT NOT NULL,
    round_win_pct DOUBLE NOT NULL,
    round_differential INT NOT NULL,
    avg_round_differential DOUBLE NOT NULL,
    attack_rounds_played INT NOT NULL,
    attack_rounds_won INT NOT NULL,
    attack_win_pct DOUBLE NOT NULL,
    defense_rounds_played INT NOT NULL,
    defense_rounds_won INT NOT NULL,
    defense_win_pct DOUBLE NOT NULL,
    attack_start_matches INT NOT NULL,
    attack_start_wins INT NOT NULL,
    attack_start_win_pct DOUBLE NOT NULL,
    attack_start_first_half_round_win_pct DOUBLE NOT NULL,
    defense_start_matches INT NOT NULL,
    defense_start_wins INT NOT NULL,
    defense_start_win_pct DOUBLE NOT NULL,
    defense_start_first_half_round_win_pct DOUBLE NOT NULL,
    side_bias STRING NOT NULL,
    team_kd_ratio DOUBLE NOT NULL,
    thrifty_rounds_won INT NOT NULL,
    avg_match_duration_minutes DOUBLE,
    map_tier STRING NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold map performance: win rates, starting side splits, side bias, and map tier classifications'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Starting Sides and First-Half Momentum from fact_round

# COMMAND ----------
fact_round_df = spark.table(SOURCE_FACT_ROUND)

# 1. Determine which side our squad started on (Round 1)
starting_side_df = fact_round_df.filter(
    F.col("round_number") == 1
).select(
    F.col("match_id"),
    F.col("our_team_side").alias("starting_side")
)

# 2. First Half (Rounds 1–12) round conversions
first_half_df = fact_round_df.filter(
    F.col("round_number") <= 12
).groupBy("match_id").agg(
    F.count("*").alias("fh_rounds_played"),
    F.count(F.when(F.col("is_our_team_win") == True, 1)).alias("fh_rounds_won")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Enrich Match Summaries with Starting Side Attributes

# COMMAND ----------
match_df = spark.table(SOURCE_GOLD_MATCH) \
    .join(starting_side_df, on="match_id", how="left") \
    .join(first_half_df, on="match_id", how="left")

# Total matches played across the entire squad to calculate play frequency %
total_matches_all_maps = match_df.select("match_id").distinct().count()

# Map-level Aggregation
map_agg_df = match_df.groupBy("map_name").agg(
    F.countDistinct("match_id").alias("matches_played"),
    F.count(F.when(F.col("is_our_team_win") == True, 1)).alias("matches_won"),
    F.count(F.when(F.col("match_outcome") == "DEFEAT", 1)).alias("matches_lost"),
    F.count(F.when(F.col("match_outcome") == "DRAW", 1)).alias("matches_drawn"),
    F.sum("rounds_played").alias("rounds_played"),
    F.sum("our_team_rounds_won").alias("rounds_won"),
    F.sum("opponent_rounds_won").alias("rounds_lost"),
    F.sum("round_differential").alias("round_differential"),
    F.round(F.avg("round_differential"), 2).alias("avg_round_differential"),
    F.sum("attack_rounds_played").alias("attack_rounds_played"),
    F.sum("attack_rounds_won").alias("attack_rounds_won"),
    F.sum("defense_rounds_played").alias("defense_rounds_played"),
    F.sum("defense_rounds_won").alias("defense_rounds_won"),
    # Starting on Attack splits
    F.count(F.when(F.col("starting_side") == "Attack", 1)).alias("attack_start_matches"),
    F.count(F.when((F.col("starting_side") == "Attack") & (F.col("is_our_team_win") == True), 1)).alias("attack_start_wins"),
    F.coalesce(F.sum(F.when(F.col("starting_side") == "Attack", F.coalesce(F.col("fh_rounds_won"), F.lit(0)))), F.lit(0)).alias("attack_start_fh_rounds_won"),
    F.coalesce(F.sum(F.when(F.col("starting_side") == "Attack", F.coalesce(F.col("fh_rounds_played"), F.lit(0)))), F.lit(0)).alias("attack_start_fh_rounds_played"),
    # Starting on Defense splits
    F.count(F.when(F.col("starting_side") == "Defense", 1)).alias("defense_start_matches"),
    F.count(F.when((F.col("starting_side") == "Defense") & (F.col("is_our_team_win") == True), 1)).alias("defense_start_wins"),
    F.coalesce(F.sum(F.when(F.col("starting_side") == "Defense", F.coalesce(F.col("fh_rounds_won"), F.lit(0)))), F.lit(0)).alias("defense_start_fh_rounds_won"),
    F.coalesce(F.sum(F.when(F.col("starting_side") == "Defense", F.coalesce(F.col("fh_rounds_played"), F.lit(0)))), F.lit(0)).alias("defense_start_fh_rounds_played"),
    # Team combat & pace
    F.coalesce(F.sum("team_kills"), F.lit(0)).alias("team_kills"),
    F.coalesce(F.sum("team_deaths"), F.lit(0)).alias("team_deaths"),
    F.coalesce(F.sum("thrifty_rounds_won"), F.lit(0)).alias("thrifty_rounds_won"),
    F.round(F.avg("game_duration_minutes"), 1).alias("avg_match_duration_minutes")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Join Map Dimensions, Compute Rates, Tiers, and Side Bias

# COMMAND ----------
dim_map_df = spark.table(SOURCE_DIM_MAP).select(
    F.col("map_name"),
    F.col("map_splash_url"),
    F.col("site_count").alias("bomb_site_count"),
    F.col("is_active_pool")
)

final_gold_map_df = dim_map_df.join(map_agg_df, on="map_name", how="inner").select(
    F.col("map_name"),
    F.col("map_splash_url"),
    F.col("bomb_site_count"),
    F.col("is_active_pool"),
    F.col("matches_played"),
    F.col("matches_won"),
    F.col("matches_lost"),
    F.col("matches_drawn"),
    F.coalesce(F.round(F.col("matches_won") * 100.0 / F.when(F.col("matches_played") == 0, 1).otherwise(F.col("matches_played")), 2), F.lit(0.0)).alias("map_win_pct"),
    F.coalesce(F.round(F.col("matches_played") * 100.0 / F.lit(max(total_matches_all_maps, 1)), 2), F.lit(0.0)).alias("play_frequency_pct"),
    F.coalesce(F.col("rounds_played"), F.lit(0)).alias("rounds_played"),
    F.coalesce(F.col("rounds_won"), F.lit(0)).alias("rounds_won"),
    F.coalesce(F.col("rounds_lost"), F.lit(0)).alias("rounds_lost"),
    F.coalesce(F.round(F.col("rounds_won") * 100.0 / F.when(F.coalesce(F.col("rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("rounds_played")), 2), F.lit(0.0)).alias("round_win_pct"),
    F.coalesce(F.col("round_differential"), F.lit(0)).alias("round_differential"),
    F.coalesce(F.col("avg_round_differential"), F.lit(0.0)).alias("avg_round_differential"),
    F.coalesce(F.col("attack_rounds_played"), F.lit(0)).alias("attack_rounds_played"),
    F.coalesce(F.col("attack_rounds_won"), F.lit(0)).alias("attack_rounds_won"),
    F.coalesce(F.round(F.col("attack_rounds_won") * 100.0 / F.when(F.coalesce(F.col("attack_rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("attack_rounds_played")), 2), F.lit(0.0)).alias("attack_win_pct"),
    F.coalesce(F.col("defense_rounds_played"), F.lit(0)).alias("defense_rounds_played"),
    F.coalesce(F.col("defense_rounds_won"), F.lit(0)).alias("defense_rounds_won"),
    F.coalesce(F.round(F.col("defense_rounds_won") * 100.0 / F.when(F.coalesce(F.col("defense_rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("defense_rounds_played")), 2), F.lit(0.0)).alias("defense_win_pct"),
    F.coalesce(F.col("attack_start_matches"), F.lit(0)).alias("attack_start_matches"),
    F.coalesce(F.col("attack_start_wins"), F.lit(0)).alias("attack_start_wins"),
    F.coalesce(F.round(F.col("attack_start_wins") * 100.0 / F.when(F.coalesce(F.col("attack_start_matches"), F.lit(0)) == 0, 1).otherwise(F.col("attack_start_matches")), 2), F.lit(0.0)).alias("attack_start_win_pct"),
    F.coalesce(F.round(F.col("attack_start_fh_rounds_won") * 100.0 / F.when(F.coalesce(F.col("attack_start_fh_rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("attack_start_fh_rounds_played")), 2), F.lit(0.0)).alias("attack_start_first_half_round_win_pct"),
    F.coalesce(F.col("defense_start_matches"), F.lit(0)).alias("defense_start_matches"),
    F.coalesce(F.col("defense_start_wins"), F.lit(0)).alias("defense_start_wins"),
    F.coalesce(F.round(F.col("defense_start_wins") * 100.0 / F.when(F.coalesce(F.col("defense_start_matches"), F.lit(0)) == 0, 1).otherwise(F.col("defense_start_matches")), 2), F.lit(0.0)).alias("defense_start_win_pct"),
    F.coalesce(F.round(F.col("defense_start_fh_rounds_won") * 100.0 / F.when(F.coalesce(F.col("defense_start_fh_rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("defense_start_fh_rounds_played")), 2), F.lit(0.0)).alias("defense_start_first_half_round_win_pct"),
    F.coalesce(F.round(F.col("team_kills") / F.when(F.coalesce(F.col("team_deaths"), F.lit(0)) == 0, 1.0).otherwise(F.col("team_deaths")), 2), F.lit(0.0)).alias("team_kd_ratio"),
    F.coalesce(F.col("thrifty_rounds_won"), F.lit(0)).alias("thrifty_rounds_won"),
    F.col("avg_match_duration_minutes")
).withColumn(
    "side_bias",
    F.when((F.col("attack_win_pct") - F.col("defense_win_pct")) >= 7.0, F.lit("Attack-Biased"))
     .when((F.col("defense_win_pct") - F.col("attack_win_pct")) >= 7.0, F.lit("Defense-Biased"))
     .otherwise(F.lit("Balanced"))
).withColumn(
    "map_tier",
    F.when(F.col("map_win_pct") >= 65.0, F.lit("S-Tier"))
     .when(F.col("map_win_pct") >= 55.0, F.lit("A-Tier"))
     .when(F.col("map_win_pct") >= 45.0, F.lit("B-Tier"))
     .otherwise(F.lit("Perma-Ban"))
).withColumn(
    "updated_at", F.current_timestamp()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_map_df.createOrReplaceTempView("staged_gold_map_performance")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_map_performance AS source
ON target.map_name = source.map_name
WHEN MATCHED THEN
  UPDATE SET
    target.map_splash_url = source.map_splash_url,
    target.bomb_site_count = source.bomb_site_count,
    target.is_active_pool = source.is_active_pool,
    target.matches_played = source.matches_played,
    target.matches_won = source.matches_won,
    target.matches_lost = source.matches_lost,
    target.matches_drawn = source.matches_drawn,
    target.map_win_pct = source.map_win_pct,
    target.play_frequency_pct = source.play_frequency_pct,
    target.rounds_played = source.rounds_played,
    target.rounds_won = source.rounds_won,
    target.rounds_lost = source.rounds_lost,
    target.round_win_pct = source.round_win_pct,
    target.round_differential = source.round_differential,
    target.avg_round_differential = source.avg_round_differential,
    target.attack_rounds_played = source.attack_rounds_played,
    target.attack_rounds_won = source.attack_rounds_won,
    target.attack_win_pct = source.attack_win_pct,
    target.defense_rounds_played = source.defense_rounds_played,
    target.defense_rounds_won = source.defense_rounds_won,
    target.defense_win_pct = source.defense_win_pct,
    target.attack_start_matches = source.attack_start_matches,
    target.attack_start_wins = source.attack_start_wins,
    target.attack_start_win_pct = source.attack_start_win_pct,
    target.attack_start_first_half_round_win_pct = source.attack_start_first_half_round_win_pct,
    target.defense_start_matches = source.defense_start_matches,
    target.defense_start_wins = source.defense_start_wins,
    target.defense_start_win_pct = source.defense_start_win_pct,
    target.defense_start_first_half_round_win_pct = source.defense_start_first_half_round_win_pct,
    target.side_bias = source.side_bias,
    target.team_kd_ratio = source.team_kd_ratio,
    target.thrifty_rounds_won = source.thrifty_rounds_won,
    target.avg_match_duration_minutes = source.avg_match_duration_minutes,
    target.map_tier = source.map_tier,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    map_name, map_splash_url, bomb_site_count, is_active_pool,
    matches_played, matches_won, matches_lost, matches_drawn,
    map_win_pct, play_frequency_pct, rounds_played, rounds_won, rounds_lost,
    round_win_pct, round_differential, avg_round_differential,
    attack_rounds_played, attack_rounds_won, attack_win_pct,
    defense_rounds_played, defense_rounds_won, defense_win_pct,
    attack_start_matches, attack_start_wins, attack_start_win_pct, attack_start_first_half_round_win_pct,
    defense_start_matches, defense_start_wins, defense_start_win_pct, defense_start_first_half_round_win_pct,
    side_bias, team_kd_ratio, thrifty_rounds_won, avg_match_duration_minutes,
    map_tier, updated_at
  )
  VALUES (
    source.map_name, source.map_splash_url, source.bomb_site_count, source.is_active_pool,
    source.matches_played, source.matches_won, source.matches_lost, source.matches_drawn,
    source.map_win_pct, source.play_frequency_pct, source.rounds_played, source.rounds_won, source.rounds_lost,
    source.round_win_pct, source.round_differential, source.avg_round_differential,
    source.attack_rounds_played, source.attack_rounds_won, source.attack_win_pct,
    source.defense_rounds_played, source.defense_rounds_won, source.defense_win_pct,
    source.attack_start_matches, source.attack_start_wins, source.attack_start_win_pct, source.attack_start_first_half_round_win_pct,
    source.defense_start_matches, source.defense_start_wins, source.defense_start_win_pct, source.defense_start_first_half_round_win_pct,
    source.side_bias, source.team_kd_ratio, source.thrifty_rounds_won, source.avg_match_duration_minutes,
    source.map_tier, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Verification & Map Tier List Display

# COMMAND ----------
total_maps = spark.table(TARGET_TABLE).count()
print(f"Total Maps Recorded in Gold: {total_maps}")

# Display Map Leaderboard & Starting Side Splits
print("Map Tier List & Starting Side Performance:")
spark.table(TARGET_TABLE).select(
    "map_name", "map_tier", "matches_played", "map_win_pct",
    "attack_start_matches", "attack_start_win_pct",
    "defense_start_matches", "defense_start_win_pct",
    "side_bias", "team_kd_ratio"
).orderBy(F.col("map_win_pct").desc()).show(15, truncate=False)
