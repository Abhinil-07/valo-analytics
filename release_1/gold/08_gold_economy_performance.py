# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_economy_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per buy tier matchup (e.g. `Full Buy vs Full Buy`, `Full Buy vs Eco`, `Eco vs Full Buy`)
# MAGIC - **Composite Primary Key:** `our_buy_tier` + `opponent_buy_tier`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.silver.fact_round` (Our buy tier, opponent buy tier, loadout values, thrifty flags, outcomes)
# MAGIC   - `valorant.silver.fact_round_player` (Round kills and deaths)
# MAGIC - **Target:** `valorant.gold.gold_economy_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `our_buy_tier` + `opponent_buy_tier`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Buy Tier Matchup Matrix:** Win rates across all economic scenarios (e.g. Full Buy vs Full Buy gun rounds).
# MAGIC - **Anti-Eco Stability:** Measuring conversion and throw rates when holding economic advantage.
# MAGIC - **Thrifty ROI:** Win rate and kill efficiency when operating on eco or save rounds against enemy rifle buys.
# MAGIC - **Loadout Expenditure Advantage:** Correlation between credit investment advantage and round victories.

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "gold"
SOURCE_FACT_ROUND        = f"{CATALOG}.silver.fact_round"
SOURCE_FACT_ROUND_PLAYER = f"{CATALOG}.silver.fact_round_player"
TARGET_TABLE             = f"{CATALOG}.{SCHEMA}.gold_economy_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_FACT_ROUND}, {SOURCE_FACT_ROUND_PLAYER}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    buy_matchup STRING NOT NULL,
    our_buy_tier STRING NOT NULL,
    opponent_buy_tier STRING NOT NULL,
    rounds_played INT NOT NULL,
    rounds_won INT NOT NULL,
    rounds_lost INT NOT NULL,
    matchup_win_pct DOUBLE NOT NULL,
    avg_our_loadout_value INT NOT NULL,
    avg_opponent_loadout_value INT NOT NULL,
    avg_loadout_advantage INT NOT NULL,
    thrifty_rounds_won INT NOT NULL,
    thrifty_conversion_pct DOUBLE NOT NULL,
    anti_eco_throw_count INT NOT NULL,
    team_kills INT NOT NULL,
    team_deaths INT NOT NULL,
    team_kd_ratio DOUBLE NOT NULL,
    spikes_planted INT NOT NULL,
    spikes_defused INT NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold economy performance: buy tier matchup matrix, anti-eco conversion, and thrifty ROI'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Round Combat and Join with fact_round

# COMMAND ----------
# 1. Round-level kills and deaths from fact_round_player
round_combat_df = spark.table(SOURCE_FACT_ROUND_PLAYER).filter(
    F.col("is_our_team") == True
).groupBy("match_id", "round_number").agg(
    F.sum("kills").alias("team_kills"),
    F.sum("deaths").alias("team_deaths")
)

# 2. Base enriched rounds
enriched_rounds_df = spark.table(SOURCE_FACT_ROUND).join(
    round_combat_df, on=["match_id", "round_number"], how="left"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Aggregate by Economic Matchup and Derive Metrics

# COMMAND ----------
economy_agg_df = enriched_rounds_df.filter(
    F.col("our_buy_tier").isNotNull() & F.col("opponent_buy_tier").isNotNull()
).groupBy(
    "our_buy_tier", "opponent_buy_tier"
).agg(
    F.count("*").alias("rounds_played"),
    F.count(F.when(F.col("is_our_team_win") == True, 1)).alias("rounds_won"),
    F.count(F.when(F.col("is_our_team_win") == False, 1)).alias("rounds_lost"),
    F.round(F.avg("our_team_loadout_value"), 0).cast("int").alias("avg_our_loadout_value"),
    F.round(F.avg("opponent_loadout_value"), 0).cast("int").alias("avg_opponent_loadout_value"),
    F.count(F.when(F.col("is_thrifty") == True, 1)).alias("thrifty_rounds_won"),
    F.count(F.when((F.col("our_buy_tier") == "Full Buy") & (F.col("opponent_buy_tier") == "Eco") & (F.col("is_our_team_win") == False), 1)).alias("anti_eco_throw_count"),
    F.coalesce(F.sum("team_kills"), F.lit(0)).alias("team_kills"),
    F.coalesce(F.sum("team_deaths"), F.lit(0)).alias("team_deaths"),
    F.count(F.when((F.col("our_team_side") == "Attack") & (F.col("bomb_planted") == True), 1)).alias("spikes_planted"),
    F.count(F.when((F.col("our_team_side") == "Defense") & (F.col("bomb_defused") == True), 1)).alias("spikes_defused")
)

final_gold_economy_df = economy_agg_df.select(
    F.concat_ws(" vs ", F.col("our_buy_tier"), F.col("opponent_buy_tier")).alias("buy_matchup"),
    F.col("our_buy_tier"),
    F.col("opponent_buy_tier"),
    F.col("rounds_played"),
    F.col("rounds_won"),
    F.col("rounds_lost"),
    F.round(F.col("rounds_won") * 100.0 / F.when(F.col("rounds_played") == 0, 1).otherwise(F.col("rounds_played")), 2).alias("matchup_win_pct"),
    F.col("avg_our_loadout_value"),
    F.col("avg_opponent_loadout_value"),
    (F.col("avg_our_loadout_value") - F.col("avg_opponent_loadout_value")).alias("avg_loadout_advantage"),
    F.col("thrifty_rounds_won"),
    F.round(F.col("thrifty_rounds_won") * 100.0 / F.when(F.col("rounds_played") == 0, 1).otherwise(F.col("rounds_played")), 2).alias("thrifty_conversion_pct"),
    F.col("anti_eco_throw_count"),
    F.col("team_kills"),
    F.col("team_deaths"),
    F.round(F.col("team_kills") / F.when(F.col("team_deaths") == 0, 1.0).otherwise(F.col("team_deaths")), 2).alias("team_kd_ratio"),
    F.col("spikes_planted"),
    F.col("spikes_defused"),
    F.current_timestamp().alias("updated_at")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_economy_df.createOrReplaceTempView("staged_gold_economy")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_economy AS source
ON target.our_buy_tier = source.our_buy_tier AND target.opponent_buy_tier = source.opponent_buy_tier
WHEN MATCHED THEN
  UPDATE SET
    target.buy_matchup = source.buy_matchup,
    target.rounds_played = source.rounds_played,
    target.rounds_won = source.rounds_won,
    target.rounds_lost = source.rounds_lost,
    target.matchup_win_pct = source.matchup_win_pct,
    target.avg_our_loadout_value = source.avg_our_loadout_value,
    target.avg_opponent_loadout_value = source.avg_opponent_loadout_value,
    target.avg_loadout_advantage = source.avg_loadout_advantage,
    target.thrifty_rounds_won = source.thrifty_rounds_won,
    target.thrifty_conversion_pct = source.thrifty_conversion_pct,
    target.anti_eco_throw_count = source.anti_eco_throw_count,
    target.team_kills = source.team_kills,
    target.team_deaths = source.team_deaths,
    target.team_kd_ratio = source.team_kd_ratio,
    target.spikes_planted = source.spikes_planted,
    target.spikes_defused = source.spikes_defused,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    buy_matchup, our_buy_tier, opponent_buy_tier, rounds_played, rounds_won, rounds_lost,
    matchup_win_pct, avg_our_loadout_value, avg_opponent_loadout_value, avg_loadout_advantage,
    thrifty_rounds_won, thrifty_conversion_pct, anti_eco_throw_count,
    team_kills, team_deaths, team_kd_ratio, spikes_planted, spikes_defused, updated_at
  )
  VALUES (
    source.buy_matchup, source.our_buy_tier, source.opponent_buy_tier, source.rounds_played, source.rounds_won, source.rounds_lost,
    source.matchup_win_pct, source.avg_our_loadout_value, source.avg_opponent_loadout_value, source.avg_loadout_advantage,
    source.thrifty_rounds_won, source.thrifty_conversion_pct, source.anti_eco_throw_count,
    source.team_kills, source.team_deaths, source.team_kd_ratio, source.spikes_planted, source.spikes_defused, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Verification & Buy Tier Matchup Display

# COMMAND ----------
total_matchups = spark.table(TARGET_TABLE).count()
print(f"Total Buy Tier Matchups Recorded: {total_matchups}")

# Display Economy Matchup Matrix
print("Economy Buy Tier Matchup Matrix:")
spark.table(TARGET_TABLE).select(
    "buy_matchup", "rounds_played", "rounds_won", "matchup_win_pct",
    "avg_our_loadout_value", "avg_opponent_loadout_value", "avg_loadout_advantage",
    "thrifty_rounds_won", "anti_eco_throw_count", "team_kd_ratio"
).orderBy(F.col("rounds_played").desc()).show(20, truncate=False)
