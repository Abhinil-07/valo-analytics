# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_agent_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per squad member per agent
# MAGIC - **Composite Primary Key:** `player_puuid` + `agent_name`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.gold.gold_player_match_performance` (Match scorecards, agent picks, MVPs, ability casts)
# MAGIC   - `valorant.silver.fact_round_player` (Rounds contested, rounds won, credits spent per match)
# MAGIC   - `valorant.silver.dim_agent` (Riot CDN icons, tactical roles)
# MAGIC - **Target:** `valorant.gold.gold_agent_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `player_puuid` + `agent_name`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Pick Rate vs. Win Rate Quadrant:** Identifying true "Signature Picks", "Comfort Traps", and "Pocket Picks".
# MAGIC - **Hero Pool Depth:** Slicing player flexibility and agent rotation across tactical roles.
# MAGIC - **Agent Roster Heatmap:** Matrix comparing squad win rates and combat scores on each agent.
# MAGIC - **Utility Efficiency:** Average ultimates farmed and tactical abilities cast per round.

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
SOURCE_GOLD_PLAYER_MATCH = f"{CATALOG}.gold.gold_player_match_performance"
SOURCE_FACT_ROUND_PLAYER = f"{CATALOG}.silver.fact_round_player"
SOURCE_DIM_AGENT         = f"{CATALOG}.silver.dim_agent"
SOURCE_DIM_PLAYER        = f"{CATALOG}.silver.dim_player"
TARGET_TABLE             = f"{CATALOG}.{SCHEMA}.gold_agent_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_GOLD_PLAYER_MATCH}, {SOURCE_FACT_ROUND_PLAYER}, {SOURCE_DIM_AGENT}, {SOURCE_DIM_PLAYER}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    player_puuid STRING NOT NULL,
    player_name STRING,
    current_display_name STRING,
    roster_role STRING,
    is_core_team BOOLEAN NOT NULL,
    agent_name STRING NOT NULL,
    agent_role STRING,
    agent_icon_url STRING,
    matches_played INT NOT NULL,
    matches_won INT NOT NULL,
    matches_lost INT NOT NULL,
    agent_win_pct DOUBLE NOT NULL,
    agent_pick_pct DOUBLE NOT NULL,
    rounds_played INT NOT NULL,
    rounds_won INT NOT NULL,
    round_win_pct DOUBLE NOT NULL,
    total_kills INT NOT NULL,
    total_deaths INT NOT NULL,
    total_assists INT NOT NULL,
    kd_ratio DOUBLE NOT NULL,
    kill_differential INT NOT NULL,
    avg_acs DOUBLE NOT NULL,
    avg_adr DOUBLE NOT NULL,
    headshot_pct DOUBLE NOT NULL,
    total_ultimate_casts INT NOT NULL,
    avg_ultimates_per_match DOUBLE NOT NULL,
    total_ability_casts INT NOT NULL,
    avg_abilities_per_round DOUBLE NOT NULL,
    avg_spent_credits DOUBLE NOT NULL,
    match_mvp_count INT NOT NULL,
    team_top_fragger_count INT NOT NULL,
    mastery_tier STRING NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold agent performance: hero pool mastery, pick vs win rates, and utility efficiency per player'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Round Conversions and Player Totals

# COMMAND ----------
# 1. Round volumes per player per match from fact_round_player
round_player_df = spark.table(SOURCE_FACT_ROUND_PLAYER).filter(
    F.col("is_our_team") == True
).groupBy("player_puuid", "match_id").agg(
    F.count("*").alias("match_rounds_played"),
    F.count(F.when(F.col("is_round_win") == True, 1)).alias("match_rounds_won")
)

# 2. Base Player Match Records
player_match_df = spark.table(SOURCE_GOLD_PLAYER_MATCH).join(
    round_player_df, on=["player_puuid", "match_id"], how="left"
)

# 3. Calculate each player's lifetime match volume to derive pick %
player_match_totals_df = player_match_df.groupBy("player_puuid").agg(
    F.countDistinct("match_id").alias("player_lifetime_matches")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Aggregate Performance per Player per Agent

# COMMAND ----------
agent_agg_df = player_match_df.groupBy(
    "player_puuid", "agent_name"
).agg(
    # Player identities (latest)
    F.max("player_name").alias("player_name"),
    F.max("current_display_name").alias("current_display_name"),
    F.max("roster_role").alias("roster_role"),
    F.max("is_core_team").alias("is_core_team"),
    F.max("agent_role").alias("agent_role"),
    F.max("agent_icon_url").alias("agent_icon_url"),
    # Match volumes
    F.countDistinct("match_id").alias("matches_played"),
    F.count(F.when(F.col("is_match_win") == True, 1)).alias("matches_won"),
    F.count(F.when(F.col("match_outcome") == "DEFEAT", 1)).alias("matches_lost"),
    # Round volumes
    F.sum("match_rounds_played").alias("rounds_played"),
    F.sum("match_rounds_won").alias("rounds_won"),
    # Combat metrics
    F.sum("kills").alias("total_kills"),
    F.sum("deaths").alias("total_deaths"),
    F.sum("assists").alias("total_assists"),
    F.round(F.avg("average_combat_score"), 1).alias("avg_acs"),
    F.round(F.avg("average_damage_per_round"), 1).alias("avg_adr"),
    F.round(F.avg("headshot_pct"), 2).alias("headshot_pct"),
    # Ability casts & economy
    F.sum("ultimate_casts").alias("total_ultimate_casts"),
    F.sum("total_ability_casts").alias("total_ability_casts"),
    F.round(F.avg("spent_average"), 1).alias("avg_spent_credits"),
    # Accolades
    F.count(F.when(F.col("is_match_mvp") == True, 1)).alias("match_mvp_count"),
    F.count(F.when(F.col("is_team_top_fragger") == True, 1)).alias("team_top_fragger_count")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Compute Rates, Pick %, and Mastery Tiers

# COMMAND ----------
# Canonical Player Identity from dim_player
try:
    dim_player_df = spark.table(SOURCE_DIM_PLAYER).select(
        F.col("player_puuid"),
        F.col("current_display_name").alias("canonical_display_name")
    )
except Exception:
    dim_player_df = spark.createDataFrame([], "player_puuid STRING, canonical_display_name STRING")

final_gold_agent_df = agent_agg_df.join(
    player_match_totals_df, on="player_puuid", how="left"
).join(
    dim_player_df, on="player_puuid", how="left"
).select(
    F.col("player_puuid"),
    F.col("player_name"),
    F.coalesce(F.col("canonical_display_name"), F.col("current_display_name")).alias("current_display_name"),
    F.col("roster_role"),
    F.coalesce(F.col("is_core_team"), F.lit(False)).alias("is_core_team"),
    F.col("agent_name"),
    F.col("agent_role"),
    F.col("agent_icon_url"),
    F.col("matches_played"),
    F.col("matches_won"),
    F.col("matches_lost"),
    F.round(F.col("matches_won") * 100.0 / F.col("matches_played"), 2).alias("agent_win_pct"),
    F.round(F.col("matches_played") * 100.0 / F.when(F.col("player_lifetime_matches") == 0, 1).otherwise(F.col("player_lifetime_matches")), 2).alias("agent_pick_pct"),
    F.coalesce(F.col("rounds_played"), F.lit(0)).alias("rounds_played"),
    F.coalesce(F.col("rounds_won"), F.lit(0)).alias("rounds_won"),
    F.round(F.coalesce(F.col("rounds_won"), F.lit(0)) * 100.0 / F.when(F.coalesce(F.col("rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("rounds_played")), 2).alias("round_win_pct"),
    F.col("total_kills"),
    F.col("total_deaths"),
    F.col("total_assists"),
    F.round(F.col("total_kills") / F.when(F.col("total_deaths") == 0, 1.0).otherwise(F.col("total_deaths")), 2).alias("kd_ratio"),
    (F.col("total_kills") - F.col("total_deaths")).alias("kill_differential"),
    F.col("avg_acs"),
    F.col("avg_adr"),
    F.col("headshot_pct"),
    F.col("total_ultimate_casts"),
    F.round(F.col("total_ultimate_casts") / F.col("matches_played"), 2).alias("avg_ultimates_per_match"),
    F.col("total_ability_casts"),
    F.round(F.col("total_ability_casts") / F.when(F.coalesce(F.col("rounds_played"), F.lit(0)) == 0, 1.0).otherwise(F.col("rounds_played")), 2).alias("avg_abilities_per_round"),
    F.col("avg_spent_credits"),
    F.col("match_mvp_count"),
    F.col("team_top_fragger_count")
).withColumn(
    "mastery_tier",
    F.when((F.col("matches_played") >= 20) & (F.col("agent_win_pct") >= 55.0), F.lit("Signature"))
     .when(F.col("matches_played") >= 10, F.lit("Comfort Pick"))
     .when((F.col("matches_played") < 10) & (F.col("agent_win_pct") >= 60.0), F.lit("Pocket Pick"))
     .otherwise(F.lit("Experimental"))
).withColumn(
    "updated_at", F.current_timestamp()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_agent_df.createOrReplaceTempView("staged_gold_agent_performance")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_agent_performance AS source
ON target.player_puuid = source.player_puuid AND target.agent_name = source.agent_name
WHEN MATCHED THEN
  UPDATE SET
    target.player_name = source.player_name,
    target.current_display_name = source.current_display_name,
    target.roster_role = source.roster_role,
    target.is_core_team = source.is_core_team,
    target.agent_role = source.agent_role,
    target.agent_icon_url = source.agent_icon_url,
    target.matches_played = source.matches_played,
    target.matches_won = source.matches_won,
    target.matches_lost = source.matches_lost,
    target.agent_win_pct = source.agent_win_pct,
    target.agent_pick_pct = source.agent_pick_pct,
    target.rounds_played = source.rounds_played,
    target.rounds_won = source.rounds_won,
    target.round_win_pct = source.round_win_pct,
    target.total_kills = source.total_kills,
    target.total_deaths = source.total_deaths,
    target.total_assists = source.total_assists,
    target.kd_ratio = source.kd_ratio,
    target.kill_differential = source.kill_differential,
    target.avg_acs = source.avg_acs,
    target.avg_adr = source.avg_adr,
    target.headshot_pct = source.headshot_pct,
    target.total_ultimate_casts = source.total_ultimate_casts,
    target.avg_ultimates_per_match = source.avg_ultimates_per_match,
    target.total_ability_casts = source.total_ability_casts,
    target.avg_abilities_per_round = source.avg_abilities_per_round,
    target.avg_spent_credits = source.avg_spent_credits,
    target.match_mvp_count = source.match_mvp_count,
    target.team_top_fragger_count = source.team_top_fragger_count,
    target.mastery_tier = source.mastery_tier,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    player_puuid, player_name, current_display_name, roster_role, is_core_team,
    agent_name, agent_role, agent_icon_url,
    matches_played, matches_won, matches_lost, agent_win_pct, agent_pick_pct,
    rounds_played, rounds_won, round_win_pct,
    total_kills, total_deaths, total_assists, kd_ratio, kill_differential,
    avg_acs, avg_adr, headshot_pct,
    total_ultimate_casts, avg_ultimates_per_match, total_ability_casts, avg_abilities_per_round,
    avg_spent_credits, match_mvp_count, team_top_fragger_count, mastery_tier, updated_at
  )
  VALUES (
    source.player_puuid, source.player_name, source.current_display_name, source.roster_role, source.is_core_team,
    source.agent_name, source.agent_role, source.agent_icon_url,
    source.matches_played, source.matches_won, source.matches_lost, source.agent_win_pct, source.agent_pick_pct,
    source.rounds_played, source.rounds_won, source.round_win_pct,
    source.total_kills, source.total_deaths, source.total_assists, source.kd_ratio, source.kill_differential,
    source.avg_acs, source.avg_adr, source.headshot_pct,
    source.total_ultimate_casts, source.avg_ultimates_per_match, source.total_ability_casts, source.avg_abilities_per_round,
    source.avg_spent_credits, source.match_mvp_count, source.team_top_fragger_count, source.mastery_tier, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Verification & Hero Pool Matrix Display

# COMMAND ----------
total_rows = spark.table(TARGET_TABLE).count()
print(f"Total Player-Agent Mastery Records: {total_rows}")

# Display Sample Hero Pools
print("Sample Player Agent Mastery (Top Picks by Matches Played):")
spark.table(TARGET_TABLE).filter(
    F.col("is_core_team") == True
).select(
    "current_display_name", "agent_name", "agent_role", "matches_played", "agent_win_pct",
    "agent_pick_pct", "kd_ratio", "avg_acs", "mastery_tier"
).orderBy(F.col("matches_played").desc()).show(15, truncate=False)
