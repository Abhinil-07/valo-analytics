# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_player_match_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per squad member per match (~5 rows per match)
# MAGIC - **Composite Primary Key:** `match_id` + `player_puuid`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.silver.fact_match_player` (Individual combat stats, ACS, ADR, K/D, ability casts)
# MAGIC   - `valorant.silver.dim_match` (Match date, map, score display, outcome)
# MAGIC   - `valorant.silver.dim_agent` (Agent roles and Riot CDN portrait URLs)
# MAGIC   - `valorant.silver.dim_team_roster` (Core squad hierarchy and roster roles)
# MAGIC - **Target:** `valorant.gold.gold_player_match_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id` + `player_puuid`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Player Match History & Consistency:** Individual player scorecards with MVP badges and ratings.
# MAGIC - **Player Trendlines:** ACS, ADR, and K/D progression over time.
# MAGIC - **Opponent Comp Analysis:** How players perform against Duelist-heavy or Double-Controller enemy teams.

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
SOURCE_FACT_PLAYER = f"{CATALOG}.silver.fact_match_player"
SOURCE_DIM_MATCH   = f"{CATALOG}.silver.dim_match"
SOURCE_DIM_AGENT   = f"{CATALOG}.silver.dim_agent"
SOURCE_DIM_ROSTER  = f"{CATALOG}.silver.dim_team_roster"
TARGET_TABLE       = f"{CATALOG}.{SCHEMA}.gold_player_match_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_FACT_PLAYER}, {SOURCE_DIM_MATCH}, {SOURCE_DIM_AGENT}")

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
    current_display_name STRING,
    roster_role STRING,
    is_core_team BOOLEAN,
    match_date DATE NOT NULL,
    map_name STRING NOT NULL,
    score_display STRING,
    match_outcome STRING,
    is_match_win BOOLEAN,
    agent_name STRING,
    agent_role STRING,
    agent_icon_url STRING,
    competitive_rank STRING,
    account_level INT,
    opponent_duelist_count INT,
    opponent_comp_type STRING,
    is_match_mvp BOOLEAN,
    is_team_top_fragger BOOLEAN,
    kills INT,
    deaths INT,
    assists INT,
    kill_death_ratio DOUBLE,
    kill_differential INT,
    average_combat_score DOUBLE,
    average_damage_per_round DOUBLE,
    damage_differential INT,
    headshot_pct DOUBLE,
    bodyshot_pct DOUBLE,
    legshot_pct DOUBLE,
    ultimate_casts INT,
    signature_casts INT,
    ability_1_casts INT,
    ability_2_casts INT,
    total_ability_casts INT,
    spent_average DOUBLE,
    performance_rating STRING,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Gold player match performance: scorecard, agent roles, MVP badges, and opponent comp matchup'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Derive Badges, Opponent Comps, and Tier Ratings

# COMMAND ----------
# 1. Roster Roles from dim_team_roster
try:
    roster_df = spark.table(SOURCE_DIM_ROSTER).select(
        F.col("player_puuid"),
        F.col("team_role").alias("roster_role")
    )
except Exception:
    roster_df = spark.createDataFrame([], "player_puuid STRING, roster_role STRING")

# 2. Agent Metadata from dim_agent
agent_df = spark.table(SOURCE_DIM_AGENT).select(
    F.col("agent_name"),
    F.col("agent_role"),
    F.col("agent_icon_url")
)

# 3. Match Context from dim_match
match_df = spark.table(SOURCE_DIM_MATCH).select(
    F.col("match_id"),
    F.col("match_date"),
    F.col("map_name"),
    F.col("our_team_rounds_won"),
    F.col("opponent_rounds_won"),
    F.col("match_outcome")
).withColumn(
    "score_display",
    F.concat_ws(" - ", F.coalesce(F.col("our_team_rounds_won"), F.lit(0)), F.coalesce(F.col("opponent_rounds_won"), F.lit(0)))
)

# 4. Calculate Opponent Team Composition from fact_match_player (where is_our_team == false)
opponent_players_df = spark.table(SOURCE_FACT_PLAYER).filter(
    F.col("is_our_team") == False
).join(
    agent_df.select("agent_name", "agent_role"), on="agent_name", how="left"
)

opponent_comp_df = opponent_players_df.groupBy("match_id").agg(
    F.count(F.when(F.col("agent_role") == "Duelist", 1)).alias("opponent_duelist_count"),
    F.count(F.when(F.col("agent_role") == "Controller", 1)).alias("opponent_controller_count"),
    F.count(F.when(F.col("agent_role") == "Initiator", 1)).alias("opponent_initiator_count"),
    F.count(F.when(F.col("agent_role") == "Sentinel", 1)).alias("opponent_sentinel_count")
).withColumn(
    "opponent_comp_type",
    F.when(F.col("opponent_duelist_count") >= 3, F.lit("Triple Duelist"))
     .when(F.col("opponent_duelist_count") == 2, F.lit("Double Duelist"))
     .when(F.col("opponent_controller_count") >= 2, F.lit("Double Controller"))
     .when(F.col("opponent_duelist_count") == 1, F.lit("Standard Single Duelist"))
     .otherwise(F.lit("No Duelist / Custom"))
)

# 5. Base Player Performance for Our Squad
our_squad_df = spark.table(SOURCE_FACT_PLAYER).filter(
    F.col("is_our_team") == True
)

# 6. Window Functions for Team Top Fragger and Match MVP on Our Squad
match_rank_window = Window.partitionBy("match_id").orderBy(
    F.col("average_combat_score").desc_nulls_last(),
    F.col("kills").desc_nulls_last()
)

match_kills_window = Window.partitionBy("match_id").orderBy(
    F.col("kills").desc_nulls_last(),
    F.col("average_combat_score").desc_nulls_last()
)

ranked_squad_df = our_squad_df.withColumn(
    "acs_rank", F.row_number().over(match_rank_window)
).withColumn(
    "kills_rank", F.row_number().over(match_kills_window)
).withColumn(
    "is_match_mvp", F.col("acs_rank") == 1
).withColumn(
    "is_team_top_fragger", F.col("kills_rank") == 1
)

# 7. Join Everything Together and Compute Performance Tier
staged_gold_player_df = ranked_squad_df.join(
    match_df, on="match_id", how="inner"
).join(
    agent_df, on="agent_name", how="left"
).join(
    roster_df, on="player_puuid", how="left"
).join(
    opponent_comp_df, on="match_id", how="left"
).withColumn(
    "performance_rating",
    F.when(F.col("average_combat_score") >= 275, F.lit("Elite"))
     .when((F.col("average_combat_score") >= 200) & (F.col("average_combat_score") < 275), F.lit("Solid"))
     .otherwise(F.lit("Developing"))
).select(
    "match_id",
    "player_puuid",
    "player_name",
    "player_tag",
    "current_display_name",
    F.coalesce(F.col("roster_role"), F.lit("Squad Member")).alias("roster_role"),
    "is_core_team",
    "match_date",
    "map_name",
    "score_display",
    "match_outcome",
    "is_match_win",
    "agent_name",
    F.coalesce(F.col("agent_role"), F.lit("Duelist")).alias("agent_role"),
    "agent_icon_url",
    "competitive_rank",
    "account_level",
    F.coalesce(F.col("opponent_duelist_count"), F.lit(1)).alias("opponent_duelist_count"),
    F.coalesce(F.col("opponent_comp_type"), F.lit("Standard Single Duelist")).alias("opponent_comp_type"),
    "is_match_mvp",
    "is_team_top_fragger",
    "kills",
    "deaths",
    "assists",
    "kill_death_ratio",
    "kill_differential",
    "average_combat_score",
    "average_damage_per_round",
    "damage_differential",
    "headshot_pct",
    "bodyshot_pct",
    "legshot_pct",
    "ultimate_casts",
    "signature_casts",
    "ability_1_casts",
    "ability_2_casts",
    "total_ability_casts",
    "spent_average",
    "performance_rating",
    F.current_timestamp().alias("updated_at")
)

staged_gold_player_df.createOrReplaceTempView("staged_gold_player_perf")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `gold_player_match_performance`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_player_perf AS source
ON target.match_id = source.match_id AND target.player_puuid = source.player_puuid
WHEN MATCHED THEN
  UPDATE SET
    target.player_name = source.player_name,
    target.player_tag = source.player_tag,
    target.current_display_name = source.current_display_name,
    target.roster_role = source.roster_role,
    target.is_core_team = source.is_core_team,
    target.match_date = source.match_date,
    target.map_name = source.map_name,
    target.score_display = source.score_display,
    target.match_outcome = source.match_outcome,
    target.is_match_win = source.is_match_win,
    target.agent_name = source.agent_name,
    target.agent_role = source.agent_role,
    target.agent_icon_url = source.agent_icon_url,
    target.competitive_rank = source.competitive_rank,
    target.account_level = source.account_level,
    target.opponent_duelist_count = source.opponent_duelist_count,
    target.opponent_comp_type = source.opponent_comp_type,
    target.is_match_mvp = source.is_match_mvp,
    target.is_team_top_fragger = source.is_team_top_fragger,
    target.kills = source.kills,
    target.deaths = source.deaths,
    target.assists = source.assists,
    target.kill_death_ratio = source.kill_death_ratio,
    target.kill_differential = source.kill_differential,
    target.average_combat_score = source.average_combat_score,
    target.average_damage_per_round = source.average_damage_per_round,
    target.damage_differential = source.damage_differential,
    target.headshot_pct = source.headshot_pct,
    target.bodyshot_pct = source.bodyshot_pct,
    target.legshot_pct = source.legshot_pct,
    target.ultimate_casts = source.ultimate_casts,
    target.signature_casts = source.signature_casts,
    target.ability_1_casts = source.ability_1_casts,
    target.ability_2_casts = source.ability_2_casts,
    target.total_ability_casts = source.total_ability_casts,
    target.spent_average = source.spent_average,
    target.performance_rating = source.performance_rating,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id, player_puuid, player_name, player_tag, current_display_name, roster_role,
    is_core_team, match_date, map_name, score_display, match_outcome, is_match_win,
    agent_name, agent_role, agent_icon_url, competitive_rank, account_level,
    opponent_duelist_count, opponent_comp_type, is_match_mvp, is_team_top_fragger,
    kills, deaths, assists, kill_death_ratio, kill_differential, average_combat_score,
    average_damage_per_round, damage_differential, headshot_pct, bodyshot_pct, legshot_pct,
    ultimate_casts, signature_casts, ability_1_casts, ability_2_casts, total_ability_casts,
    spent_average, performance_rating, updated_at
  )
  VALUES (
    source.match_id, source.player_puuid, source.player_name, source.player_tag, source.current_display_name, source.roster_role,
    source.is_core_team, source.match_date, source.map_name, source.score_display, source.match_outcome, source.is_match_win,
    source.agent_name, source.agent_role, source.agent_icon_url, source.competitive_rank, source.account_level,
    source.opponent_duelist_count, source.opponent_comp_type, source.is_match_mvp, source.is_team_top_fragger,
    source.kills, source.deaths, source.assists, source.kill_death_ratio, source.kill_differential, source.average_combat_score,
    source.average_damage_per_round, source.damage_differential, source.headshot_pct, source.bodyshot_pct, source.legshot_pct,
    source.ultimate_casts, source.signature_casts, source.ability_1_casts, source.ability_2_casts, source.total_ability_casts,
    source.spent_average, source.performance_rating, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification & Matchup Sample Display

# COMMAND ----------
total_rows = spark.table(TARGET_TABLE).count()
total_mvps = spark.table(TARGET_TABLE).filter(F.col("is_match_mvp") == True).count()

print(f"Total Player Match Records: {total_rows}")
print(f"Total Match MVPs Awarded: {total_mvps}")

# Display Sample: Performance vs Opponent Comp Types
print("Squad Win Rate & ACS against different Opponent Compositions:")
spark.table(TARGET_TABLE).groupBy(
    "opponent_comp_type"
).agg(
    F.countDistinct("match_id").alias("matches_faced"),
    F.round(F.avg(F.col("is_match_win").cast("double")) * 100, 1).alias("win_rate_pct"),
    F.round(F.avg("average_combat_score"), 1).alias("squad_avg_acs")
).orderBy(F.col("matches_faced").desc()).show(10, truncate=False)
