# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Fact Table: `fact_round_player`
# MAGIC 
# MAGIC - **Grain:** 1 row per player per round per match (10 rows per round)
# MAGIC - **Composite Primary Key:** `match_id` + `round_number` + `player_puuid`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.bronze.bronze_round_player_stats` (Round combat, loadouts, weapons, armor, survival)
# MAGIC   - `valorant.silver.dim_match` (`our_team_color`)
# MAGIC   - `valorant.silver.fact_round` (`round_winner`, `attack_team`, `defense_team`, `our_buy_tier`, `opponent_buy_tier`)
# MAGIC   - `valorant.silver.dim_team_roster` (`is_core_team` flag)
# MAGIC   - `valorant.silver.dim_weapon` (`weapon_category`)
# MAGIC - **Target:** `valorant.silver.fact_round_player`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id` + `round_number` + `player_puuid`)

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "silver"
SOURCE_ROUND_STATS = f"{CATALOG}.bronze.bronze_round_player_stats"
SOURCE_FACT_ROUND  = f"{CATALOG}.silver.fact_round"
SOURCE_DIM_MATCH   = f"{CATALOG}.silver.dim_match"
SOURCE_DIM_ROSTER  = f"{CATALOG}.silver.dim_team_roster"
SOURCE_DIM_WEAPON  = f"{CATALOG}.silver.dim_weapon"
TARGET_TABLE       = f"{CATALOG}.{SCHEMA}.fact_round_player"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_ROUND_STATS}, {SOURCE_FACT_ROUND}, {SOURCE_DIM_MATCH}")

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
    team_perspective STRING,
    is_our_team BOOLEAN,
    is_core_team BOOLEAN,
    player_side STRING,
    is_round_win BOOLEAN,
    kills INT,
    deaths INT,
    assists INT,
    score INT,
    damage INT,
    headshots INT,
    bodyshots INT,
    legshots INT,
    was_alive BOOLEAN,
    weapon_name STRING,
    weapon_category STRING,
    armor_name STRING,
    loadout_value INT,
    spent_credits INT,
    remaining_credits INT,
    round_buy_tier STRING,
    ultimate_casts INT,
    signature_casts INT,
    ability_1_casts INT,
    ability_2_casts INT,
    was_afk BOOLEAN,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Round-level player statistics: weapons, armor, damage, side, and survival'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Enrich, and Map Dimensions

# COMMAND ----------
# 1. Active Core Roster PUUIDs
try:
    roster_df = spark.table(SOURCE_DIM_ROSTER).filter(F.col("is_active_roster") == True)
    core_puuids = [row["player_puuid"] for row in roster_df.select("player_puuid").collect()]
except Exception as e:
    print(f"Warning: Could not read {SOURCE_DIM_ROSTER} ({e}). Fallback to empty core squad.")
    core_puuids = []

# 2. Read Match squad color
match_df = spark.table(SOURCE_DIM_MATCH).select(
    F.col("match_id"),
    F.col("our_team_color")
)

# 3. Read Round context from fact_round
round_df = spark.table(SOURCE_FACT_ROUND).select(
    F.col("match_id"),
    F.col("round_number"),
    F.col("round_winner"),
    F.col("attack_team"),
    F.col("defense_team"),
    F.col("our_buy_tier"),
    F.col("opponent_buy_tier")
)

# 4. Read Weapon categories
weapon_df = spark.table(SOURCE_DIM_WEAPON).select(
    F.col("weapon_name"),
    F.col("weapon_category")
)

# 5. Base round player stats
stats_df = spark.table(SOURCE_ROUND_STATS).filter(F.col("player_puuid").isNotNull())

# 6. Join and compute derived attributes
staged_fact_round_player_df = stats_df.join(
    match_df, on="match_id", how="inner"
).join(
    round_df, on=["match_id", "round_number"], how="inner"
).join(
    weapon_df, on="weapon_name", how="left"
).withColumn(
    "player_team_clean", F.initcap(F.col("player_team"))
).withColumn(
    "is_our_team", F.col("player_team_clean") == F.col("our_team_color")
).withColumn(
    "team_perspective",
    F.when(F.col("is_our_team") == True, F.lit("OUR_TEAM")).otherwise(F.lit("OPPONENT"))
).withColumn(
    "is_core_team", F.col("player_puuid").isin(core_puuids)
).withColumn(
    # Player tactical side for this round
    "player_side",
    F.when(F.col("player_team_clean") == F.col("attack_team"), F.lit("Attack"))
     .when(F.col("player_team_clean") == F.col("defense_team"), F.lit("Defense"))
     .otherwise(None)
).withColumn(
    # Did this player's team win this round?
    "is_round_win",
    F.col("player_team_clean") == F.col("round_winner")
).withColumn(
    # Round buy tier for this player's team
    "round_buy_tier",
    F.when(F.col("is_our_team") == True, F.col("our_buy_tier"))
     .otherwise(F.col("opponent_buy_tier"))
).select(
    "match_id",
    "round_number",
    "player_puuid",
    "player_display_name",
    F.col("player_team_clean").alias("player_team"),
    "team_perspective",
    "is_our_team",
    "is_core_team",
    "player_side",
    "is_round_win",
    "kills",
    "deaths",
    "assists",
    "score",
    "damage",
    "headshots",
    "bodyshots",
    "legshots",
    "was_alive",
    "weapon_name",
    F.coalesce(F.col("weapon_category"), F.lit("Sidearm")).alias("weapon_category"),
    "armor_name",
    "loadout_value",
    "spent_credits",
    "remaining_credits",
    "round_buy_tier",
    F.col("x_casts").alias("ultimate_casts"),
    F.col("e_casts").alias("signature_casts"),
    F.col("q_casts").alias("ability_1_casts"),
    F.col("c_casts").alias("ability_2_casts"),
    "was_afk",
    F.current_timestamp().alias("updated_at")
)

staged_fact_round_player_df.createOrReplaceTempView("staged_fact_round_player")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `fact_round_player`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_fact_round_player AS source
ON target.match_id = source.match_id 
   AND target.round_number = source.round_number 
   AND target.player_puuid = source.player_puuid
WHEN MATCHED THEN
  UPDATE SET
    target.player_display_name = source.player_display_name,
    target.player_team = source.player_team,
    target.team_perspective = source.team_perspective,
    target.is_our_team = source.is_our_team,
    target.is_core_team = source.is_core_team,
    target.player_side = source.player_side,
    target.is_round_win = source.is_round_win,
    target.kills = source.kills,
    target.deaths = source.deaths,
    target.assists = source.assists,
    target.score = source.score,
    target.damage = source.damage,
    target.headshots = source.headshots,
    target.bodyshots = source.bodyshots,
    target.legshots = source.legshots,
    target.was_alive = source.was_alive,
    target.weapon_name = source.weapon_name,
    target.weapon_category = source.weapon_category,
    target.armor_name = source.armor_name,
    target.loadout_value = source.loadout_value,
    target.spent_credits = source.spent_credits,
    target.remaining_credits = source.remaining_credits,
    target.round_buy_tier = source.round_buy_tier,
    target.ultimate_casts = source.ultimate_casts,
    target.signature_casts = source.signature_casts,
    target.ability_1_casts = source.ability_1_casts,
    target.ability_2_casts = source.ability_2_casts,
    target.was_afk = source.was_afk,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id,
    round_number,
    player_puuid,
    player_display_name,
    player_team,
    team_perspective,
    is_our_team,
    is_core_team,
    player_side,
    is_round_win,
    kills,
    deaths,
    assists,
    score,
    damage,
    headshots,
    bodyshots,
    legshots,
    was_alive,
    weapon_name,
    weapon_category,
    armor_name,
    loadout_value,
    spent_credits,
    remaining_credits,
    round_buy_tier,
    ultimate_casts,
    signature_casts,
    ability_1_casts,
    ability_2_casts,
    was_afk,
    updated_at
  )
  VALUES (
    source.match_id,
    source.round_number,
    source.player_puuid,
    source.player_display_name,
    source.player_team,
    source.team_perspective,
    source.is_our_team,
    source.is_core_team,
    source.player_side,
    source.is_round_win,
    source.kills,
    source.deaths,
    source.assists,
    source.score,
    source.damage,
    source.headshots,
    source.bodyshots,
    source.legshots,
    source.was_alive,
    source.weapon_name,
    source.weapon_category,
    source.armor_name,
    source.loadout_value,
    source.spent_credits,
    source.remaining_credits,
    source.round_buy_tier,
    source.ultimate_casts,
    source.signature_casts,
    source.ability_1_casts,
    source.ability_2_casts,
    source.was_afk,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification and Summary Metrics

# COMMAND ----------
total_rows = spark.table(TARGET_TABLE).count()
total_matches = spark.table(TARGET_TABLE).select("match_id").distinct().count()
total_rounds = spark.table(TARGET_TABLE).select("match_id", "round_number").distinct().count()

print(f"Total Round Player Rows: {total_rows}")
print(f"Total Matches Represented: {total_matches}")
print(f"Total Rounds Represented: {total_rounds}")

# Sample Attack vs Defense summary
spark.table(TARGET_TABLE).filter(
    F.col("is_core_team") == True
).groupBy(
    "player_display_name", "player_side"
).agg(
    F.count("round_number").alias("rounds"),
    F.sum("kills").alias("kills"),
    F.sum("deaths").alias("deaths"),
    F.round(F.avg("damage"), 1).alias("avg_damage_per_round"),
    F.round(F.sum("kills") / F.when(F.sum("deaths") > 0, F.sum("deaths")).otherwise(1), 2).alias("kd_ratio"),
    F.round(F.avg(F.col("is_round_win").cast("double")) * 100, 1).alias("win_rate_pct")
).orderBy("player_display_name", "player_side").show(20, truncate=False)
