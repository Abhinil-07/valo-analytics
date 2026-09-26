# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Fact Table: `fact_match_player`
# MAGIC 
# MAGIC - **Grain:** 1 row per player per match (10 rows per standard 5v5 match)
# MAGIC - **Composite Primary Key:** `match_id` + `player_puuid`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.bronze.bronze_player` (KDA, combat stats, ability casts, rank)
# MAGIC   - `valorant.silver.dim_match` (Rounds played, our_team_color, match outcome)
# MAGIC   - `valorant.silver.dim_team_roster` (Core squad roster flag)
# MAGIC - **Target:** `valorant.silver.fact_match_player`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id` + `player_puuid`)
# MAGIC 
# MAGIC ### Core Analytical Derivations (SRS Section 4, 18, 23 & 29):
# MAGIC 1. **Team Perspective:**
# MAGIC    - `is_our_team`: Boolean indicating if the player belongs to your squad (`team == our_team_color`).
# MAGIC    - `team_perspective`: `'OUR_TEAM'` vs `'OPPONENT'`.
# MAGIC 2. **Combat Performance KPIs:**
# MAGIC    - `kill_death_ratio`: Safe division `kills / greatest(deaths, 1)`
# MAGIC    - `kill_differential`: `kills - deaths`
# MAGIC    - `average_combat_score` (ACS): `round(score / rounds_played, 1)`
# MAGIC    - `average_damage_per_round` (ADR): `round(damage_made / rounds_played, 1)`
# MAGIC    - `damage_differential`: `damage_made - damage_received`
# MAGIC 3. **Shooting Accuracy (Shot Distribution):**
# MAGIC    - `headshot_pct`: `round((headshots / (headshots + bodyshots + legshots)) * 100.0, 1)`
# MAGIC    - `bodyshot_pct`: `round((bodyshots / (headshots + bodyshots + legshots)) * 100.0, 1)`
# MAGIC    - `legshot_pct`: `round((legshots / (headshots + bodyshots + legshots)) * 100.0, 1)`
# MAGIC 4. **Utility & Economy:**
# MAGIC    - `total_ability_casts`: `x_casts + e_casts + q_casts + c_casts`
# MAGIC    - `spent_overall`, `spent_average`, `loadout_value_average`

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "silver"
SOURCE_BRONZE_PLAYER = f"{CATALOG}.bronze.bronze_player"
SOURCE_DIM_MATCH = f"{CATALOG}.silver.dim_match"
SOURCE_DIM_ROSTER = f"{CATALOG}.silver.dim_team_roster"
SOURCE_DIM_PLAYER = f"{CATALOG}.silver.dim_player"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.fact_match_player"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_BRONZE_PLAYER}, {SOURCE_DIM_MATCH}, {SOURCE_DIM_PLAYER}")

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
    team STRING,
    team_perspective STRING,
    is_our_team BOOLEAN,
    is_core_team BOOLEAN,
    agent_name STRING,
    competitive_rank STRING,
    account_level INT,
    score INT,
    average_combat_score DOUBLE,
    kills INT,
    deaths INT,
    assists INT,
    kill_death_ratio DOUBLE,
    kill_differential INT,
    damage_made INT,
    damage_received INT,
    damage_differential INT,
    average_damage_per_round DOUBLE,
    headshots INT,
    bodyshots INT,
    legshots INT,
    headshot_pct DOUBLE,
    bodyshot_pct DOUBLE,
    legshot_pct DOUBLE,
    ultimate_casts INT,
    signature_casts INT,
    ability_1_casts INT,
    ability_2_casts INT,
    total_ability_casts INT,
    spent_overall INT,
    spent_average DOUBLE,
    loadout_value_average DOUBLE,
    is_match_win BOOLEAN,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Enriched player match scorecard: K/D, ACS, ADR, Headshot %, ability casts, and perspective'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Enrich, and Calculate Performance Metrics

# COMMAND ----------
# 1. Active Core Roster PUUIDs
try:
    roster_df = spark.table(SOURCE_DIM_ROSTER).filter(F.col("is_active_roster") == True)
    core_puuids = [row["player_puuid"] for row in roster_df.select("player_puuid").collect()]
except Exception as e:
    print(f"Warning: Could not read {SOURCE_DIM_ROSTER} ({e}). Fallback to empty core squad.")
    core_puuids = []

# 2. Read Match Context from dim_match
match_df = spark.table(SOURCE_DIM_MATCH).select(
    F.col("match_id"),
    F.col("rounds_played"),
    F.col("our_team_color"),
    F.col("is_our_team_win")
)

# 3. Read Player Performance from bronze_player
player_df = spark.table(SOURCE_BRONZE_PLAYER).select(
    F.col("match_id"),
    F.col("player_puuid"),
    F.col("player_name"),
    F.col("player_tag"),
    F.initcap(F.col("team")).alias("team"),
    F.col("character").alias("agent_name"),
    F.col("level").alias("account_level"),
    F.col("current_tier_patched").alias("competitive_rank"),
    F.col("score"),
    F.col("kills"),
    F.col("deaths"),
    F.col("assists"),
    F.col("bodyshots"),
    F.col("headshots"),
    F.col("legshots"),
    F.col("damage_made"),
    F.col("damage_received"),
    F.col("x_casts").alias("ultimate_casts"),
    F.col("e_casts").alias("signature_casts"),
    F.col("q_casts").alias("ability_1_casts"),
    F.col("c_casts").alias("ability_2_casts"),
    F.col("spent_overall"),
    F.col("spent_average"),
    F.col("loadout_value_average")
).filter(F.col("player_puuid").isNotNull())

# Canonical Player Identity Mapping from dim_player
try:
    dim_player_df = spark.table(SOURCE_DIM_PLAYER).select(
        F.col("player_puuid"),
        F.col("current_display_name").alias("canonical_display_name")
    )
except Exception as e:
    print(f"Warning: Could not read {SOURCE_DIM_PLAYER} directly ({e}). Using raw match name fallback.")
    dim_player_df = None

# 4. Join Player Performance with Match Context and Derive Metrics
base_joined_df = player_df.join(
    match_df,
    on="match_id",
    how="inner"
)

if dim_player_df is not None:
    base_joined_df = base_joined_df.join(
        dim_player_df,
        on="player_puuid",
        how="left"
    )
    staged_fact_player_df = base_joined_df.withColumn(
        "current_display_name",
        F.coalesce(F.col("canonical_display_name"), F.concat_ws("#", F.col("player_name"), F.col("player_tag")))
    ).drop("canonical_display_name")
else:
    staged_fact_player_df = base_joined_df.withColumn(
        "current_display_name", F.concat_ws("#", F.col("player_name"), F.col("player_tag"))
    )

staged_fact_player_df = staged_fact_player_df.withColumn(
    "is_core_team", F.col("player_puuid").isin(core_puuids)
).withColumn(
    # Perspective Mapping
    "is_our_team",
    F.when(F.col("our_team_color").isNotNull(), F.col("team") == F.col("our_team_color"))
     .otherwise(None)
).withColumn(
    "team_perspective",
    F.when(F.col("is_our_team") == True, F.lit("OUR_TEAM"))
     .when(F.col("is_our_team") == False, F.lit("OPPONENT"))
     .otherwise(F.lit("UNKNOWN"))
).withColumn(
    # Match Result for this player
    "is_match_win",
    F.when(F.col("is_our_team") == True, F.col("is_our_team_win"))
     .when(F.col("is_our_team") == False, ~F.col("is_our_team_win"))
     .otherwise(None)
).withColumn(
    # Safe rounds divisor (prevent divide-by-zero)
    "safe_rounds", F.when(F.col("rounds_played") > 0, F.col("rounds_played")).otherwise(1)
).withColumn(
    # ACS (Average Combat Score)
    "average_combat_score",
    F.round(F.col("score") / F.col("safe_rounds"), 1)
).withColumn(
    # ADR (Average Damage per Round)
    "average_damage_per_round",
    F.round(F.col("damage_made") / F.col("safe_rounds"), 1)
).withColumn(
    # Damage Differential
    "damage_differential",
    F.col("damage_made") - F.col("damage_received")
).withColumn(
    # K/D Ratio & Differential
    "kill_death_ratio",
    F.round(F.col("kills") / F.when(F.col("deaths") > 0, F.col("deaths")).otherwise(1), 2)
).withColumn(
    "kill_differential",
    F.col("kills") - F.col("deaths")
).withColumn(
    # Total shots landed
    "total_shots_hit",
    F.col("headshots") + F.col("bodyshots") + F.col("legshots")
).withColumn(
    # Safe shots divisor
    "safe_shots",
    F.when(F.col("total_shots_hit") > 0, F.col("total_shots_hit")).otherwise(1)
).withColumn(
    # Shot accuracy percentages
    "headshot_pct",
    F.round((F.col("headshots") / F.col("safe_shots")) * 100.0, 1)
).withColumn(
    "bodyshot_pct",
    F.round((F.col("bodyshots") / F.col("safe_shots")) * 100.0, 1)
).withColumn(
    "legshot_pct",
    F.round((F.col("legshots") / F.col("safe_shots")) * 100.0, 1)
).withColumn(
    # Total ability casts
    "total_ability_casts",
    F.coalesce(F.col("ultimate_casts"), F.lit(0)) +
    F.coalesce(F.col("signature_casts"), F.lit(0)) +
    F.coalesce(F.col("ability_1_casts"), F.lit(0)) +
    F.coalesce(F.col("ability_2_casts"), F.lit(0))
).select(
    "match_id",
    "player_puuid",
    "player_name",
    "player_tag",
    "current_display_name",
    "team",
    "team_perspective",
    "is_our_team",
    "is_core_team",
    "agent_name",
    "competitive_rank",
    "account_level",
    "score",
    "average_combat_score",
    "kills",
    "deaths",
    "assists",
    "kill_death_ratio",
    "kill_differential",
    "damage_made",
    "damage_received",
    "damage_differential",
    "average_damage_per_round",
    "headshots",
    "bodyshots",
    "legshots",
    "headshot_pct",
    "bodyshot_pct",
    "legshot_pct",
    "ultimate_casts",
    "signature_casts",
    "ability_1_casts",
    "ability_2_casts",
    "total_ability_casts",
    "spent_overall",
    "spent_average",
    "loadout_value_average",
    "is_match_win",
    F.current_timestamp().alias("updated_at")
)

staged_fact_player_df.createOrReplaceTempView("staged_fact_match_player")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `fact_match_player`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_fact_match_player AS source
ON target.match_id = source.match_id AND target.player_puuid = source.player_puuid
WHEN MATCHED THEN
  UPDATE SET
    target.player_name = source.player_name,
    target.player_tag = source.player_tag,
    target.current_display_name = source.current_display_name,
    target.team = source.team,
    target.team_perspective = source.team_perspective,
    target.is_our_team = source.is_our_team,
    target.is_core_team = source.is_core_team,
    target.agent_name = source.agent_name,
    target.competitive_rank = source.competitive_rank,
    target.account_level = source.account_level,
    target.score = source.score,
    target.average_combat_score = source.average_combat_score,
    target.kills = source.kills,
    target.deaths = source.deaths,
    target.assists = source.assists,
    target.kill_death_ratio = source.kill_death_ratio,
    target.kill_differential = source.kill_differential,
    target.damage_made = source.damage_made,
    target.damage_received = source.damage_received,
    target.damage_differential = source.damage_differential,
    target.average_damage_per_round = source.average_damage_per_round,
    target.headshots = source.headshots,
    target.bodyshots = source.bodyshots,
    target.legshots = source.legshots,
    target.headshot_pct = source.headshot_pct,
    target.bodyshot_pct = source.bodyshot_pct,
    target.legshot_pct = source.legshot_pct,
    target.ultimate_casts = source.ultimate_casts,
    target.signature_casts = source.signature_casts,
    target.ability_1_casts = source.ability_1_casts,
    target.ability_2_casts = source.ability_2_casts,
    target.total_ability_casts = source.total_ability_casts,
    target.spent_overall = source.spent_overall,
    target.spent_average = source.spent_average,
    target.loadout_value_average = source.loadout_value_average,
    target.is_match_win = source.is_match_win,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id,
    player_puuid,
    player_name,
    player_tag,
    current_display_name,
    team,
    team_perspective,
    is_our_team,
    is_core_team,
    agent_name,
    competitive_rank,
    account_level,
    score,
    average_combat_score,
    kills,
    deaths,
    assists,
    kill_death_ratio,
    kill_differential,
    damage_made,
    damage_received,
    damage_differential,
    average_damage_per_round,
    headshots,
    bodyshots,
    legshots,
    headshot_pct,
    bodyshot_pct,
    legshot_pct,
    ultimate_casts,
    signature_casts,
    ability_1_casts,
    ability_2_casts,
    total_ability_casts,
    spent_overall,
    spent_average,
    loadout_value_average,
    is_match_win,
    updated_at
  )
  VALUES (
    source.match_id,
    source.player_puuid,
    source.player_name,
    source.player_tag,
    source.current_display_name,
    source.team,
    source.team_perspective,
    source.is_our_team,
    source.is_core_team,
    source.agent_name,
    source.competitive_rank,
    source.account_level,
    source.score,
    source.average_combat_score,
    source.kills,
    source.deaths,
    source.assists,
    source.kill_death_ratio,
    source.kill_differential,
    source.damage_made,
    source.damage_received,
    source.damage_differential,
    source.average_damage_per_round,
    source.headshots,
    source.bodyshots,
    source.legshots,
    source.headshot_pct,
    source.bodyshot_pct,
    source.legshot_pct,
    source.ultimate_casts,
    source.signature_casts,
    source.ability_1_casts,
    source.ability_2_casts,
    source.total_ability_casts,
    source.spent_overall,
    source.spent_average,
    source.loadout_value_average,
    source.is_match_win,
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
core_rows = spark.table(TARGET_TABLE).filter(F.col("is_core_team") == True).count()

print(f"Total Player Match Scorecard Rows: {total_rows}")
print(f"Total Unique Matches: {total_matches}")
print(f"Core Squad Records: {core_rows}")

# Sample display: Top performers
spark.table(TARGET_TABLE).filter(
    F.col("is_core_team") == True
).select(
    "match_id",
    "current_display_name",
    "agent_name",
    "team_perspective",
    "kills",
    "deaths",
    "assists",
    "kill_death_ratio",
    "average_combat_score",
    "average_damage_per_round",
    "headshot_pct"
).orderBy(F.col("average_combat_score").desc()).show(10, truncate=False)
