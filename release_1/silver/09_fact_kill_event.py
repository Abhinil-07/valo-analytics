# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Fact Table: `fact_kill_event`
# MAGIC 
# MAGIC - **Grain:** 1 row per kill event in a round
# MAGIC - **Composite Primary Key:** `match_id` + `round_number` + `kill_event_sequence`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.bronze.bronze_kill_event` (Canonical kill timeline, killer, victim, weapon, locations)
# MAGIC   - `valorant.silver.dim_match` (`our_team_color`)
# MAGIC   - `valorant.silver.fact_round` (`attack_team`, `defense_team`, `is_our_team_win`)
# MAGIC   - `valorant.silver.dim_team_roster` (Core squad PUUIDs)
# MAGIC   - `valorant.silver.dim_weapon` (`weapon_category`)
# MAGIC - **Target:** `valorant.silver.fact_kill_event`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id` + `round_number` + `kill_event_sequence`)
# MAGIC 
# MAGIC ### Core Tactical Analytics:
# MAGIC 1. **First Blood / Opening Duel:**
# MAGIC    - `is_opening_kill`: True if first kill of the round (`rn == 1`).
# MAGIC    - `is_opening_death`: True if first victim of the round (`rn == 1`).
# MAGIC 2. **Revenge Trade Kills (4-Second Window):**
# MAGIC    - `is_trade_kill`: True if killer eliminated the exact enemy who killed their teammate within 4,000ms.
# MAGIC    - `was_traded_death`: True if this victim was avenged by a teammate within 4,000ms.
# MAGIC 3. **Spatial Coordinates:**
# MAGIC    - `victim_death_x`, `victim_death_y` for radar heatmap visual rendering.

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "silver"
SOURCE_BRONZE_KILL = f"{CATALOG}.bronze.bronze_kill_event"
SOURCE_FACT_ROUND   = f"{CATALOG}.silver.fact_round"
SOURCE_DIM_MATCH    = f"{CATALOG}.silver.dim_match"
SOURCE_DIM_ROSTER   = f"{CATALOG}.silver.dim_team_roster"
SOURCE_DIM_WEAPON   = f"{CATALOG}.silver.dim_weapon"
TARGET_TABLE        = f"{CATALOG}.{SCHEMA}.fact_kill_event"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_BRONZE_KILL}, {SOURCE_FACT_ROUND}, {SOURCE_DIM_MATCH}")

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
    kill_event_sequence INT NOT NULL,
    kill_time_in_round INT,
    kill_time_seconds DOUBLE,
    kill_time_in_match INT,
    killer_puuid STRING,
    killer_display_name STRING,
    killer_team STRING,
    is_killer_our_team BOOLEAN,
    is_killer_core_team BOOLEAN,
    killer_side STRING,
    victim_puuid STRING,
    victim_display_name STRING,
    victim_team STRING,
    is_victim_our_team BOOLEAN,
    is_victim_core_team BOOLEAN,
    victim_side STRING,
    is_opening_kill BOOLEAN,
    is_opening_death BOOLEAN,
    is_trade_kill BOOLEAN,
    was_traded_death BOOLEAN,
    damage_weapon_name STRING,
    weapon_category STRING,
    secondary_fire_mode BOOLEAN,
    assist_count INT,
    victim_death_x DOUBLE,
    victim_death_y DOUBLE,
    is_our_team_round_win BOOLEAN,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Enriched kill timeline with First Blood, revenge trade kills, and coordinates'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Enrich, and Compute Opening & Trade Flags

# COMMAND ----------
# 1. Active Core Roster PUUIDs
try:
    roster_df = spark.table(SOURCE_DIM_ROSTER).filter(F.col("is_active_roster") == True)
    core_puuids = [row["player_puuid"] for row in roster_df.select("player_puuid").collect()]
except Exception as e:
    print(f"Warning: Could not read {SOURCE_DIM_ROSTER} ({e}). Fallback to empty core squad.")
    core_puuids = []

# 2. Match Context
match_df = spark.table(SOURCE_DIM_MATCH).select(
    F.col("match_id"),
    F.col("our_team_color")
)

# 3. Round Context
round_df = spark.table(SOURCE_FACT_ROUND).select(
    F.col("match_id"),
    F.col("round_number"),
    F.col("attack_team"),
    F.col("defense_team"),
    F.col("is_our_team_win").alias("is_our_team_round_win")
)

# 4. Weapon Categories
weapon_df = spark.table(SOURCE_DIM_WEAPON).select(
    F.col("weapon_name").alias("damage_weapon_name"),
    F.col("weapon_category")
)

# 5. Base Kill Events
kills_raw_df = spark.table(SOURCE_BRONZE_KILL)

# 6. Join Dimension Context
kills_joined_df = kills_raw_df.join(
    match_df, on="match_id", how="inner"
).join(
    round_df, on=["match_id", "round_number"], how="inner"
).join(
    weapon_df, on="damage_weapon_name", how="left"
).withColumn(
    "killer_team_clean", F.initcap(F.col("killer_team"))
).withColumn(
    "victim_team_clean", F.initcap(F.col("victim_team"))
).withColumn(
    "is_killer_our_team", F.col("killer_team_clean") == F.col("our_team_color")
).withColumn(
    "is_victim_our_team", F.col("victim_team_clean") == F.col("our_team_color")
).withColumn(
    "is_killer_core_team", F.col("killer_puuid").isin(core_puuids)
).withColumn(
    "is_victim_core_team", F.col("victim_puuid").isin(core_puuids)
).withColumn(
    "killer_side",
    F.when(F.col("killer_team_clean") == F.col("attack_team"), F.lit("Attack"))
     .when(F.col("killer_team_clean") == F.col("defense_team"), F.lit("Defense"))
     .otherwise(None)
).withColumn(
    "victim_side",
    F.when(F.col("victim_team_clean") == F.col("attack_team"), F.lit("Attack"))
     .when(F.col("victim_team_clean") == F.col("defense_team"), F.lit("Defense"))
     .otherwise(None)
).withColumn(
    "kill_time_seconds", F.round(F.col("kill_time_in_round") / 1000.0, 2)
).withColumn(
    "assist_count", F.coalesce(F.size(F.col("assist_puuids")), F.lit(0))
).withColumn(
    "victim_death_x", F.col("victim_death_location.x")
).withColumn(
    "victim_death_y", F.col("victim_death_location.y")
)

# 7. Window Computations for First Blood & Trade Kills
round_timeline_window = Window.partitionBy("match_id", "round_number").orderBy(
    F.col("kill_time_in_round"), F.col("kill_event_sequence")
)

staged_kills_df = kills_joined_df.withColumn(
    "kill_rn_in_round", F.row_number().over(round_timeline_window)
).withColumn(
    # First Blood / Opening Duel
    "is_opening_kill", F.col("kill_rn_in_round") == 1
).withColumn(
    "is_opening_death", F.col("kill_rn_in_round") == 1
).withColumn(
    # Trade Kill Analysis: Look at the previous kill in the round (LAG)
    "prev_killer_puuid", F.lag("killer_puuid", 1).over(round_timeline_window)
).withColumn(
    "prev_victim_team", F.lag("victim_team_clean", 1).over(round_timeline_window)
).withColumn(
    "prev_kill_time", F.lag("kill_time_in_round", 1).over(round_timeline_window)
).withColumn(
    # Is this kill a revenge trade? (Killer avenged teammate on the exact enemy who killed them <= 4s ago)
    "is_trade_kill",
    (F.col("killer_team_clean") == F.col("prev_victim_team")) &
    (F.col("victim_puuid") == F.col("prev_killer_puuid")) &
    ((F.col("kill_time_in_round") - F.col("prev_kill_time")) <= 4000)
).withColumn(
    # Look at the next kill in the round (LEAD) to see if this victim was traded
    "next_killer_team", F.lead("killer_team_clean", 1).over(round_timeline_window)
).withColumn(
    "next_victim_puuid", F.lead("victim_puuid", 1).over(round_timeline_window)
).withColumn(
    "next_kill_time", F.lead("kill_time_in_round", 1).over(round_timeline_window)
).withColumn(
    # Was this death traded?
    "was_traded_death",
    (F.col("next_killer_team") == F.col("victim_team_clean")) &
    (F.col("next_victim_puuid") == F.col("killer_puuid")) &
    ((F.col("next_kill_time") - F.col("kill_time_in_round")) <= 4000)
).select(
    "match_id",
    "round_number",
    "kill_event_sequence",
    "kill_time_in_round",
    "kill_time_seconds",
    "kill_time_in_match",
    "killer_puuid",
    "killer_display_name",
    F.col("killer_team_clean").alias("killer_team"),
    "is_killer_our_team",
    "is_killer_core_team",
    "killer_side",
    "victim_puuid",
    "victim_display_name",
    F.col("victim_team_clean").alias("victim_team"),
    "is_victim_our_team",
    "is_victim_core_team",
    "victim_side",
    "is_opening_kill",
    "is_opening_death",
    F.coalesce(F.col("is_trade_kill"), F.lit(False)).alias("is_trade_kill"),
    F.coalesce(F.col("was_traded_death"), F.lit(False)).alias("was_traded_death"),
    "damage_weapon_name",
    F.coalesce(F.col("weapon_category"), F.lit("Ability/Other")).alias("weapon_category"),
    F.coalesce(F.col("secondary_fire_mode"), F.lit(False)).alias("secondary_fire_mode"),
    "assist_count",
    "victim_death_x",
    "victim_death_y",
    "is_our_team_round_win",
    F.current_timestamp().alias("updated_at")
)

staged_kills_df.createOrReplaceTempView("staged_fact_kill_event")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `fact_kill_event`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_fact_kill_event AS source
ON target.match_id = source.match_id 
   AND target.round_number = source.round_number 
   AND target.kill_event_sequence = source.kill_event_sequence
WHEN MATCHED THEN
  UPDATE SET
    target.kill_time_in_round = source.kill_time_in_round,
    target.kill_time_seconds = source.kill_time_seconds,
    target.kill_time_in_match = source.kill_time_in_match,
    target.killer_puuid = source.killer_puuid,
    target.killer_display_name = source.killer_display_name,
    target.killer_team = source.killer_team,
    target.is_killer_our_team = source.is_killer_our_team,
    target.is_killer_core_team = source.is_killer_core_team,
    target.killer_side = source.killer_side,
    target.victim_puuid = source.victim_puuid,
    target.victim_display_name = source.victim_display_name,
    target.victim_team = source.victim_team,
    target.is_victim_our_team = source.is_victim_our_team,
    target.is_victim_core_team = source.is_victim_core_team,
    target.victim_side = source.victim_side,
    target.is_opening_kill = source.is_opening_kill,
    target.is_opening_death = source.is_opening_death,
    target.is_trade_kill = source.is_trade_kill,
    target.was_traded_death = source.was_traded_death,
    target.damage_weapon_name = source.damage_weapon_name,
    target.weapon_category = source.weapon_category,
    target.secondary_fire_mode = source.secondary_fire_mode,
    target.assist_count = source.assist_count,
    target.victim_death_x = source.victim_death_x,
    target.victim_death_y = source.victim_death_y,
    target.is_our_team_round_win = source.is_our_team_round_win,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id, round_number, kill_event_sequence, kill_time_in_round, kill_time_seconds,
    kill_time_in_match, killer_puuid, killer_display_name, killer_team, is_killer_our_team,
    is_killer_core_team, killer_side, victim_puuid, victim_display_name, victim_team,
    is_victim_our_team, is_victim_core_team, victim_side, is_opening_kill, is_opening_death,
    is_trade_kill, was_traded_death, damage_weapon_name, weapon_category, secondary_fire_mode,
    assist_count, victim_death_x, victim_death_y, is_our_team_round_win, updated_at
  )
  VALUES (
    source.match_id, source.round_number, source.kill_event_sequence, source.kill_time_in_round, source.kill_time_seconds,
    source.kill_time_in_match, source.killer_puuid, source.killer_display_name, source.killer_team, source.is_killer_our_team,
    source.is_killer_core_team, source.killer_side, source.victim_puuid, source.victim_display_name, source.victim_team,
    source.is_victim_our_team, source.is_victim_core_team, source.victim_side, source.is_opening_kill, source.is_opening_death,
    source.is_trade_kill, source.was_traded_death, source.damage_weapon_name, source.weapon_category, source.secondary_fire_mode,
    source.assist_count, source.victim_death_x, source.victim_death_y, source.is_our_team_round_win, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification and Summary Metrics

# COMMAND ----------
total_kills = spark.table(TARGET_TABLE).count()
first_bloods = spark.table(TARGET_TABLE).filter(F.col("is_opening_kill") == True).count()
trade_kills = spark.table(TARGET_TABLE).filter(F.col("is_trade_kill") == True).count()

print(f"Total Kill Events: {total_kills}")
print(f"Total Opening Kills (First Bloods): {first_bloods}")
print(f"Total Direct Trade Kills: {trade_kills}")

# First Blood Leaderboard for Core Squad
print("Core Squad First Blood Leaders & Round Conversion:")
spark.table(TARGET_TABLE).filter(
    (F.col("is_killer_core_team") == True) & (F.col("is_opening_kill") == True)
).groupBy(
    "killer_display_name"
).agg(
    F.count("match_id").alias("first_bloods"),
    F.round(F.avg(F.col("is_our_team_round_win").cast("double")) * 100, 1).alias("first_blood_win_pct")
).orderBy(F.col("first_bloods").desc()).show(10, truncate=False)
