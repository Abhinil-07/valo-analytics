# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Fact Table: `fact_damage_event`
# MAGIC 
# MAGIC - **Grain:** 1 row per damage engagement per round
# MAGIC - **Composite Primary Key:** `match_id` + `round_number` + `damage_event_sequence` + `attacker_puuid`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.bronze.bronze_damage_event` (Attacker, receiver, damage, hit distribution)
# MAGIC   - `valorant.silver.dim_match` (`our_team_color`)
# MAGIC   - `valorant.silver.fact_round` (`attack_team`, `defense_team`, `is_our_team_win`)
# MAGIC   - `valorant.silver.dim_team_roster` (Core squad PUUIDs)
# MAGIC - **Target:** `valorant.silver.fact_damage_event`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id` + `round_number` + `damage_event_sequence` + `attacker_puuid`)

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "silver"
SOURCE_BRONZE_DAMAGE = f"{CATALOG}.bronze.bronze_damage_event"
SOURCE_FACT_ROUND    = f"{CATALOG}.silver.fact_round"
SOURCE_DIM_MATCH     = f"{CATALOG}.silver.dim_match"
SOURCE_DIM_ROSTER    = f"{CATALOG}.silver.dim_team_roster"
TARGET_TABLE         = f"{CATALOG}.{SCHEMA}.fact_damage_event"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_BRONZE_DAMAGE}, {SOURCE_FACT_ROUND}, {SOURCE_DIM_MATCH}")

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
    damage_event_sequence INT NOT NULL,
    attacker_puuid STRING NOT NULL,
    attacker_display_name STRING,
    attacker_team STRING,
    is_attacker_our_team BOOLEAN,
    is_attacker_core_team BOOLEAN,
    attacker_side STRING,
    receiver_puuid STRING,
    receiver_display_name STRING,
    receiver_team STRING,
    is_receiver_our_team BOOLEAN,
    is_receiver_core_team BOOLEAN,
    receiver_side STRING,
    damage INT,
    headshots INT,
    bodyshots INT,
    legshots INT,
    total_shots INT,
    engagement_headshot_pct DOUBLE,
    is_our_team_round_win BOOLEAN,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Granular damage exchanges between players: damage dealt, shot distributions, and side context'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Enrich, and Calculate Engagement Accuracy

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

# 4. Base Damage Events
damage_raw_df = spark.table(SOURCE_BRONZE_DAMAGE).filter(F.col("attacker_puuid").isNotNull())

# 5. Join and Derive Metrics
staged_damage_df = damage_raw_df.join(
    match_df, on="match_id", how="inner"
).join(
    round_df, on=["match_id", "round_number"], how="inner"
).withColumn(
    "attacker_team_clean", F.initcap(F.col("attacker_team"))
).withColumn(
    "receiver_team_clean", F.initcap(F.col("receiver_team"))
).withColumn(
    "is_attacker_our_team", F.col("attacker_team_clean") == F.col("our_team_color")
).withColumn(
    "is_receiver_our_team", F.col("receiver_team_clean") == F.col("our_team_color")
).withColumn(
    "is_attacker_core_team", F.col("attacker_puuid").isin(core_puuids)
).withColumn(
    "is_receiver_core_team", F.col("receiver_puuid").isin(core_puuids)
).withColumn(
    "attacker_side",
    F.when(F.col("attacker_team_clean") == F.col("attack_team"), F.lit("Attack"))
     .when(F.col("attacker_team_clean") == F.col("defense_team"), F.lit("Defense"))
     .otherwise(None)
).withColumn(
    "receiver_side",
    F.when(F.col("receiver_team_clean") == F.col("attack_team"), F.lit("Attack"))
     .when(F.col("receiver_team_clean") == F.col("defense_team"), F.lit("Defense"))
     .otherwise(None)
).withColumn(
    "total_shots",
    F.coalesce(F.col("headshots"), F.lit(0)) +
    F.coalesce(F.col("bodyshots"), F.lit(0)) +
    F.coalesce(F.col("legshots"), F.lit(0))
).withColumn(
    "engagement_headshot_pct",
    F.when(F.col("total_shots") > 0,
           F.round((F.col("headshots") / F.col("total_shots")) * 100.0, 1)
    ).otherwise(0.0)
).select(
    "match_id",
    "round_number",
    "damage_event_sequence",
    "attacker_puuid",
    "attacker_display_name",
    F.col("attacker_team_clean").alias("attacker_team"),
    "is_attacker_our_team",
    "is_attacker_core_team",
    "attacker_side",
    "receiver_puuid",
    "receiver_display_name",
    F.col("receiver_team_clean").alias("receiver_team"),
    "is_receiver_our_team",
    "is_receiver_core_team",
    "receiver_side",
    "damage",
    "headshots",
    "bodyshots",
    "legshots",
    "total_shots",
    "engagement_headshot_pct",
    "is_our_team_round_win",
    F.current_timestamp().alias("updated_at")
)

staged_damage_df.createOrReplaceTempView("staged_fact_damage_event")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `fact_damage_event`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_fact_damage_event AS source
ON target.match_id = source.match_id 
   AND target.round_number = source.round_number 
   AND target.damage_event_sequence = source.damage_event_sequence
   AND target.attacker_puuid = source.attacker_puuid
WHEN MATCHED THEN
  UPDATE SET
    target.attacker_display_name = source.attacker_display_name,
    target.attacker_team = source.attacker_team,
    target.is_attacker_our_team = source.is_attacker_our_team,
    target.is_attacker_core_team = source.is_attacker_core_team,
    target.attacker_side = source.attacker_side,
    target.receiver_puuid = source.receiver_puuid,
    target.receiver_display_name = source.receiver_display_name,
    target.receiver_team = source.receiver_team,
    target.is_receiver_our_team = source.is_receiver_our_team,
    target.is_receiver_core_team = source.is_receiver_core_team,
    target.receiver_side = source.receiver_side,
    target.damage = source.damage,
    target.headshots = source.headshots,
    target.bodyshots = source.bodyshots,
    target.legshots = source.legshots,
    target.total_shots = source.total_shots,
    target.engagement_headshot_pct = source.engagement_headshot_pct,
    target.is_our_team_round_win = source.is_our_team_round_win,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id, round_number, damage_event_sequence, attacker_puuid, attacker_display_name,
    attacker_team, is_attacker_our_team, is_attacker_core_team, attacker_side, receiver_puuid,
    receiver_display_name, receiver_team, is_receiver_our_team, is_receiver_core_team, receiver_side,
    damage, headshots, bodyshots, legshots, total_shots, engagement_headshot_pct,
    is_our_team_round_win, updated_at
  )
  VALUES (
    source.match_id, source.round_number, source.damage_event_sequence, source.attacker_puuid, source.attacker_display_name,
    source.attacker_team, source.is_attacker_our_team, source.is_attacker_core_team, source.attacker_side, source.receiver_puuid,
    source.receiver_display_name, source.receiver_team, source.is_receiver_our_team, source.is_receiver_core_team, source.receiver_side,
    source.damage, source.headshots, source.bodyshots, source.legshots, source.total_shots, source.engagement_headshot_pct,
    source.is_our_team_round_win, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification and Summary Metrics

# COMMAND ----------
total_damages = spark.table(TARGET_TABLE).count()
total_damage_dealt = spark.table(TARGET_TABLE).agg(F.sum("damage")).collect()[0][0]

print(f"Total Damage Events: {total_damages}")
print(f"Total Damage Dealt across all events: {total_damage_dealt}")

# Core Squad Duel Efficiency
spark.table(TARGET_TABLE).filter(
    F.col("is_attacker_core_team") == True
).groupBy(
    "attacker_display_name"
).agg(
    F.count("match_id").alias("engagements"),
    F.sum("damage").alias("total_damage"),
    F.sum("headshots").alias("headshots"),
    F.sum("total_shots").alias("total_shots"),
    F.round((F.sum("headshots") / F.sum("total_shots")) * 100.0, 1).alias("duel_headshot_pct")
).orderBy(F.col("total_damage").desc()).show(10, truncate=False)
