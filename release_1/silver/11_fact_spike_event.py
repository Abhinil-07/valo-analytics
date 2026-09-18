# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Fact Table: `fact_spike_event`
# MAGIC 
# MAGIC - **Grain:** 1 row per spike plant or defuse event in a round
# MAGIC - **Composite Primary Key:** `match_id` + `round_number` + `event_type` + `event_sequence`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.bronze.bronze_plant_event` (Spike plants, site, player, coordinates)
# MAGIC   - `valorant.bronze.bronze_defuse_event` (Spike defusals, site, player, coordinates)
# MAGIC   - `valorant.silver.dim_match` (`our_team_color`)
# MAGIC   - `valorant.silver.fact_round` (`round_winner`, `is_our_team_win`, `end_type`)
# MAGIC   - `valorant.silver.dim_team_roster` (Core squad PUUIDs)
# MAGIC - **Target:** `valorant.silver.fact_spike_event`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id` + `round_number` + `event_type` + `event_sequence`)

# COMMAND ----------
import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "silver"
SOURCE_PLANT_BRONZE  = f"{CATALOG}.bronze.bronze_plant_event"
SOURCE_DEFUSE_BRONZE = f"{CATALOG}.bronze.bronze_defuse_event"
SOURCE_FACT_ROUND    = f"{CATALOG}.silver.fact_round"
SOURCE_DIM_MATCH     = f"{CATALOG}.silver.dim_match"
SOURCE_DIM_ROSTER    = f"{CATALOG}.silver.dim_team_roster"
TARGET_TABLE         = f"{CATALOG}.{SCHEMA}.fact_spike_event"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_PLANT_BRONZE}, {SOURCE_DEFUSE_BRONZE}, {SOURCE_FACT_ROUND}")

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
    event_type STRING NOT NULL,
    event_sequence INT NOT NULL,
    event_time_in_round INT,
    event_time_seconds DOUBLE,
    site STRING,
    player_puuid STRING,
    player_display_name STRING,
    player_team STRING,
    team_perspective STRING,
    is_our_team BOOLEAN,
    is_core_team BOOLEAN,
    event_location_x DOUBLE,
    event_location_y DOUBLE,
    round_winner STRING,
    is_our_team_round_win BOOLEAN,
    round_end_type STRING,
    is_spike_converted BOOLEAN,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Unified spike plant and defuse facts for site execution and post-plant analytics'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Unify, and Enrich Plant & Defuse Events

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
    F.col("round_winner"),
    F.col("is_our_team_win").alias("is_our_team_round_win"),
    F.col("end_type").alias("round_end_type")
)

# 4. Plant Events
plant_df = spark.table(SOURCE_PLANT_BRONZE).select(
    F.col("match_id"),
    F.col("round_number"),
    F.lit("PLANT").alias("event_type"),
    F.col("plant_event_sequence").alias("event_sequence"),
    F.col("plant_time_in_round").alias("event_time_in_round"),
    F.col("plant_site").alias("site"),
    F.col("player_puuid"),
    F.col("player_display_name"),
    F.initcap(F.col("player_team")).alias("player_team"),
    F.col("plant_location.x").alias("event_location_x"),
    F.col("plant_location.y").alias("event_location_y")
)

# 5. Defuse Events
defuse_df = spark.table(SOURCE_DEFUSE_BRONZE).select(
    F.col("match_id"),
    F.col("round_number"),
    F.lit("DEFUSE").alias("event_type"),
    F.col("defuse_event_sequence").alias("event_sequence"),
    F.col("defuse_time_in_round").alias("event_time_in_round"),
    F.col("defuse_site").alias("site"),
    F.col("player_puuid"),
    F.col("player_display_name"),
    F.initcap(F.col("player_team")).alias("player_team"),
    F.col("defuse_location.x").alias("event_location_x"),
    F.col("defuse_location.y").alias("event_location_y")
)

# 6. Union Plants and Defuses
unified_spike_df = plant_df.unionByName(defuse_df)

# 7. Join Dimension Context and Derive Metrics
staged_spike_df = unified_spike_df.join(
    match_df, on="match_id", how="inner"
).join(
    round_df, on=["match_id", "round_number"], how="inner"
).withColumn(
    "event_time_seconds", F.round(F.col("event_time_in_round") / 1000.0, 2)
).withColumn(
    "is_our_team", F.col("player_team") == F.col("our_team_color")
).withColumn(
    "team_perspective",
    F.when(F.col("is_our_team") == True, F.lit("OUR_TEAM")).otherwise(F.lit("OPPONENT"))
).withColumn(
    "is_core_team", F.col("player_puuid").isin(core_puuids)
).withColumn(
    # Spike Conversion:
    # - If PLANT: did the planter's team win the round (detonation or elimination)?
    # - If DEFUSE: defuse event is inherently a successful conversion
    "is_spike_converted",
    F.when(F.col("event_type") == "DEFUSE", F.lit(True))
     .when(F.col("event_type") == "PLANT", F.col("player_team") == F.col("round_winner"))
     .otherwise(F.lit(False))
).select(
    "match_id",
    "round_number",
    "event_type",
    "event_sequence",
    "event_time_in_round",
    "event_time_seconds",
    "site",
    "player_puuid",
    "player_display_name",
    "player_team",
    "team_perspective",
    "is_our_team",
    "is_core_team",
    "event_location_x",
    "event_location_y",
    "round_winner",
    "is_our_team_round_win",
    "round_end_type",
    "is_spike_converted",
    F.current_timestamp().alias("updated_at")
)

staged_spike_df.createOrReplaceTempView("staged_fact_spike_event")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `fact_spike_event`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_fact_spike_event AS source
ON target.match_id = source.match_id 
   AND target.round_number = source.round_number 
   AND target.event_type = source.event_type
   AND target.event_sequence = source.event_sequence
WHEN MATCHED THEN
  UPDATE SET
    target.event_time_in_round = source.event_time_in_round,
    target.event_time_seconds = source.event_time_seconds,
    target.site = source.site,
    target.player_puuid = source.player_puuid,
    target.player_display_name = source.player_display_name,
    target.player_team = source.player_team,
    target.team_perspective = source.team_perspective,
    target.is_our_team = source.is_our_team,
    target.is_core_team = source.is_core_team,
    target.event_location_x = source.event_location_x,
    target.event_location_y = source.event_location_y,
    target.round_winner = source.round_winner,
    target.is_our_team_round_win = source.is_our_team_round_win,
    target.round_end_type = source.round_end_type,
    target.is_spike_converted = source.is_spike_converted,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id, round_number, event_type, event_sequence, event_time_in_round,
    event_time_seconds, site, player_puuid, player_display_name, player_team,
    team_perspective, is_our_team, is_core_team, event_location_x, event_location_y,
    round_winner, is_our_team_round_win, round_end_type, is_spike_converted, updated_at
  )
  VALUES (
    source.match_id, source.round_number, source.event_type, source.event_sequence, source.event_time_in_round,
    source.event_time_seconds, source.site, source.player_puuid, source.player_display_name, source.player_team,
    source.team_perspective, source.is_our_team, source.is_core_team, source.event_location_x, source.event_location_y,
    source.round_winner, source.is_our_team_round_win, source.round_end_type, source.is_spike_converted, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification and Summary Metrics

# COMMAND ----------
total_spikes = spark.table(TARGET_TABLE).count()
total_plants = spark.table(TARGET_TABLE).filter(F.col("event_type") == "PLANT").count()
total_defuses = spark.table(TARGET_TABLE).filter(F.col("event_type") == "DEFUSE").count()

print(f"Total Spike Events: {total_spikes}")
print(f"Total Plants: {total_plants}")
print(f"Total Defuses: {total_defuses}")

# Site Execution Success Rate for Our Squad
print("Our Squad Plant Conversion by Site:")
spark.table(TARGET_TABLE).filter(
    (F.col("is_our_team") == True) & (F.col("event_type") == "PLANT")
).groupBy(
    "site"
).agg(
    F.count("match_id").alias("total_plants"),
    F.round(F.avg(F.col("is_spike_converted").cast("double")) * 100, 1).alias("post_plant_win_pct")
).orderBy(F.col("total_plants").desc()).show(10, truncate=False)
