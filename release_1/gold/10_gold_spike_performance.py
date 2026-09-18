# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_spike_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per `map_name` per bomb site (`site`: `'A'`, `'B'`, `'C'`)
# MAGIC - **Composite Primary Key:** `map_name` + `site`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.silver.fact_spike_event` (Plants, defusals, bomb sites, conversions, elapsed times)
# MAGIC   - `valorant.silver.dim_match` (Map name mapping)
# MAGIC - **Target:** `valorant.gold.gold_spike_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `map_name` + `site`)
# MAGIC 
# MAGIC ### Business Objectives & Databricks Dashboard Widgets:
# MAGIC - **Site Execution Preference:** Identifying whether the squad attacks A, B, or C site more frequently per map.
# MAGIC - **Post-Plant Conversion Rates:** Measuring round win % once the spike is successfully planted on a site.
# MAGIC - **Defense Retake Success:** Measuring how frequently the squad recovers the site and defuses the bomb on defense.
# MAGIC - **Attack Execution Pace:** Average seconds elapsed into the round before the spike is planted.
# MAGIC - **Objective Leaders:** Primary spike planters and retake defusers per site.

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
SOURCE_FACT_SPIKE = f"{CATALOG}.silver.fact_spike_event"
SOURCE_DIM_MATCH  = f"{CATALOG}.silver.dim_match"
TARGET_TABLE      = f"{CATALOG}.{SCHEMA}.gold_spike_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_FACT_SPIKE}, {SOURCE_DIM_MATCH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    map_name STRING NOT NULL,
    site STRING NOT NULL,
    our_plants_count INT NOT NULL,
    our_post_plant_wins INT NOT NULL,
    our_post_plant_win_pct DOUBLE NOT NULL,
    our_spike_detonations INT NOT NULL,
    enemy_defuses_allowed INT NOT NULL,
    site_plant_preference_pct DOUBLE NOT NULL,
    avg_plant_time_seconds DOUBLE NOT NULL,
    opponent_plants_count INT NOT NULL,
    our_retake_defuses INT NOT NULL,
    our_retake_win_pct DOUBLE NOT NULL,
    opponent_detonations INT NOT NULL,
    top_planter_display_name STRING,
    top_defuser_display_name STRING,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold spike performance: bomb site execution bias, post-plant hold conversion, and retake defusals'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Enriched Spike Events with Map Context

# COMMAND ----------
spike_df = spark.table(SOURCE_FACT_SPIKE).join(
    spark.table(SOURCE_DIM_MATCH).select("match_id", "map_name"),
    on="match_id",
    how="inner"
).filter(
    F.col("site").isin("A", "B", "C")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Compute Attack Plants, Defense Retakes, and Objective Leaders

# COMMAND ----------
# 1. Attack Side: Plants by Our Squad
our_plants_df = spike_df.filter(
    (F.col("event_type") == "PLANT") & (F.col("is_our_team") == True)
)

our_plant_agg_df = our_plants_df.groupBy("map_name", "site").agg(
    F.count("*").alias("our_plants_count"),
    F.count(F.when(F.col("is_our_team_round_win") == True, 1)).alias("our_post_plant_wins"),
    F.count(F.when(F.col("round_end_type") == "Bomb detonated", 1)).alias("our_spike_detonations"),
    F.count(F.when(F.col("round_end_type") == "Bomb defused", 1)).alias("enemy_defuses_allowed"),
    F.round(F.avg("event_time_seconds"), 1).alias("avg_plant_time_seconds")
)

# 2. Defense Side: Plants by Opponent Squad (Retake Scenarios)
opp_plants_df = spike_df.filter(
    (F.col("event_type") == "PLANT") & (F.col("is_our_team") == False)
)

opp_plant_agg_df = opp_plants_df.groupBy("map_name", "site").agg(
    F.count("*").alias("opponent_plants_count"),
    F.count(F.when(F.col("is_our_team_round_win") == True, 1)).alias("our_retake_defuses"),
    F.count(F.when(F.col("round_end_type") == "Bomb detonated", 1)).alias("opponent_detonations")
)

# 3. Top Planter per Map and Site
planter_counts_df = our_plants_df.filter(
    F.col("player_display_name").isNotNull()
).groupBy("map_name", "site", "player_display_name").agg(
    F.count("*").alias("plant_count")
)

planter_window = Window.partitionBy("map_name", "site").orderBy(F.col("plant_count").desc())

top_planters_df = planter_counts_df.withColumn(
    "rn", F.row_number().over(planter_window)
).filter(
    F.col("rn") == 1
).select(
    "map_name", "site", F.col("player_display_name").alias("top_planter_display_name")
)

# 4. Top Defuser per Map and Site (from DEFUSE events)
our_defuses_df = spike_df.filter(
    (F.col("event_type") == "DEFUSE") & (F.col("is_our_team") == True) & F.col("player_display_name").isNotNull()
).groupBy("map_name", "site", "player_display_name").agg(
    F.count("*").alias("defuse_count")
)

defuser_window = Window.partitionBy("map_name", "site").orderBy(F.col("defuse_count").desc())

top_defusers_df = our_defuses_df.withColumn(
    "rn", F.row_number().over(defuser_window)
).filter(
    F.col("rn") == 1
).select(
    "map_name", "site", F.col("player_display_name").alias("top_defuser_display_name")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Join Aggregations and Calculate Conversion Rates & Preferences

# COMMAND ----------
# Distinct list of map and site pairs present in spike events
sites_base_df = spike_df.select("map_name", "site").distinct()

combined_spike_df = sites_base_df \
    .join(our_plant_agg_df, on=["map_name", "site"], how="left") \
    .join(opp_plant_agg_df, on=["map_name", "site"], how="left") \
    .join(top_planters_df, on=["map_name", "site"], how="left") \
    .join(top_defusers_df, on=["map_name", "site"], how="left")

# Window to calculate Total Plants by Our Squad on the Map for Preference %
w_map = Window.partitionBy("map_name")

final_gold_spike_df = combined_spike_df.withColumn(
    "our_plants_count", F.coalesce(F.col("our_plants_count"), F.lit(0))
).withColumn(
    "our_post_plant_wins", F.coalesce(F.col("our_post_plant_wins"), F.lit(0))
).withColumn(
    "our_spike_detonations", F.coalesce(F.col("our_spike_detonations"), F.lit(0))
).withColumn(
    "enemy_defuses_allowed", F.coalesce(F.col("enemy_defuses_allowed"), F.lit(0))
).withColumn(
    "avg_plant_time_seconds", F.coalesce(F.col("avg_plant_time_seconds"), F.lit(0.0))
).withColumn(
    "opponent_plants_count", F.coalesce(F.col("opponent_plants_count"), F.lit(0))
).withColumn(
    "our_retake_defuses", F.coalesce(F.col("our_retake_defuses"), F.lit(0))
).withColumn(
    "opponent_detonations", F.coalesce(F.col("opponent_detonations"), F.lit(0))
).withColumn(
    "total_map_plants", F.sum("our_plants_count").over(w_map)
).select(
    F.col("map_name"),
    F.col("site"),
    F.col("our_plants_count"),
    F.col("our_post_plant_wins"),
    F.round(F.col("our_post_plant_wins") * 100.0 / F.when(F.col("our_plants_count") == 0, 1).otherwise(F.col("our_plants_count")), 2).alias("our_post_plant_win_pct"),
    F.col("our_spike_detonations"),
    F.col("enemy_defuses_allowed"),
    F.round(F.col("our_plants_count") * 100.0 / F.when(F.col("total_map_plants") == 0, 1).otherwise(F.col("total_map_plants")), 2).alias("site_plant_preference_pct"),
    F.col("avg_plant_time_seconds"),
    F.col("opponent_plants_count"),
    F.col("our_retake_defuses"),
    F.round(F.col("our_retake_defuses") * 100.0 / F.when(F.col("opponent_plants_count") == 0, 1).otherwise(F.col("opponent_plants_count")), 2).alias("our_retake_win_pct"),
    F.col("opponent_detonations"),
    F.col("top_planter_display_name"),
    F.col("top_defuser_display_name"),
    F.current_timestamp().alias("updated_at")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_spike_df.createOrReplaceTempView("staged_gold_spike")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_spike AS source
ON target.map_name = source.map_name AND target.site = source.site
WHEN MATCHED THEN
  UPDATE SET
    target.our_plants_count = source.our_plants_count,
    target.our_post_plant_wins = source.our_post_plant_wins,
    target.our_post_plant_win_pct = source.our_post_plant_win_pct,
    target.our_spike_detonations = source.our_spike_detonations,
    target.enemy_defuses_allowed = source.enemy_defuses_allowed,
    target.site_plant_preference_pct = source.site_plant_preference_pct,
    target.avg_plant_time_seconds = source.avg_plant_time_seconds,
    target.opponent_plants_count = source.opponent_plants_count,
    target.our_retake_defuses = source.our_retake_defuses,
    target.our_retake_win_pct = source.our_retake_win_pct,
    target.opponent_detonations = source.opponent_detonations,
    target.top_planter_display_name = source.top_planter_display_name,
    target.top_defuser_display_name = source.top_defuser_display_name,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    map_name, site, our_plants_count, our_post_plant_wins, our_post_plant_win_pct,
    our_spike_detonations, enemy_defuses_allowed, site_plant_preference_pct,
    avg_plant_time_seconds, opponent_plants_count, our_retake_defuses,
    our_retake_win_pct, opponent_detonations, top_planter_display_name,
    top_defuser_display_name, updated_at
  )
  VALUES (
    source.map_name, source.site, source.our_plants_count, source.our_post_plant_wins, source.our_post_plant_win_pct,
    source.our_spike_detonations, source.enemy_defuses_allowed, source.site_plant_preference_pct,
    source.avg_plant_time_seconds, source.opponent_plants_count, source.our_retake_defuses,
    source.our_retake_win_pct, source.opponent_detonations, source.top_planter_display_name,
    source.top_defuser_display_name, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Verification & Spike Site Execution Display

# COMMAND ----------
total_sites = spark.table(TARGET_TABLE).count()
print(f"Total Map Bomb Sites Recorded: {total_sites}")

# Display Sample Spike Site Leaderboard
print("Sample Bomb Site Execution & Conversion (Sorted by Plant Count):")
spark.table(TARGET_TABLE).select(
    "map_name", "site", "our_plants_count", "site_plant_preference_pct",
    "our_post_plant_win_pct", "our_retake_win_pct", "avg_plant_time_seconds",
    "top_planter_display_name", "top_defuser_display_name"
).orderBy(F.col("our_plants_count").desc()).show(15, truncate=False)
