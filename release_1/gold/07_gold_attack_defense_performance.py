# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_attack_defense_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per `map_name` per `tactical_side` (including an `'ALL_MAPS'` macro rollup)
# MAGIC - **Composite Primary Key:** `map_name` + `tactical_side`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.silver.fact_round` (Side attribution, round wins, plant/defuse, thrifty)
# MAGIC   - `valorant.silver.dim_match` (Match outcomes, starting side determination, halftime scores)
# MAGIC   - `valorant.silver.fact_kill_event` (Opening kill / First Blood momentum)
# MAGIC   - `valorant.silver.fact_round_player` (Combat kills and deaths per side)
# MAGIC - **Target:** `valorant.gold.gold_attack_defense_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `map_name` + `tactical_side`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Macro Tactical Bias:** Overall squad conversion on Attack vs Defense across all 211 matches.
# MAGIC - **Starting Side Win Rates:** Hard win % comparison when starting on Attack vs starting on Defense.
# MAGIC - **First Blood Conversion:** Win rate after opening kill (Attack FB vs Defense FB).
# MAGIC - **Pistol & Anti-Eco Momentum:** Conversion rate in pistol rounds (R1/R13) and subsequent anti-ecos (R2/R14).
# MAGIC - **Halftime Conversion:** Win rate when leading at halftime after starting Attack vs Defense.

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
SOURCE_FACT_ROUND        = f"{CATALOG}.silver.fact_round"
SOURCE_DIM_MATCH         = f"{CATALOG}.silver.dim_match"
SOURCE_FACT_KILL         = f"{CATALOG}.silver.fact_kill_event"
SOURCE_FACT_ROUND_PLAYER = f"{CATALOG}.silver.fact_round_player"
TARGET_TABLE             = f"{CATALOG}.{SCHEMA}.gold_attack_defense_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_FACT_ROUND}, {SOURCE_DIM_MATCH}, {SOURCE_FACT_KILL}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    map_name STRING NOT NULL,
    tactical_side STRING NOT NULL,
    rounds_played INT NOT NULL,
    rounds_won INT NOT NULL,
    rounds_lost INT NOT NULL,
    side_win_pct DOUBLE NOT NULL,
    matches_started_on_side INT NOT NULL,
    match_wins_started_on_side INT NOT NULL,
    starting_side_match_win_pct DOUBLE NOT NULL,
    avg_first_half_rounds_won DOUBLE NOT NULL,
    first_blood_rounds INT NOT NULL,
    first_blood_wins INT NOT NULL,
    first_blood_conversion_pct DOUBLE NOT NULL,
    first_death_rounds INT NOT NULL,
    first_death_losses INT NOT NULL,
    first_death_loss_pct DOUBLE NOT NULL,
    pistol_rounds_played INT NOT NULL,
    pistol_rounds_won INT NOT NULL,
    pistol_win_pct DOUBLE NOT NULL,
    anti_eco_rounds_played INT NOT NULL,
    anti_eco_rounds_won INT NOT NULL,
    anti_eco_conversion_pct DOUBLE NOT NULL,
    team_kills INT NOT NULL,
    team_deaths INT NOT NULL,
    team_kd_ratio DOUBLE NOT NULL,
    spikes_planted INT NOT NULL,
    spikes_defused INT NOT NULL,
    thrifty_rounds_won INT NOT NULL,
    leading_at_half_matches INT NOT NULL,
    leading_at_half_wins INT NOT NULL,
    halftime_lead_conversion_pct DOUBLE NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold Attack vs Defense performance: macro conversion, starting side impact, first bloods, and halftime closeouts'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Match-Level Starting Sides and Halftime Scores

# COMMAND ----------
fact_round_df = spark.table(SOURCE_FACT_ROUND)
dim_match_df  = spark.table(SOURCE_DIM_MATCH)

# 1. Starting side per match (Round 1)
match_starting_side_df = fact_round_df.filter(
    F.col("round_number") == 1
).select(
    F.col("match_id"),
    F.col("our_team_side").alias("starting_side")
)

# 2. First-half score tracking (Rounds 1–12)
first_half_match_df = fact_round_df.filter(
    F.col("round_number") <= 12
).groupBy("match_id").agg(
    F.count("*").alias("fh_rounds_played"),
    F.count(F.when(F.col("is_our_team_win") == True, 1)).alias("fh_rounds_won"),
    F.count(F.when(F.col("is_our_team_win") == False, 1)).alias("fh_rounds_lost")
).withColumn(
    "is_leading_at_half",
    F.when(F.col("fh_rounds_won") > F.col("fh_rounds_lost"), True).otherwise(False)
)

# 3. Match Context Table
match_context_df = dim_match_df.select(
    F.col("match_id"),
    F.col("map_name"),
    F.col("is_our_team_win")
).join(
    match_starting_side_df, on="match_id", how="inner"
).join(
    first_half_match_df, on="match_id", how="left"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Round-Level Tactical Attributes (First Bloods, Pistols, Anti-Ecos, Combat)

# COMMAND ----------
# 1. Opening Duel Flags (First Blood / First Death)
kill_event_df = spark.table(SOURCE_FACT_KILL)

opening_kills_df = kill_event_df.filter(
    (F.col("is_opening_kill") == True) & (F.col("is_killer_our_team") == True)
).select(
    "match_id", "round_number", F.lit(True).alias("squad_had_fb")
).dropDuplicates(["match_id", "round_number"])

opening_deaths_df = kill_event_df.filter(
    (F.col("is_opening_death") == True) & (F.col("is_victim_our_team") == True)
).select(
    "match_id", "round_number", F.lit(True).alias("squad_had_fd")
).dropDuplicates(["match_id", "round_number"])

# 2. Round Player Kills & Deaths per round
round_combat_df = spark.table(SOURCE_FACT_ROUND_PLAYER).filter(
    F.col("is_our_team") == True
).groupBy("match_id", "round_number").agg(
    F.sum("kills").alias("team_kills"),
    F.sum("deaths").alias("team_deaths")
)

# 3. Anti-Eco Detection: Subsequent round after winning Pistol (Round 2 following R1 win, Round 14 following R13 win)
round_window = Window.partitionBy("match_id").orderBy("round_number")

enriched_round_base_df = fact_round_df.join(
    dim_match_df.select("match_id", "map_name"), on="match_id", how="inner"
).join(
    opening_kills_df, on=["match_id", "round_number"], how="left"
).join(
    opening_deaths_df, on=["match_id", "round_number"], how="left"
).join(
    round_combat_df, on=["match_id", "round_number"], how="left"
).withColumn(
    "prev_round_win", F.lag("is_our_team_win", 1).over(round_window)
).withColumn(
    "is_pistol_round", F.when(F.col("round_number").isin(1, 13), True).otherwise(False)
).withColumn(
    "is_anti_eco_round",
    F.when((F.col("round_number").isin(2, 14)) & (F.col("prev_round_win") == True), True).otherwise(False)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Aggregate by Map & Tactical Side (Plus 'ALL_MAPS' Rollup)

# COMMAND ----------
def build_side_aggregation(df_rounds, df_matches, group_cols):
    # Match-level aggregates (Starting side & Halftime momentum)
    match_agg = df_matches.groupBy(group_cols + ["starting_side"]).agg(
        F.countDistinct("match_id").alias("matches_started_on_side"),
        F.count(F.when(F.col("is_our_team_win") == True, 1)).alias("match_wins_started_on_side"),
        F.round(F.avg("fh_rounds_won"), 1).alias("avg_first_half_rounds_won"),
        F.count(F.when(F.col("is_leading_at_half") == True, 1)).alias("leading_at_half_matches"),
        F.count(F.when((F.col("is_leading_at_half") == True) & (F.col("is_our_team_win") == True), 1)).alias("leading_at_half_wins")
    ).withColumnRenamed("starting_side", "tactical_side")

    # Round-level aggregates
    round_agg = df_rounds.groupBy(group_cols + ["our_team_side"]).agg(
        F.count("*").alias("rounds_played"),
        F.count(F.when(F.col("is_our_team_win") == True, 1)).alias("rounds_won"),
        F.count(F.when(F.col("is_our_team_win") == False, 1)).alias("rounds_lost"),
        # First Blood conversion
        F.count(F.when(F.col("squad_had_fb") == True, 1)).alias("first_blood_rounds"),
        F.count(F.when((F.col("squad_had_fb") == True) & (F.col("is_our_team_win") == True), 1)).alias("first_blood_wins"),
        # First Death conversion
        F.count(F.when(F.col("squad_had_fd") == True, 1)).alias("first_death_rounds"),
        F.count(F.when((F.col("squad_had_fd") == True) & (F.col("is_our_team_win") == False), 1)).alias("first_death_losses"),
        # Pistol execution
        F.count(F.when(F.col("is_pistol_round") == True, 1)).alias("pistol_rounds_played"),
        F.count(F.when((F.col("is_pistol_round") == True) & (F.col("is_our_team_win") == True), 1)).alias("pistol_rounds_won"),
        # Anti-Eco conversion
        F.count(F.when(F.col("is_anti_eco_round") == True, 1)).alias("anti_eco_rounds_played"),
        F.count(F.when((F.col("is_anti_eco_round") == True) & (F.col("is_our_team_win") == True), 1)).alias("anti_eco_rounds_won"),
        # Combat & Spikes
        F.sum("team_kills").alias("team_kills"),
        F.sum("team_deaths").alias("team_deaths"),
        F.count(F.when((F.col("our_team_side") == "Attack") & (F.col("bomb_planted") == True), 1)).alias("spikes_planted"),
        F.count(F.when((F.col("our_team_side") == "Defense") & (F.col("bomb_defused") == True), 1)).alias("spikes_defused"),
        F.count(F.when(F.col("is_thrifty") == True, 1)).alias("thrifty_rounds_won")
    ).withColumnRenamed("our_team_side", "tactical_side")

    return round_agg.join(match_agg, on=group_cols + ["tactical_side"], how="left")

# 1. Per-Map Side Breakdown
map_side_df = build_side_aggregation(enriched_round_base_df, match_context_df, ["map_name"])

# 2. Squad-Wide Macro Rollup ('ALL_MAPS')
all_maps_rounds_df = enriched_round_base_df.withColumn("map_name", F.lit("ALL_MAPS"))
all_maps_matches_df = match_context_df.withColumn("map_name", F.lit("ALL_MAPS"))
all_maps_side_df = build_side_aggregation(all_maps_rounds_df, all_maps_matches_df, ["map_name"])

# Union Map Breakdown and Macro Rollup
unified_side_df = map_side_df.unionByName(all_maps_side_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Compute Conversion Rates and Win Percentages

# COMMAND ----------
final_gold_side_df = unified_side_df.select(
    F.col("map_name"),
    F.col("tactical_side"),
    F.col("rounds_played"),
    F.col("rounds_won"),
    F.col("rounds_lost"),
    F.round(F.col("rounds_won") * 100.0 / F.when(F.col("rounds_played") == 0, 1).otherwise(F.col("rounds_played")), 2).alias("side_win_pct"),
    F.coalesce(F.col("matches_started_on_side"), F.lit(0)).alias("matches_started_on_side"),
    F.coalesce(F.col("match_wins_started_on_side"), F.lit(0)).alias("match_wins_started_on_side"),
    F.round(F.coalesce(F.col("match_wins_started_on_side"), F.lit(0)) * 100.0 / F.when(F.coalesce(F.col("matches_started_on_side"), F.lit(0)) == 0, 1).otherwise(F.col("matches_started_on_side")), 2).alias("starting_side_match_win_pct"),
    F.coalesce(F.col("avg_first_half_rounds_won"), F.lit(0.0)).alias("avg_first_half_rounds_won"),
    F.coalesce(F.col("first_blood_rounds"), F.lit(0)).alias("first_blood_rounds"),
    F.coalesce(F.col("first_blood_wins"), F.lit(0)).alias("first_blood_wins"),
    F.round(F.coalesce(F.col("first_blood_wins"), F.lit(0)) * 100.0 / F.when(F.coalesce(F.col("first_blood_rounds"), F.lit(0)) == 0, 1).otherwise(F.col("first_blood_rounds")), 2).alias("first_blood_conversion_pct"),
    F.coalesce(F.col("first_death_rounds"), F.lit(0)).alias("first_death_rounds"),
    F.coalesce(F.col("first_death_losses"), F.lit(0)).alias("first_death_losses"),
    F.round(F.coalesce(F.col("first_death_losses"), F.lit(0)) * 100.0 / F.when(F.coalesce(F.col("first_death_rounds"), F.lit(0)) == 0, 1).otherwise(F.col("first_death_rounds")), 2).alias("first_death_loss_pct"),
    F.coalesce(F.col("pistol_rounds_played"), F.lit(0)).alias("pistol_rounds_played"),
    F.coalesce(F.col("pistol_rounds_won"), F.lit(0)).alias("pistol_rounds_won"),
    F.round(F.coalesce(F.col("pistol_rounds_won"), F.lit(0)) * 100.0 / F.when(F.coalesce(F.col("pistol_rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("pistol_rounds_played")), 2).alias("pistol_win_pct"),
    F.coalesce(F.col("anti_eco_rounds_played"), F.lit(0)).alias("anti_eco_rounds_played"),
    F.coalesce(F.col("anti_eco_rounds_won"), F.lit(0)).alias("anti_eco_rounds_won"),
    F.round(F.coalesce(F.col("anti_eco_rounds_won"), F.lit(0)) * 100.0 / F.when(F.coalesce(F.col("anti_eco_rounds_played"), F.lit(0)) == 0, 1).otherwise(F.col("anti_eco_rounds_played")), 2).alias("anti_eco_conversion_pct"),
    F.coalesce(F.col("team_kills"), F.lit(0)).alias("team_kills"),
    F.coalesce(F.col("team_deaths"), F.lit(0)).alias("team_deaths"),
    F.round(F.coalesce(F.col("team_kills"), F.lit(0)) / F.when(F.coalesce(F.col("team_deaths"), F.lit(0)) == 0, 1.0).otherwise(F.col("team_deaths")), 2).alias("team_kd_ratio"),
    F.coalesce(F.col("spikes_planted"), F.lit(0)).alias("spikes_planted"),
    F.coalesce(F.col("spikes_defused"), F.lit(0)).alias("spikes_defused"),
    F.coalesce(F.col("thrifty_rounds_won"), F.lit(0)).alias("thrifty_rounds_won"),
    F.coalesce(F.col("leading_at_half_matches"), F.lit(0)).alias("leading_at_half_matches"),
    F.coalesce(F.col("leading_at_half_wins"), F.lit(0)).alias("leading_at_half_wins"),
    F.round(F.coalesce(F.col("leading_at_half_wins"), F.lit(0)) * 100.0 / F.when(F.coalesce(F.col("leading_at_half_matches"), F.lit(0)) == 0, 1).otherwise(F.col("leading_at_half_matches")), 2).alias("halftime_lead_conversion_pct"),
    F.current_timestamp().alias("updated_at")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_side_df.createOrReplaceTempView("staged_gold_attack_defense")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_attack_defense AS source
ON target.map_name = source.map_name AND target.tactical_side = source.tactical_side
WHEN MATCHED THEN
  UPDATE SET
    target.rounds_played = source.rounds_played,
    target.rounds_won = source.rounds_won,
    target.rounds_lost = source.rounds_lost,
    target.side_win_pct = source.side_win_pct,
    target.matches_started_on_side = source.matches_started_on_side,
    target.match_wins_started_on_side = source.match_wins_started_on_side,
    target.starting_side_match_win_pct = source.starting_side_match_win_pct,
    target.avg_first_half_rounds_won = source.avg_first_half_rounds_won,
    target.first_blood_rounds = source.first_blood_rounds,
    target.first_blood_wins = source.first_blood_wins,
    target.first_blood_conversion_pct = source.first_blood_conversion_pct,
    target.first_death_rounds = source.first_death_rounds,
    target.first_death_losses = source.first_death_losses,
    target.first_death_loss_pct = source.first_death_loss_pct,
    target.pistol_rounds_played = source.pistol_rounds_played,
    target.pistol_rounds_won = source.pistol_rounds_won,
    target.pistol_win_pct = source.pistol_win_pct,
    target.anti_eco_rounds_played = source.anti_eco_rounds_played,
    target.anti_eco_rounds_won = source.anti_eco_rounds_won,
    target.anti_eco_conversion_pct = source.anti_eco_conversion_pct,
    target.team_kills = source.team_kills,
    target.team_deaths = source.team_deaths,
    target.team_kd_ratio = source.team_kd_ratio,
    target.spikes_planted = source.spikes_planted,
    target.spikes_defused = source.spikes_defused,
    target.thrifty_rounds_won = source.thrifty_rounds_won,
    target.leading_at_half_matches = source.leading_at_half_matches,
    target.leading_at_half_wins = source.leading_at_half_wins,
    target.halftime_lead_conversion_pct = source.halftime_lead_conversion_pct,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    map_name, tactical_side, rounds_played, rounds_won, rounds_lost, side_win_pct,
    matches_started_on_side, match_wins_started_on_side, starting_side_match_win_pct, avg_first_half_rounds_won,
    first_blood_rounds, first_blood_wins, first_blood_conversion_pct,
    first_death_rounds, first_death_losses, first_death_loss_pct,
    pistol_rounds_played, pistol_rounds_won, pistol_win_pct,
    anti_eco_rounds_played, anti_eco_rounds_won, anti_eco_conversion_pct,
    team_kills, team_deaths, team_kd_ratio, spikes_planted, spikes_defused,
    thrifty_rounds_won, leading_at_half_matches, leading_at_half_wins, halftime_lead_conversion_pct,
    updated_at
  )
  VALUES (
    source.map_name, source.tactical_side, source.rounds_played, source.rounds_won, source.rounds_lost, source.side_win_pct,
    source.matches_started_on_side, source.match_wins_started_on_side, source.starting_side_match_win_pct, source.avg_first_half_rounds_won,
    source.first_blood_rounds, source.first_blood_wins, source.first_blood_conversion_pct,
    source.first_death_rounds, source.first_death_losses, source.first_death_loss_pct,
    source.pistol_rounds_played, source.pistol_rounds_won, source.pistol_win_pct,
    source.anti_eco_rounds_played, source.anti_eco_rounds_won, source.anti_eco_conversion_pct,
    source.team_kills, source.team_deaths, source.team_kd_ratio, source.spikes_planted, source.spikes_defused,
    source.thrifty_rounds_won, source.leading_at_half_matches, source.leading_at_half_wins, source.halftime_lead_conversion_pct,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 7: Verification & Macro Side Diagnostics Display

# COMMAND ----------
total_rows = spark.table(TARGET_TABLE).count()
print(f"Total Attack/Defense Diagnostic Records: {total_rows}")

# Display Macro Squad Comparison
print("Macro Squad Attack vs Defense Breakdown ('ALL_MAPS'):")
spark.table(TARGET_TABLE).filter(
    F.col("map_name") == "ALL_MAPS"
).select(
    "tactical_side", "rounds_played", "side_win_pct", "starting_side_match_win_pct",
    "first_blood_conversion_pct", "first_death_loss_pct", "pistol_win_pct",
    "anti_eco_conversion_pct", "team_kd_ratio", "halftime_lead_conversion_pct"
).show(truncate=False)
