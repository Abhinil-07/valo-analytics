# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_team_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per `period_type` per `time_period` (Multi-granularity: `'WEEKLY'`, `'FORTNIGHTLY'`, `'MONTHLY'`)
# MAGIC - **Composite Primary Key:** `period_type` + `time_period`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.gold.gold_match_summary` (Pre-aggregated match team metrics, scores, duration, outcomes)
# MAGIC - **Target:** `valorant.gold.gold_team_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `period_type` + `time_period`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Team Overview & Executive Dashboard:** Slicer toggle between **Weekly**, **Fortnightly**, and **Monthly** views.
# MAGIC - **Team Trajectory Trendline:** Tracking squad win rate %, round differential trends, and K/D over time.
# MAGIC - **Attack vs. Defense Evolution:** Monitoring whether team improvement is driven by better Attack conversions or Defense holds.
# MAGIC - **Form Guide & Streaks:** Recent form indicators (e.g. `'W-W-L-W-W'`) and longest win streaks per period.

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
SOURCE_GOLD_MATCH = f"{CATALOG}.gold.gold_match_summary"
TARGET_TABLE      = f"{CATALOG}.{SCHEMA}.gold_team_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_GOLD_MATCH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    period_type STRING NOT NULL,
    time_period STRING NOT NULL,
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    matches_played INT NOT NULL,
    matches_won INT NOT NULL,
    matches_lost INT NOT NULL,
    matches_drawn INT NOT NULL,
    team_win_pct DOUBLE NOT NULL,
    rounds_played INT NOT NULL,
    rounds_won INT NOT NULL,
    rounds_lost INT NOT NULL,
    round_win_pct DOUBLE NOT NULL,
    round_differential INT NOT NULL,
    avg_round_differential DOUBLE NOT NULL,
    attack_rounds_played INT NOT NULL,
    attack_rounds_won INT NOT NULL,
    attack_win_pct DOUBLE NOT NULL,
    defense_rounds_played INT NOT NULL,
    defense_rounds_won INT NOT NULL,
    defense_win_pct DOUBLE NOT NULL,
    team_kills INT NOT NULL,
    team_deaths INT NOT NULL,
    team_assists INT NOT NULL,
    team_kd_ratio DOUBLE NOT NULL,
    team_damage_dealt INT NOT NULL,
    team_damage_received INT NOT NULL,
    team_damage_differential INT NOT NULL,
    thrifty_rounds_won INT NOT NULL,
    longest_win_streak INT NOT NULL,
    form_guide STRING,
    most_played_map STRING,
    avg_match_duration_minutes DOUBLE,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold team performance trajectory with Weekly, Fortnightly, and Monthly cohorts'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Match Summaries and Construct Multi-Granularity Cohorts

# COMMAND ----------
base_match_df = spark.table(SOURCE_GOLD_MATCH)

# 1. Weekly Cohort: Monday to Sunday (ISO Week)
weekly_df = base_match_df.withColumn(
    "period_type", F.lit("WEEKLY")
).withColumn(
    "time_period",
    F.concat(F.year("match_date"), F.lit("-W"), F.lpad(F.weekofyear("match_date"), 2, "0"))
).withColumn(
    "period_start_date",
    F.date_trunc("week", F.col("match_date")).cast("date")
).withColumn(
    "period_end_date",
    F.date_add(F.date_trunc("week", F.col("match_date")), 6).cast("date")
)

# 2. Fortnightly Cohort: 2-Week Block
fortnightly_df = base_match_df.withColumn(
    "period_type", F.lit("FORTNIGHTLY")
).withColumn(
    "fn_num",
    ((F.weekofyear("match_date") - 1) / 2 + 1).cast("int")
).withColumn(
    "time_period",
    F.concat(F.year("match_date"), F.lit("-FN"), F.lpad(F.col("fn_num"), 2, "0"))
).withColumn(
    "week_start",
    F.date_trunc("week", F.col("match_date")).cast("date")
)

# Derive actual date boundaries for fortnights
fn_bounds_df = fortnightly_df.groupBy("time_period").agg(
    F.min("week_start").alias("period_start_date"),
    F.date_add(F.max("week_start"), 6).alias("period_end_date")
)

fortnightly_df = fortnightly_df.drop("period_start_date", "period_end_date", "week_start") \
    .join(fn_bounds_df, on="time_period", how="left")

# 3. Monthly Cohort: Calendar Month
monthly_df = base_match_df.withColumn(
    "period_type", F.lit("MONTHLY")
).withColumn(
    "time_period",
    F.date_format("match_date", "yyyy-MM")
).withColumn(
    "period_start_date",
    F.trunc("match_date", "month").cast("date")
).withColumn(
    "period_end_date",
    F.last_day("match_date").cast("date")
)

# Union All Three Cohorts
unified_cohorts_df = weekly_df.select(
    "period_type", "time_period", "period_start_date", "period_end_date",
    "match_id", "match_date", "match_start_timestamp", "map_name",
    "rounds_played", "our_team_rounds_won", "opponent_rounds_won", "round_differential",
    "match_outcome", "is_our_team_win", "attack_rounds_played", "attack_rounds_won",
    "defense_rounds_played", "defense_rounds_won", "team_kills", "team_deaths", "team_assists",
    "team_damage_dealt", "team_damage_received", "thrifty_rounds_won", "game_duration_minutes"
).unionByName(
    fortnightly_df.select(
        "period_type", "time_period", "period_start_date", "period_end_date",
        "match_id", "match_date", "match_start_timestamp", "map_name",
        "rounds_played", "our_team_rounds_won", "opponent_rounds_won", "round_differential",
        "match_outcome", "is_our_team_win", "attack_rounds_played", "attack_rounds_won",
        "defense_rounds_played", "defense_rounds_won", "team_kills", "team_deaths", "team_assists",
        "team_damage_dealt", "team_damage_received", "thrifty_rounds_won", "game_duration_minutes"
    )
).unionByName(
    monthly_df.select(
        "period_type", "time_period", "period_start_date", "period_end_date",
        "match_id", "match_date", "match_start_timestamp", "map_name",
        "rounds_played", "our_team_rounds_won", "opponent_rounds_won", "round_differential",
        "match_outcome", "is_our_team_win", "attack_rounds_played", "attack_rounds_won",
        "defense_rounds_played", "defense_rounds_won", "team_kills", "team_deaths", "team_assists",
        "team_damage_dealt", "team_damage_received", "thrifty_rounds_won", "game_duration_minutes"
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Compute Form Guide, Longest Win Streaks, and Map Preferences

# COMMAND ----------
# 1. Form Guide (Last 5 matches in period)
form_window = Window.partitionBy("period_type", "time_period").orderBy(
    F.col("match_date").desc(), F.col("match_start_timestamp").desc(), F.col("match_id").desc()
)

form_df = unified_cohorts_df.withColumn(
    "rn", F.row_number().over(form_window)
).filter(
    F.col("rn") <= 5
).withColumn(
    "result_code",
    F.when(F.col("is_our_team_win") == True, F.lit("W"))
     .when(F.col("match_outcome") == "DRAW", F.lit("D"))
     .otherwise(F.lit("L"))
).groupBy("period_type", "time_period").agg(
    F.concat_ws("-", F.collect_list("result_code")).alias("form_guide")
)

# 2. Longest Win Streak in Period
streak_order_window = Window.partitionBy("period_type", "time_period").orderBy(
    F.col("match_date").asc(), F.col("match_start_timestamp").asc(), F.col("match_id").asc()
)

cum_loss_window = Window.partitionBy("period_type", "time_period").orderBy(
    F.col("match_date").asc(), F.col("match_start_timestamp").asc(), F.col("match_id").asc()
).rowsBetween(Window.unboundedPreceding, Window.currentRow)

streak_base_df = unified_cohorts_df.withColumn(
    "loss_flag", F.when(F.col("is_our_team_win") == False, 1).otherwise(0)
).withColumn(
    "streak_group", F.sum("loss_flag").over(cum_loss_window)
)

streak_df = streak_base_df.filter(
    F.col("is_our_team_win") == True
).groupBy(
    "period_type", "time_period", "streak_group"
).agg(
    F.count("*").alias("streak_len")
).groupBy(
    "period_type", "time_period"
).agg(
    F.max("streak_len").alias("longest_win_streak")
)

# 3. Most Played Map in Period
map_counts_df = unified_cohorts_df.groupBy("period_type", "time_period", "map_name").agg(
    F.count("*").alias("map_play_count")
)

map_window = Window.partitionBy("period_type", "time_period").orderBy(F.col("map_play_count").desc(), F.col("map_name").asc())

most_played_map_df = map_counts_df.withColumn(
    "rn", F.row_number().over(map_window)
).filter(
    F.col("rn") == 1
).select(
    "period_type", "time_period", F.col("map_name").alias("most_played_map")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Aggregate Period Performance and Combine Metrics

# COMMAND ----------
period_agg_df = unified_cohorts_df.groupBy(
    "period_type", "time_period"
).agg(
    F.min("period_start_date").alias("period_start_date"),
    F.max("period_end_date").alias("period_end_date"),
    F.countDistinct("match_id").alias("matches_played"),
    F.count(F.when(F.col("is_our_team_win") == True, 1)).alias("matches_won"),
    F.count(F.when(F.col("match_outcome") == "DEFEAT", 1)).alias("matches_lost"),
    F.count(F.when(F.col("match_outcome") == "DRAW", 1)).alias("matches_drawn"),
    F.sum("rounds_played").alias("rounds_played"),
    F.sum("our_team_rounds_won").alias("rounds_won"),
    F.sum("opponent_rounds_won").alias("rounds_lost"),
    F.sum("round_differential").alias("round_differential"),
    F.round(F.avg("round_differential"), 2).alias("avg_round_differential"),
    F.sum("attack_rounds_played").alias("attack_rounds_played"),
    F.sum("attack_rounds_won").alias("attack_rounds_won"),
    F.sum("defense_rounds_played").alias("defense_rounds_played"),
    F.sum("defense_rounds_won").alias("defense_rounds_won"),
    F.sum("team_kills").alias("team_kills"),
    F.sum("team_deaths").alias("team_deaths"),
    F.sum("team_assists").alias("team_assists"),
    F.sum("team_damage_dealt").alias("team_damage_dealt"),
    F.sum("team_damage_received").alias("team_damage_received"),
    F.sum("thrifty_rounds_won").alias("thrifty_rounds_won"),
    F.round(F.avg("game_duration_minutes"), 1).alias("avg_match_duration_minutes")
)

final_gold_team_df = period_agg_df \
    .join(form_df, on=["period_type", "time_period"], how="left") \
    .join(streak_df, on=["period_type", "time_period"], how="left") \
    .join(most_played_map_df, on=["period_type", "time_period"], how="left") \
    .select(
        F.col("period_type"),
        F.col("time_period"),
        F.col("period_start_date"),
        F.col("period_end_date"),
        F.col("matches_played"),
        F.col("matches_won"),
        F.col("matches_lost"),
        F.col("matches_drawn"),
        F.round(F.col("matches_won") * 100.0 / F.col("matches_played"), 2).alias("team_win_pct"),
        F.col("rounds_played"),
        F.col("rounds_won"),
        F.col("rounds_lost"),
        F.round(F.col("rounds_won") * 100.0 / F.when(F.col("rounds_played") == 0, 1).otherwise(F.col("rounds_played")), 2).alias("round_win_pct"),
        F.col("round_differential"),
        F.col("avg_round_differential"),
        F.col("attack_rounds_played"),
        F.col("attack_rounds_won"),
        F.round(F.col("attack_rounds_won") * 100.0 / F.when(F.col("attack_rounds_played") == 0, 1).otherwise(F.col("attack_rounds_played")), 2).alias("attack_win_pct"),
        F.col("defense_rounds_played"),
        F.col("defense_rounds_won"),
        F.round(F.col("defense_rounds_won") * 100.0 / F.when(F.col("defense_rounds_played") == 0, 1).otherwise(F.col("defense_rounds_played")), 2).alias("defense_win_pct"),
        F.col("team_kills"),
        F.col("team_deaths"),
        F.col("team_assists"),
        F.round(F.col("team_kills") / F.when(F.col("team_deaths") == 0, 1.0).otherwise(F.col("team_deaths")), 2).alias("team_kd_ratio"),
        F.col("team_damage_dealt"),
        F.col("team_damage_received"),
        (F.col("team_damage_dealt") - F.col("team_damage_received")).alias("team_damage_differential"),
        F.col("thrifty_rounds_won"),
        F.coalesce(F.col("longest_win_streak"), F.lit(0)).alias("longest_win_streak"),
        F.col("form_guide"),
        F.col("most_played_map"),
        F.col("avg_match_duration_minutes"),
        F.current_timestamp().alias("updated_at")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_team_df.createOrReplaceTempView("staged_gold_team_performance")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_team_performance AS source
ON target.period_type = source.period_type AND target.time_period = source.time_period
WHEN MATCHED THEN
  UPDATE SET
    target.period_start_date = source.period_start_date,
    target.period_end_date = source.period_end_date,
    target.matches_played = source.matches_played,
    target.matches_won = source.matches_won,
    target.matches_lost = source.matches_lost,
    target.matches_drawn = source.matches_drawn,
    target.team_win_pct = source.team_win_pct,
    target.rounds_played = source.rounds_played,
    target.rounds_won = source.rounds_won,
    target.rounds_lost = source.rounds_lost,
    target.round_win_pct = source.round_win_pct,
    target.round_differential = source.round_differential,
    target.avg_round_differential = source.avg_round_differential,
    target.attack_rounds_played = source.attack_rounds_played,
    target.attack_rounds_won = source.attack_rounds_won,
    target.attack_win_pct = source.attack_win_pct,
    target.defense_rounds_played = source.defense_rounds_played,
    target.defense_rounds_won = source.defense_rounds_won,
    target.defense_win_pct = source.defense_win_pct,
    target.team_kills = source.team_kills,
    target.team_deaths = source.team_deaths,
    target.team_assists = source.team_assists,
    target.team_kd_ratio = source.team_kd_ratio,
    target.team_damage_dealt = source.team_damage_dealt,
    target.team_damage_received = source.team_damage_received,
    target.team_damage_differential = source.team_damage_differential,
    target.thrifty_rounds_won = source.thrifty_rounds_won,
    target.longest_win_streak = source.longest_win_streak,
    target.form_guide = source.form_guide,
    target.most_played_map = source.most_played_map,
    target.avg_match_duration_minutes = source.avg_match_duration_minutes,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    period_type, time_period, period_start_date, period_end_date,
    matches_played, matches_won, matches_lost, matches_drawn, team_win_pct,
    rounds_played, rounds_won, rounds_lost, round_win_pct, round_differential, avg_round_differential,
    attack_rounds_played, attack_rounds_won, attack_win_pct,
    defense_rounds_played, defense_rounds_won, defense_win_pct,
    team_kills, team_deaths, team_assists, team_kd_ratio,
    team_damage_dealt, team_damage_received, team_damage_differential,
    thrifty_rounds_won, longest_win_streak, form_guide, most_played_map,
    avg_match_duration_minutes, updated_at
  )
  VALUES (
    source.period_type, source.time_period, source.period_start_date, source.period_end_date,
    source.matches_played, source.matches_won, source.matches_lost, source.matches_drawn, source.team_win_pct,
    source.rounds_played, source.rounds_won, source.rounds_lost, source.round_win_pct, source.round_differential, source.avg_round_differential,
    source.attack_rounds_played, source.attack_rounds_won, source.attack_win_pct,
    source.defense_rounds_played, source.defense_rounds_won, source.defense_win_pct,
    source.team_kills, source.team_deaths, source.team_assists, source.team_kd_ratio,
    source.team_damage_dealt, source.team_damage_received, source.team_damage_differential,
    source.thrifty_rounds_won, source.longest_win_streak, source.form_guide, source.most_played_map,
    source.avg_match_duration_minutes, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Verification & Multi-Granularity Sample Display

# COMMAND ----------
total_rows = spark.table(TARGET_TABLE).count()
weekly_rows = spark.table(TARGET_TABLE).filter(F.col("period_type") == "WEEKLY").count()
fortnightly_rows = spark.table(TARGET_TABLE).filter(F.col("period_type") == "FORTNIGHTLY").count()
monthly_rows = spark.table(TARGET_TABLE).filter(F.col("period_type") == "MONTHLY").count()

print(f"Total Trajectory Records: {total_rows}")
print(f"Weekly Cohorts: {weekly_rows}")
print(f"Fortnightly Cohorts: {fortnightly_rows}")
print(f"Monthly Cohorts: {monthly_rows}")

# Display Monthly Trajectory
print("\nMonthly Performance Trajectory:")
spark.table(TARGET_TABLE).filter(
    F.col("period_type") == "MONTHLY"
).select(
    "time_period", "matches_played", "team_win_pct", "rounds_won", "round_win_pct",
    "attack_win_pct", "defense_win_pct", "team_kd_ratio", "longest_win_streak", "form_guide", "most_played_map"
).orderBy(F.col("time_period").desc()).show(12, truncate=False)
