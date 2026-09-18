# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_player_overall_summary`
# MAGIC 
# MAGIC - **Grain:** 1 row per squad member (lifetime career aggregate)
# MAGIC - **Primary Key:** `player_puuid`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.gold.gold_player_match_performance` (Match-level scorecards, MVPs, top fraggers)
# MAGIC   - `valorant.silver.fact_match_player` (Shots hit, kills, deaths, damage, abilities)
# MAGIC   - `valorant.silver.fact_round_player` (Rounds played, rounds won, credits spent)
# MAGIC   - `valorant.silver.fact_kill_event` (Opening duels / First Bloods, Trade Kills)
# MAGIC   - `valorant.silver.dim_agent` (Riot CDN icons and agent roles)
# MAGIC - **Target:** `valorant.gold.gold_player_overall_summary`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `player_puuid`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Player Profile Cards:** Hero KPI cards (Agamemnon, Subham, etc.) with signature agent portrait, career K/D, win %, and ACS.
# MAGIC - **Squad Leaderboard:** Bar/radar charts comparing headshot %, First Blood conversion, and Trade Kill efficiency.
# MAGIC - **Accolades & Accreditations:** Career Match MVP count and Top Fragger awards.

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
SOURCE_FACT_MATCH_PLAYER = f"{CATALOG}.silver.fact_match_player"
SOURCE_FACT_ROUND_PLAYER = f"{CATALOG}.silver.fact_round_player"
SOURCE_FACT_KILL         = f"{CATALOG}.silver.fact_kill_event"
SOURCE_DIM_AGENT         = f"{CATALOG}.silver.dim_agent"
TARGET_TABLE             = f"{CATALOG}.{SCHEMA}.gold_player_overall_summary"

print(f"Target Table: {TARGET_TABLE}")

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
    player_tag STRING,
    current_display_name STRING,
    roster_role STRING,
    is_core_team BOOLEAN,
    total_matches_played INT,
    matches_won INT,
    match_win_pct DOUBLE,
    total_rounds_played INT,
    rounds_won INT,
    round_win_pct DOUBLE,
    total_kills INT,
    total_deaths INT,
    total_assists INT,
    career_kd_ratio DOUBLE,
    career_kill_differential INT,
    first_blood_count INT,
    first_death_count INT,
    first_blood_differential INT,
    trade_kill_count INT,
    career_avg_acs DOUBLE,
    career_avg_adr DOUBLE,
    career_headshot_pct DOUBLE,
    career_bodyshot_pct DOUBLE,
    career_legshot_pct DOUBLE,
    most_played_agent STRING,
    most_played_agent_role STRING,
    most_played_agent_icon_url STRING,
    most_played_agent_matches INT,
    most_played_agent_win_pct DOUBLE,
    highest_winrate_agent STRING,
    total_ultimate_casts INT,
    total_ability_casts INT,
    avg_abilities_per_round DOUBLE,
    career_avg_spent DOUBLE,
    match_mvp_count INT,
    team_top_fragger_count INT,
    overall_rating STRING,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Gold player lifetime career summary and squad leaderboards'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract, Aggregate, and Calculate Lifetime Player Metrics

# COMMAND ----------
# 1. Base Match Scorecard Aggregations from gold_player_match_performance
player_match_df = spark.table(SOURCE_GOLD_PLAYER_MATCH)

# Window to resolve most recent display names
player_recency_window = Window.partitionBy("player_puuid").orderBy(F.col("match_date").desc(), F.col("match_id").desc())

player_identities_df = player_match_df.withColumn(
    "rn", F.row_number().over(player_recency_window)
).filter(
    F.col("rn") == 1
).select(
    F.col("player_puuid"),
    F.col("player_name"),
    F.col("player_tag"),
    F.col("current_display_name"),
    F.col("roster_role"),
    F.col("is_core_team")
)

# Match-level Career Totals
match_summary_df = player_match_df.groupBy("player_puuid").agg(
    F.countDistinct("match_id").alias("total_matches_played"),
    F.count(F.when(F.col("is_match_win") == True, 1)).alias("matches_won"),
    F.round(F.count(F.when(F.col("is_match_win") == True, 1)) * 100.0 / F.countDistinct("match_id"), 2).alias("match_win_pct"),
    F.sum("kills").alias("total_kills"),
    F.sum("deaths").alias("total_deaths"),
    F.sum("assists").alias("total_assists"),
    F.round(F.avg("average_combat_score"), 1).alias("career_avg_acs"),
    F.round(F.avg("average_damage_per_round"), 1).alias("career_avg_adr"),
    F.sum("ultimate_casts").alias("total_ultimate_casts"),
    F.sum("total_ability_casts").alias("total_ability_casts"),
    F.count(F.when(F.col("is_match_mvp") == True, 1)).alias("match_mvp_count"),
    F.count(F.when(F.col("is_team_top_fragger") == True, 1)).alias("team_top_fragger_count")
).withColumn(
    "career_kd_ratio",
    F.round(F.col("total_kills") / F.when(F.col("total_deaths") == 0, 1.0).otherwise(F.col("total_deaths")), 2)
).withColumn(
    "career_kill_differential",
    F.col("total_kills") - F.col("total_deaths")
)

# 2. Lifetime Accuracy Breakdown from fact_match_player (squad players only)
accuracy_df = spark.table(SOURCE_FACT_MATCH_PLAYER).filter(
    F.col("is_our_team") == True
).groupBy("player_puuid").agg(
    F.sum("headshots").alias("total_headshots"),
    F.sum("bodyshots").alias("total_bodyshots"),
    F.sum("legshots").alias("total_legshots")
).withColumn(
    "total_shots_hit",
    F.col("total_headshots") + F.col("total_bodyshots") + F.col("total_legshots")
).withColumn(
    "career_headshot_pct",
    F.round(F.col("total_headshots") * 100.0 / F.when(F.col("total_shots_hit") == 0, 1).otherwise(F.col("total_shots_hit")), 2)
).withColumn(
    "career_bodyshot_pct",
    F.round(F.col("total_bodyshots") * 100.0 / F.when(F.col("total_shots_hit") == 0, 1).otherwise(F.col("total_shots_hit")), 2)
).withColumn(
    "career_legshot_pct",
    F.round(F.col("total_legshots") * 100.0 / F.when(F.col("total_shots_hit") == 0, 1).otherwise(F.col("total_shots_hit")), 2)
).select(
    "player_puuid", "career_headshot_pct", "career_bodyshot_pct", "career_legshot_pct"
)

# 3. Round Volumes & Economy from fact_round_player (squad players only)
round_summary_df = spark.table(SOURCE_FACT_ROUND_PLAYER).filter(
    F.col("is_our_team") == True
).groupBy("player_puuid").agg(
    F.count(F.lit(1)).alias("total_rounds_played"),
    F.count(F.when(F.col("is_round_win") == True, 1)).alias("rounds_won"),
    F.round(F.avg("spent_credits"), 1).alias("career_avg_spent")
).withColumn(
    "round_win_pct",
    F.round(F.col("rounds_won") * 100.0 / F.when(F.col("total_rounds_played") == 0, 1).otherwise(F.col("total_rounds_played")), 2)
)

# 4. Opening Duels (First Bloods / First Deaths) and Trade Kills from fact_kill_event
kill_event_df = spark.table(SOURCE_FACT_KILL)

opening_kills_df = kill_event_df.filter(
    (F.col("is_opening_kill") == True) & (F.col("is_killer_our_team") == True)
).groupBy("killer_puuid").agg(
    F.count(F.lit(1)).alias("first_blood_count")
).withColumnRenamed("killer_puuid", "player_puuid")

opening_deaths_df = kill_event_df.filter(
    (F.col("is_opening_death") == True) & (F.col("is_victim_our_team") == True)
).groupBy("victim_puuid").agg(
    F.count(F.lit(1)).alias("first_death_count")
).withColumnRenamed("victim_puuid", "player_puuid")

trade_kills_df = kill_event_df.filter(
    (F.col("is_trade_kill") == True) & (F.col("is_killer_our_team") == True)
).groupBy("killer_puuid").agg(
    F.count(F.lit(1)).alias("trade_kill_count")
).withColumnRenamed("killer_puuid", "player_puuid")

# 5. Signature Agent & Highest Win Rate Agent Analysis
agent_stats_df = player_match_df.groupBy(
    "player_puuid", "agent_name", "agent_role", "agent_icon_url"
).agg(
    F.count(F.lit(1)).alias("agent_matches"),
    F.count(F.when(F.col("is_match_win") == True, 1)).alias("agent_wins"),
    F.round(F.count(F.when(F.col("is_match_win") == True, 1)) * 100.0 / F.count(F.lit(1)), 2).alias("agent_win_pct")
)

# Most Played Agent Window
most_played_window = Window.partitionBy("player_puuid").orderBy(
    F.col("agent_matches").desc(), F.col("agent_win_pct").desc()
)

signature_agent_df = agent_stats_df.withColumn(
    "rank", F.row_number().over(most_played_window)
).filter(
    F.col("rank") == 1
).select(
    F.col("player_puuid"),
    F.col("agent_name").alias("most_played_agent"),
    F.col("agent_role").alias("most_played_agent_role"),
    F.col("agent_icon_url").alias("most_played_agent_icon_url"),
    F.col("agent_matches").alias("most_played_agent_matches"),
    F.col("agent_win_pct").alias("most_played_agent_win_pct")
)

# Highest Win Rate Agent (min 3 matches)
high_winrate_window = Window.partitionBy("player_puuid").orderBy(
    F.col("agent_win_pct").desc(), F.col("agent_matches").desc()
)

high_winrate_agent_df = agent_stats_df.filter(
    F.col("agent_matches") >= 3
).withColumn(
    "rank", F.row_number().over(high_winrate_window)
).filter(
    F.col("rank") == 1
).select(
    F.col("player_puuid"),
    F.col("agent_name").alias("highest_winrate_agent")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Combine All Metrics and Assign Overall Rating

# COMMAND ----------
gold_player_overall_df = player_identities_df \
    .join(match_summary_df, on="player_puuid", how="inner") \
    .join(round_summary_df, on="player_puuid", how="left") \
    .join(accuracy_df, on="player_puuid", how="left") \
    .join(opening_kills_df, on="player_puuid", how="left") \
    .join(opening_deaths_df, on="player_puuid", how="left") \
    .join(trade_kills_df, on="player_puuid", how="left") \
    .join(signature_agent_df, on="player_puuid", how="left") \
    .join(high_winrate_agent_df, on="player_puuid", how="left")

final_gold_player_df = gold_player_overall_df.select(
    F.col("player_puuid"),
    F.col("player_name"),
    F.col("player_tag"),
    F.col("current_display_name"),
    F.col("roster_role"),
    F.coalesce(F.col("is_core_team"), F.lit(False)).alias("is_core_team"),
    F.col("total_matches_played"),
    F.col("matches_won"),
    F.col("match_win_pct"),
    F.coalesce(F.col("total_rounds_played"), F.lit(0)).alias("total_rounds_played"),
    F.coalesce(F.col("rounds_won"), F.lit(0)).alias("rounds_won"),
    F.coalesce(F.col("round_win_pct"), F.lit(0.0)).alias("round_win_pct"),
    F.col("total_kills"),
    F.col("total_deaths"),
    F.col("total_assists"),
    F.col("career_kd_ratio"),
    F.col("career_kill_differential"),
    F.coalesce(F.col("first_blood_count"), F.lit(0)).alias("first_blood_count"),
    F.coalesce(F.col("first_death_count"), F.lit(0)).alias("first_death_count"),
    (F.coalesce(F.col("first_blood_count"), F.lit(0)) - F.coalesce(F.col("first_death_count"), F.lit(0))).alias("first_blood_differential"),
    F.coalesce(F.col("trade_kill_count"), F.lit(0)).alias("trade_kill_count"),
    F.col("career_avg_acs"),
    F.col("career_avg_adr"),
    F.coalesce(F.col("career_headshot_pct"), F.lit(0.0)).alias("career_headshot_pct"),
    F.coalesce(F.col("career_bodyshot_pct"), F.lit(0.0)).alias("career_bodyshot_pct"),
    F.coalesce(F.col("career_legshot_pct"), F.lit(0.0)).alias("career_legshot_pct"),
    F.col("most_played_agent"),
    F.col("most_played_agent_role"),
    F.col("most_played_agent_icon_url"),
    F.col("most_played_agent_matches"),
    F.col("most_played_agent_win_pct"),
    F.coalesce(F.col("highest_winrate_agent"), F.col("most_played_agent")).alias("highest_winrate_agent"),
    F.col("total_ultimate_casts"),
    F.col("total_ability_casts"),
    F.round(F.col("total_ability_casts") / F.when(F.coalesce(F.col("total_rounds_played"), F.lit(0)) == 0, 1.0).otherwise(F.col("total_rounds_played")), 2).alias("avg_abilities_per_round"),
    F.coalesce(F.col("career_avg_spent"), F.lit(0.0)).alias("career_avg_spent"),
    F.col("match_mvp_count"),
    F.col("team_top_fragger_count"),
    F.when((F.col("career_avg_acs") >= 240) | (F.col("career_kd_ratio") >= 1.20), F.lit("Elite"))
     .when((F.col("career_avg_acs") >= 180) | (F.col("career_kd_ratio") >= 1.00), F.lit("Solid"))
     .otherwise(F.lit("Developing")).alias("overall_rating"),
    F.current_timestamp().alias("updated_at")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_player_df.createOrReplaceTempView("staged_gold_player_overall")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_player_overall AS source
ON target.player_puuid = source.player_puuid
WHEN MATCHED THEN
  UPDATE SET
    target.player_name = source.player_name,
    target.player_tag = source.player_tag,
    target.current_display_name = source.current_display_name,
    target.roster_role = source.roster_role,
    target.is_core_team = source.is_core_team,
    target.total_matches_played = source.total_matches_played,
    target.matches_won = source.matches_won,
    target.match_win_pct = source.match_win_pct,
    target.total_rounds_played = source.total_rounds_played,
    target.rounds_won = source.rounds_won,
    target.round_win_pct = source.round_win_pct,
    target.total_kills = source.total_kills,
    target.total_deaths = source.total_deaths,
    target.total_assists = source.total_assists,
    target.career_kd_ratio = source.career_kd_ratio,
    target.career_kill_differential = source.career_kill_differential,
    target.first_blood_count = source.first_blood_count,
    target.first_death_count = source.first_death_count,
    target.first_blood_differential = source.first_blood_differential,
    target.trade_kill_count = source.trade_kill_count,
    target.career_avg_acs = source.career_avg_acs,
    target.career_avg_adr = source.career_avg_adr,
    target.career_headshot_pct = source.career_headshot_pct,
    target.career_bodyshot_pct = source.career_bodyshot_pct,
    target.career_legshot_pct = source.career_legshot_pct,
    target.most_played_agent = source.most_played_agent,
    target.most_played_agent_role = source.most_played_agent_role,
    target.most_played_agent_icon_url = source.most_played_agent_icon_url,
    target.most_played_agent_matches = source.most_played_agent_matches,
    target.most_played_agent_win_pct = source.most_played_agent_win_pct,
    target.highest_winrate_agent = source.highest_winrate_agent,
    target.total_ultimate_casts = source.total_ultimate_casts,
    target.total_ability_casts = source.total_ability_casts,
    target.avg_abilities_per_round = source.avg_abilities_per_round,
    target.career_avg_spent = source.career_avg_spent,
    target.match_mvp_count = source.match_mvp_count,
    target.team_top_fragger_count = source.team_top_fragger_count,
    target.overall_rating = source.overall_rating,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    player_puuid, player_name, player_tag, current_display_name, roster_role, is_core_team,
    total_matches_played, matches_won, match_win_pct, total_rounds_played, rounds_won, round_win_pct,
    total_kills, total_deaths, total_assists, career_kd_ratio, career_kill_differential,
    first_blood_count, first_death_count, first_blood_differential, trade_kill_count,
    career_avg_acs, career_avg_adr, career_headshot_pct, career_bodyshot_pct, career_legshot_pct,
    most_played_agent, most_played_agent_role, most_played_agent_icon_url, most_played_agent_matches,
    most_played_agent_win_pct, highest_winrate_agent, total_ultimate_casts, total_ability_casts,
    avg_abilities_per_round, career_avg_spent, match_mvp_count, team_top_fragger_count,
    overall_rating, updated_at
  )
  VALUES (
    source.player_puuid, source.player_name, source.player_tag, source.current_display_name, source.roster_role, source.is_core_team,
    source.total_matches_played, source.matches_won, source.match_win_pct, source.total_rounds_played, source.rounds_won, source.round_win_pct,
    source.total_kills, source.total_deaths, source.total_assists, source.career_kd_ratio, source.career_kill_differential,
    source.first_blood_count, source.first_death_count, source.first_blood_differential, source.trade_kill_count,
    source.career_avg_acs, source.career_avg_adr, source.career_headshot_pct, source.career_bodyshot_pct, source.career_legshot_pct,
    source.most_played_agent, source.most_played_agent_role, source.most_played_agent_icon_url, source.most_played_agent_matches,
    source.most_played_agent_win_pct, source.highest_winrate_agent, source.total_ultimate_casts, source.total_ability_casts,
    source.avg_abilities_per_round, source.career_avg_spent, source.match_mvp_count, source.team_top_fragger_count,
    source.overall_rating, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Verification & Squad Leaderboard Display

# COMMAND ----------
total_players = spark.table(TARGET_TABLE).count()
core_players = spark.table(TARGET_TABLE).filter(F.col("is_core_team") == True).count()

print(f"Total Squad Members Recorded: {total_players}")
print(f"Core Roster Members: {core_players}")

# Display Leaderboard
print("Squad Career Leaderboard (Sorted by ACS):")
spark.table(TARGET_TABLE).select(
    "current_display_name", "roster_role", "total_matches_played", "match_win_pct",
    "career_kd_ratio", "career_avg_acs", "career_headshot_pct",
    "most_played_agent", "first_blood_count", "match_mvp_count", "overall_rating"
).orderBy(F.col("career_avg_acs").desc()).show(20, truncate=False)
