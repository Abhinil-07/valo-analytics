# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Table: `gold_combat_performance`
# MAGIC 
# MAGIC - **Grain:** 1 row per `player_puuid` per `weapon_name` (plus an `'ALL_SQUAD'` team rollup)
# MAGIC - **Composite Primary Key:** `player_puuid` + `weapon_name`
# MAGIC - **Source Tables:** 
# MAGIC   - `valorant.silver.fact_round_player` (Weapon equipped per round, damage, shots hit, round wins)
# MAGIC   - `valorant.silver.fact_kill_event` (Fatal weapons, opening kills, trade kills)
# MAGIC   - `valorant.silver.dim_weapon` (Standard shop credit costs, categories, Riot CDN icons)
# MAGIC   - `valorant.gold.gold_player_overall_summary` (Player display names)
# MAGIC - **Target:** `valorant.gold.gold_combat_performance`
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `player_puuid` + `weapon_name`)
# MAGIC 
# MAGIC ### Business Objectives & Power BI Pages Powered:
# MAGIC - **Weapon Arsenal & Tier List:** Vandal vs. Phantom efficiency and overall gun rankings.
# MAGIC - **Deadliest Specialists:** Identifying top Operator, Sheriff, and Rifle specialists across the squad.
# MAGIC - **Gunfight Precision:** Exact Headshot % landed per weapon per player.
# MAGIC - **Opening Duel Impact:** Which weapons generate the highest volume of First Bloods.

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
SOURCE_FACT_ROUND_PLAYER = f"{CATALOG}.silver.fact_round_player"
SOURCE_FACT_KILL         = f"{CATALOG}.silver.fact_kill_event"
SOURCE_DIM_WEAPON        = f"{CATALOG}.silver.dim_weapon"
SOURCE_GOLD_PLAYER       = f"{CATALOG}.gold.gold_player_overall_summary"
TARGET_TABLE             = f"{CATALOG}.{SCHEMA}.gold_combat_performance"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {SOURCE_FACT_ROUND_PLAYER}, {SOURCE_FACT_KILL}, {SOURCE_DIM_WEAPON}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    player_puuid STRING NOT NULL,
    current_display_name STRING NOT NULL,
    weapon_name STRING NOT NULL,
    weapon_category STRING,
    weapon_cost INT,
    weapon_icon_url STRING,
    rounds_equipped INT NOT NULL,
    rounds_won_with_weapon INT NOT NULL,
    weapon_round_win_pct DOUBLE NOT NULL,
    total_kills INT NOT NULL,
    kill_share_pct DOUBLE NOT NULL,
    first_bloods_secured INT NOT NULL,
    trade_kills_secured INT NOT NULL,
    total_damage_dealt INT NOT NULL,
    avg_damage_per_round DOUBLE NOT NULL,
    headshots INT NOT NULL,
    bodyshots INT NOT NULL,
    legshots INT NOT NULL,
    weapon_headshot_pct DOUBLE NOT NULL,
    credits_per_kill INT NOT NULL,
    is_primary_weapon BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Gold combat performance: weapon arsenal, lethal efficiency, headshot accuracy, and duel conversion'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Round Equipment and Fatal Kill Events

# COMMAND ----------
# 1. Round Equipment, Damage, and Shots from fact_round_player
round_player_df = spark.table(SOURCE_FACT_ROUND_PLAYER).filter(
    (F.col("is_our_team") == True) & F.col("weapon_name").isNotNull()
)

# 2. Fatal Kills, First Bloods, and Trade Kills from fact_kill_event
kill_event_df = spark.table(SOURCE_FACT_KILL).filter(
    (F.col("is_killer_our_team") == True) & F.col("damage_weapon_name").isNotNull()
)

# 3. Weapon Metadata
dim_weapon_df = spark.table(SOURCE_DIM_WEAPON).select(
    "weapon_name", "weapon_category", "weapon_cost", "weapon_icon_url"
)

# 4. Player Display Names
player_names_df = spark.table(SOURCE_GOLD_PLAYER).select(
    "player_puuid", "current_display_name"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Helper Function to Aggregate Combat by Player and Weapon

# COMMAND ----------
def aggregate_combat(df_rounds, df_kills, player_id_col):
    # Round-level aggregates
    rounds_agg = df_rounds.groupBy(player_id_col, "weapon_name").agg(
        F.count("*").alias("rounds_equipped"),
        F.count(F.when(F.col("is_round_win") == True, 1)).alias("rounds_won_with_weapon"),
        F.sum("damage").alias("total_damage_dealt"),
        F.sum("headshots").alias("headshots"),
        F.sum("bodyshots").alias("bodyshots"),
        F.sum("legshots").alias("legshots")
    )

    # Kill-level aggregates
    kills_agg = df_kills.groupBy(player_id_col, F.col("damage_weapon_name").alias("weapon_name")).agg(
        F.count("*").alias("total_kills"),
        F.count(F.when(F.col("is_opening_kill") == True, 1)).alias("first_bloods_secured"),
        F.count(F.when(F.col("is_trade_kill") == True, 1)).alias("trade_kills_secured")
    )

    # Full outer join on weapon
    return rounds_agg.join(kills_agg, on=[player_id_col, "weapon_name"], how="full_outer")

# 1. Per-Player Combat Records
player_combat_raw = aggregate_combat(
    round_player_df,
    kill_event_df.withColumnRenamed("killer_puuid", "player_puuid"),
    "player_puuid"
)

# 2. Squad-Wide Macro Rollup ('ALL_SQUAD')
all_squad_rounds = round_player_df.withColumn("player_puuid", F.lit("ALL_SQUAD"))
all_squad_kills  = kill_event_df.withColumn("killer_puuid", F.lit("ALL_SQUAD"))
all_squad_combat_raw = aggregate_combat(
    all_squad_rounds,
    all_squad_kills.withColumnRenamed("killer_puuid", "player_puuid"),
    "player_puuid"
)

# Union Per-Player and Macro Rollup
unified_combat_raw = player_combat_raw.unionByName(all_squad_combat_raw)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Enrich with Dimension Metadata, Accuracy %, and Primary Flags

# COMMAND ----------
enriched_combat_df = unified_combat_raw \
    .join(dim_weapon_df, on="weapon_name", how="left") \
    .join(player_names_df, on="player_puuid", how="left") \
    .withColumn(
        "current_display_name",
        F.when(F.col("player_puuid") == "ALL_SQUAD", F.lit("Team Total")).otherwise(F.coalesce(F.col("current_display_name"), F.col("player_puuid")))
    )

# Compute Total Kills per Entity to derive kill_share_pct
w_player = Window.partitionBy("player_puuid")
w_rank   = Window.partitionBy("player_puuid").orderBy(F.col("total_kills").desc(), F.col("rounds_equipped").desc())

final_gold_combat_df = enriched_combat_df.withColumn(
    "rounds_equipped", F.coalesce(F.col("rounds_equipped"), F.lit(0))
).withColumn(
    "rounds_won_with_weapon", F.coalesce(F.col("rounds_won_with_weapon"), F.lit(0))
).withColumn(
    "total_kills", F.coalesce(F.col("total_kills"), F.lit(0))
).withColumn(
    "first_bloods_secured", F.coalesce(F.col("first_bloods_secured"), F.lit(0))
).withColumn(
    "trade_kills_secured", F.coalesce(F.col("trade_kills_secured"), F.lit(0))
).withColumn(
    "total_damage_dealt", F.coalesce(F.col("total_damage_dealt"), F.lit(0))
).withColumn(
    "headshots", F.coalesce(F.col("headshots"), F.lit(0))
).withColumn(
    "bodyshots", F.coalesce(F.col("bodyshots"), F.lit(0))
).withColumn(
    "legshots", F.coalesce(F.col("legshots"), F.lit(0))
).withColumn(
    "weapon_cost", F.coalesce(F.col("weapon_cost"), F.lit(0))
).withColumn(
    "player_total_kills", F.sum("total_kills").over(w_player)
).withColumn(
    "weapon_rank", F.row_number().over(w_rank)
).select(
    F.col("player_puuid"),
    F.col("current_display_name"),
    F.col("weapon_name"),
    F.col("weapon_category"),
    F.col("weapon_cost"),
    F.col("weapon_icon_url"),
    F.col("rounds_equipped"),
    F.col("rounds_won_with_weapon"),
    F.round(F.col("rounds_won_with_weapon") * 100.0 / F.when(F.col("rounds_equipped") == 0, 1).otherwise(F.col("rounds_equipped")), 2).alias("weapon_round_win_pct"),
    F.col("total_kills"),
    F.round(F.col("total_kills") * 100.0 / F.when(F.col("player_total_kills") == 0, 1).otherwise(F.col("player_total_kills")), 2).alias("kill_share_pct"),
    F.col("first_bloods_secured"),
    F.col("trade_kills_secured"),
    F.col("total_damage_dealt"),
    F.round(F.col("total_damage_dealt") / F.when(F.col("rounds_equipped") == 0, 1.0).otherwise(F.col("rounds_equipped")), 1).alias("avg_damage_per_round"),
    F.col("headshots"),
    F.col("bodyshots"),
    F.col("legshots"),
    F.round(F.col("headshots") * 100.0 / F.when((F.col("headshots") + F.col("bodyshots") + F.col("legshots")) == 0, 1).otherwise(F.col("headshots") + F.col("bodyshots") + F.col("legshots")), 2).alias("weapon_headshot_pct"),
    F.round((F.col("rounds_equipped") * F.col("weapon_cost")) / F.when(F.col("total_kills") == 0, 1.0).otherwise(F.col("total_kills"))).cast("int").alias("credits_per_kill"),
    F.when(F.col("weapon_rank") == 1, True).otherwise(False).alias("is_primary_weapon"),
    F.current_timestamp().alias("updated_at")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Incremental MERGE into Target Gold Table

# COMMAND ----------
final_gold_combat_df.createOrReplaceTempView("staged_gold_combat")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_gold_combat AS source
ON target.player_puuid = source.player_puuid AND target.weapon_name = source.weapon_name
WHEN MATCHED THEN
  UPDATE SET
    target.current_display_name = source.current_display_name,
    target.weapon_category = source.weapon_category,
    target.weapon_cost = source.weapon_cost,
    target.weapon_icon_url = source.weapon_icon_url,
    target.rounds_equipped = source.rounds_equipped,
    target.rounds_won_with_weapon = source.rounds_won_with_weapon,
    target.weapon_round_win_pct = source.weapon_round_win_pct,
    target.total_kills = source.total_kills,
    target.kill_share_pct = source.kill_share_pct,
    target.first_bloods_secured = source.first_bloods_secured,
    target.trade_kills_secured = source.trade_kills_secured,
    target.total_damage_dealt = source.total_damage_dealt,
    target.avg_damage_per_round = source.avg_damage_per_round,
    target.headshots = source.headshots,
    target.bodyshots = source.bodyshots,
    target.legshots = source.legshots,
    target.weapon_headshot_pct = source.weapon_headshot_pct,
    target.credits_per_kill = source.credits_per_kill,
    target.is_primary_weapon = source.is_primary_weapon,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    player_puuid, current_display_name, weapon_name, weapon_category, weapon_cost, weapon_icon_url,
    rounds_equipped, rounds_won_with_weapon, weapon_round_win_pct,
    total_kills, kill_share_pct, first_bloods_secured, trade_kills_secured,
    total_damage_dealt, avg_damage_per_round, headshots, bodyshots, legshots,
    weapon_headshot_pct, credits_per_kill, is_primary_weapon, updated_at
  )
  VALUES (
    source.player_puuid, source.current_display_name, source.weapon_name, source.weapon_category, source.weapon_cost, source.weapon_icon_url,
    source.rounds_equipped, source.rounds_won_with_weapon, source.weapon_round_win_pct,
    source.total_kills, source.kill_share_pct, source.first_bloods_secured, source.trade_kills_secured,
    source.total_damage_dealt, source.avg_damage_per_round, source.headshots, source.bodyshots, source.legshots,
    source.weapon_headshot_pct, source.credits_per_kill, source.is_primary_weapon, source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
merge_result = spark.sql(merge_sql)
merge_result.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Verification & Arsenal Leaderboard Display

# COMMAND ----------
total_combat_rows = spark.table(TARGET_TABLE).count()
print(f"Total Combat Arsenal Records: {total_combat_rows}")

# Display Squad Weapon Tier List
print("Overall Squad Weapon Tier List ('Team Total'):")
spark.table(TARGET_TABLE).filter(
    F.col("player_puuid") == "ALL_SQUAD"
).select(
    "weapon_name", "weapon_category", "total_kills", "kill_share_pct",
    "weapon_headshot_pct", "first_bloods_secured", "weapon_round_win_pct", "is_primary_weapon"
).orderBy(F.col("total_kills").desc()).show(15, truncate=False)
