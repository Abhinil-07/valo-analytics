# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Fact: `fact_round`
# MAGIC 
# MAGIC - **Catalog:** `valorant`
# MAGIC - **Schema:** `silver`
# MAGIC - **Target Table:** `valorant.silver.fact_round`
# MAGIC - **Grain:** 1 row per round per match
# MAGIC - **Composite Primary Key:** `match_id` + `round_number`
# MAGIC - **Purpose:** The core event heartbeat of the game (SRS Section 9, 10, 15, 23).
# MAGIC   Enriches raw rounds with:
# MAGIC   1. **Attack vs Defense Attribution:** `attack_team`, `defense_team`, and `our_team_side`.
# MAGIC   2. **Outcome Mapping:** `is_our_team_win` (did our squad win this round?).
# MAGIC   3. **Spike & Plant Details:** `bomb_planted`, `bomb_defused`, and `plant_site`.
# MAGIC   4. **Economy & Buy Tiers:** `our_buy_tier`, `opponent_buy_tier`, and `is_thrifty`.
# MAGIC - **Write Strategy:** Incremental Delta `MERGE` (Upsert on `match_id` + `round_number`)

# COMMAND ----------
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration
CATALOG = "valorant"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

SOURCE_ROUND = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_round"
SOURCE_PLANT = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_plant_event"
SOURCE_STATS = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_round_player_stats"
SOURCE_DIM_MATCH = f"{CATALOG}.{SILVER_SCHEMA}.dim_match"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.fact_round"

print(f"Source Round:  {SOURCE_ROUND}")
print(f"Source Plant:  {SOURCE_PLANT}")
print(f"Source Stats:  {SOURCE_STATS}")
print(f"Source Match:  {SOURCE_DIM_MATCH}")
print(f"Target Table:  {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Silver Schema and `fact_round` Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    match_id STRING NOT NULL,
    round_number INT NOT NULL,
    round_winner STRING,
    end_type STRING,
    attack_team STRING,
    defense_team STRING,
    our_team_side STRING,
    is_our_team_win BOOLEAN,
    bomb_planted BOOLEAN,
    bomb_defused BOOLEAN,
    plant_site STRING,
    our_team_loadout_value INT,
    opponent_loadout_value INT,
    our_buy_tier STRING,
    opponent_buy_tier STRING,
    is_thrifty BOOLEAN,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Enriched round-level fact table with Attack/Defense attribution, buy tiers, and outcome'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract and Derive Round Attributes
# MAGIC 
# MAGIC 1. **Determine Starting Attacker (1st Half):**
# MAGIC    - Query `bronze_plant_event` for rounds 1–12 to find the team that planted the spike.
# MAGIC    - If no plant in rounds 1–12, standard competitive default is Red attacks first.
# MAGIC 2. **Derive Side per Round:**
# MAGIC    - Rounds 1–12: Team A is Attack, Team B is Defense.
# MAGIC    - Rounds 13–24: Team B is Attack, Team A is Defense (Halftime swap).
# MAGIC    - Rounds 25+ (Overtime): Alternates every 2 rounds.
# MAGIC 3. **Economy & Buy Tiers:**
# MAGIC    - Sum `loadout_value` for each team from `bronze_round_player_stats`.
# MAGIC    - Classify:
# MAGIC      - Rounds 1 & 13: `Pistol Round`
# MAGIC      - Avg Loadout < 1,500: `Eco`
# MAGIC      - Avg Loadout 1,500 - 3,900: `Semi-Buy`
# MAGIC      - Avg Loadout > 3,900: `Full Buy`
# MAGIC 4. **Map to OUR_TEAM perspective using `dim_match.our_team_side`:**
# MAGIC    - Populate `our_team_side` (`Attack` vs `Defense`).
# MAGIC    - Populate `is_our_team_win` and `is_thrifty`.

# COMMAND ----------
# 1. Base Round Data
round_df = spark.table(SOURCE_ROUND).select(
    "match_id",
    "round_number",
    F.initcap(F.col("winning_team")).alias("round_winner"),
    "end_type",
    "bomb_planted",
    "bomb_defused"
)

# 2. Extract Plant Site if spike was planted
plant_df = spark.table(SOURCE_PLANT).select(
    "match_id",
    "round_number",
    "plant_event_sequence",
    F.col("plant_site"),
    F.initcap(F.col("player_team")).alias("planter_team")
)

# Pick the first plant event per round
plant_window = Window.partitionBy("match_id", "round_number").orderBy("plant_event_sequence")
plant_first_df = plant_df.withColumn(
    "rn", F.row_number().over(plant_window)
).filter(F.col("rn") == 1).drop("rn")

# 3. Find Starting Attacker for Each Match (from 1st half plants)
first_half_plants = plant_df.filter(F.col("round_number") <= 12).filter(F.col("planter_team").isNotNull())
plant_match_window = Window.partitionBy("match_id").orderBy("round_number")
starting_attack_df = first_half_plants.withColumn(
    "rn", F.row_number().over(plant_match_window)
).filter(F.col("rn") == 1).select(
    F.col("match_id"),
    F.col("planter_team").alias("start_attacker")
)

# 4. Economy Aggregations per Team per Round from bronze_round_player_stats
stats_df = spark.table(SOURCE_STATS).select(
    "match_id",
    "round_number",
    F.initcap(F.col("player_team")).alias("team"),
    F.col("loadout_value")
)

team_loadout_df = stats_df.groupBy("match_id", "round_number", "team").agg(
    F.sum("loadout_value").alias("team_loadout"),
    F.avg("loadout_value").alias("avg_loadout")
)

red_loadout_df = team_loadout_df.filter(F.col("team") == "Red").select(
    F.col("match_id"),
    F.col("round_number"),
    F.col("team_loadout").alias("red_team_loadout"),
    F.col("avg_loadout").alias("red_avg_loadout")
)

blue_loadout_df = team_loadout_df.filter(F.col("team") == "Blue").select(
    F.col("match_id"),
    F.col("round_number"),
    F.col("team_loadout").alias("blue_team_loadout"),
    F.col("avg_loadout").alias("blue_avg_loadout")
)

# 5. Get our_team_side for the match from dim_match
match_side_df = spark.table(SOURCE_DIM_MATCH).select(
    F.col("match_id"),
    F.initcap(F.col("our_team_color")).alias("our_team_color")  # 'Red' or 'Blue'
)

# 6. Join All Pieces and Compute Derived Fields
staged_fact_round_df = round_df.join(
    plant_first_df.select("match_id", "round_number", "plant_site"),
    on=["match_id", "round_number"],
    how="left"
).join(
    starting_attack_df,
    on="match_id",
    how="left"
).join(
    red_loadout_df,
    on=["match_id", "round_number"],
    how="left"
).join(
    blue_loadout_df,
    on=["match_id", "round_number"],
    how="left"
).join(
    match_side_df,
    on="match_id",
    how="left"
).withColumn(
    # Fallback to 'Red' if no spike was planted in first 12 rounds
    "base_attacker", F.coalesce(F.col("start_attacker"), F.lit("Red"))
).withColumn(
    "base_defender", F.when(F.col("base_attacker") == "Red", "Blue").otherwise("Red")
).withColumn(
    # Attack/Defense Side Derivation
    "attack_team",
    F.when(F.col("round_number") <= 12, F.col("base_attacker"))
     .when((F.col("round_number") > 12) & (F.col("round_number") <= 24), F.col("base_defender"))
     # Overtime (25+): alternates every 2 rounds
     .when((((F.col("round_number") - 25) / 2).cast("int") % 2) == 0, F.col("base_attacker"))
     .otherwise(F.col("base_defender"))
).withColumn(
    "defense_team",
    F.when(F.col("attack_team") == "Red", "Blue").otherwise("Red")
).withColumn(
    # Our Team Side: Is our squad attacking or defending this round?
    "our_team_side",
    F.when(F.col("our_team_color") == F.col("attack_team"), "Attack")
     .when(F.col("our_team_color") == F.col("defense_team"), "Defense")
     .otherwise(None)
).withColumn(
    # Did our squad win this round?
    "is_our_team_win",
    F.when(F.col("round_winner") == F.col("our_team_color"), True)
     .when(F.col("round_winner").isNotNull(), False)
     .otherwise(None)
).withColumn(
    # Loadout Values for Our Team vs Opponent
    "our_team_loadout_value",
    F.when(F.col("our_team_color") == "Red", F.col("red_team_loadout"))
     .when(F.col("our_team_color") == "Blue", F.col("blue_team_loadout"))
     .otherwise(None)
).withColumn(
    "our_avg_loadout",
    F.when(F.col("our_team_color") == "Red", F.col("red_avg_loadout"))
     .when(F.col("our_team_color") == "Blue", F.col("blue_avg_loadout"))
     .otherwise(None)
).withColumn(
    "opponent_loadout_value",
    F.when(F.col("our_team_color") == "Red", F.col("blue_team_loadout"))
     .when(F.col("our_team_color") == "Blue", F.col("red_team_loadout"))
     .otherwise(None)
).withColumn(
    "opponent_avg_loadout",
    F.when(F.col("our_team_color") == "Red", F.col("blue_avg_loadout"))
     .when(F.col("our_team_color") == "Blue", F.col("red_avg_loadout"))
     .otherwise(None)
).withColumn(
    # Buy Tier Classifications
    "our_buy_tier",
    F.when(F.col("round_number").isin(1, 13), "Pistol Round")
     .when(F.col("our_avg_loadout") < 1500, "Eco")
     .when((F.col("our_avg_loadout") >= 1500) & (F.col("our_avg_loadout") <= 3900), "Semi-Buy")
     .when(F.col("our_avg_loadout") > 3900, "Full Buy")
     .otherwise("Unknown")
).withColumn(
    "opponent_buy_tier",
    F.when(F.col("round_number").isin(1, 13), "Pistol Round")
     .when(F.col("opponent_avg_loadout") < 1500, "Eco")
     .when((F.col("opponent_avg_loadout") >= 1500) & (F.col("opponent_avg_loadout") <= 3900), "Semi-Buy")
     .when(F.col("opponent_avg_loadout") > 3900, "Full Buy")
     .otherwise("Unknown")
).withColumn(
    # Thrifty: We won the round AND our team loadout was at least 5000 credits less than opponent
    "is_thrifty",
    (F.col("is_our_team_win") == True) & 
    (F.col("opponent_loadout_value") - F.col("our_team_loadout_value") >= 5000)
).select(
    "match_id",
    "round_number",
    "round_winner",
    "end_type",
    "attack_team",
    "defense_team",
    "our_team_side",
    "is_our_team_win",
    "bomb_planted",
    "bomb_defused",
    "plant_site",
    "our_team_loadout_value",
    "opponent_loadout_value",
    "our_buy_tier",
    "opponent_buy_tier",
    "is_thrifty",
    F.current_timestamp().alias("updated_at")
)

staged_fact_round_df.createOrReplaceTempView("staged_fact_round")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `fact_round`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_fact_round AS source
ON target.match_id = source.match_id AND target.round_number = source.round_number
WHEN MATCHED THEN
  UPDATE SET
    target.round_winner = source.round_winner,
    target.end_type = source.end_type,
    target.attack_team = source.attack_team,
    target.defense_team = source.defense_team,
    target.our_team_side = source.our_team_side,
    target.is_our_team_win = source.is_our_team_win,
    target.bomb_planted = source.bomb_planted,
    target.bomb_defused = source.bomb_defused,
    target.plant_site = source.plant_site,
    target.our_team_loadout_value = source.our_team_loadout_value,
    target.opponent_loadout_value = source.opponent_loadout_value,
    target.our_buy_tier = source.our_buy_tier,
    target.opponent_buy_tier = source.opponent_buy_tier,
    target.is_thrifty = source.is_thrifty,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    match_id,
    round_number,
    round_winner,
    end_type,
    attack_team,
    defense_team,
    our_team_side,
    is_our_team_win,
    bomb_planted,
    bomb_defused,
    plant_site,
    our_team_loadout_value,
    opponent_loadout_value,
    our_buy_tier,
    opponent_buy_tier,
    is_thrifty,
    updated_at
  )
  VALUES (
    source.match_id,
    source.round_number,
    source.round_winner,
    source.end_type,
    source.attack_team,
    source.defense_team,
    source.our_team_side,
    source.is_our_team_win,
    source.bomb_planted,
    source.bomb_defused,
    source.plant_site,
    source.our_team_loadout_value,
    source.opponent_loadout_value,
    source.our_buy_tier,
    source.opponent_buy_tier,
    source.is_thrifty,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
spark.sql(merge_sql).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification & Economy / Side Breakdown

# COMMAND ----------
total_rounds = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
our_wins = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE is_our_team_win = true").collect()[0][0]
thrifty_wins = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE is_thrifty = true").collect()[0][0]

print("=== fact_round Summary ===")
print(f"Total rounds recorded: {total_rounds}")
print(f"Rounds won by our squad: {our_wins} ({round(our_wins*100.0/total_rounds, 1) if total_rounds > 0 else 0}%)")
print(f"Thrifty (eco/budget) wins: {thrifty_wins}")

# Tactical Breakdown: Attack vs Defense Win Rates
display(spark.sql(f"""
    SELECT 
        our_team_side,
        COUNT(*) AS total_rounds,
        SUM(CASE WHEN is_our_team_win THEN 1 ELSE 0 END) AS rounds_won,
        ROUND(AVG(CASE WHEN is_our_team_win THEN 1.0 ELSE 0.0 END) * 100, 1) AS win_rate_pct,
        SUM(CASE WHEN bomb_planted THEN 1 ELSE 0 END) AS plants
    FROM {TARGET_TABLE}
    WHERE our_team_side IS NOT NULL
    GROUP BY our_team_side
"""))

# Tactical Breakdown: Buy Tier Matchups
display(spark.sql(f"""
    SELECT 
        our_buy_tier,
        opponent_buy_tier,
        COUNT(*) AS rounds_played,
        SUM(CASE WHEN is_our_team_win THEN 1 ELSE 0 END) AS rounds_won,
        ROUND(AVG(CASE WHEN is_our_team_win THEN 1.0 ELSE 0.0 END) * 100, 1) AS win_rate_pct
    FROM {TARGET_TABLE}
    WHERE our_buy_tier IS NOT NULL
    GROUP BY our_buy_tier, opponent_buy_tier
    ORDER BY rounds_played DESC
"""))
