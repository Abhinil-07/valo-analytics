# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Dimension: `dim_team_roster`
# MAGIC 
# MAGIC - **Catalog:** `valorant`
# MAGIC - **Schema:** `silver`
# MAGIC - **Target Table:** `valorant.silver.dim_team_roster`
# MAGIC - **Grain:** 1 row per roster member
# MAGIC - **Primary Key:** `player_puuid`
# MAGIC - **Purpose:** Official roster configuration representing the team (SRS Section 24 & 27).
# MAGIC   Identifies our core players, assigns team roles, and powers automated `OUR_TEAM` resolution.
# MAGIC - **Write Strategy:** Delta `MERGE` (Upsert on `player_puuid`)

# COMMAND ----------
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType

# Configuration
CATALOG = "valorant"
SILVER_SCHEMA = "silver"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_team_roster"

print(f"Target Table: {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Silver Schema and `dim_team_roster` Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    player_puuid STRING NOT NULL,
    player_nickname STRING,
    default_display_name STRING,
    team_role STRING,
    is_team_owner BOOLEAN NOT NULL,
    is_active_roster BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Official team roster configuration table for OUR_TEAM resolution and player identity'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Seed Official Team Roster

# COMMAND ----------
# Roster Seed Data (Agamemnon and core 5-stack teammates)
roster_data = [
    (
        "c38ffa2e-ce9d-5d95-9399-3a89c6af6b16",
        "Agamemnon",
        "Agamemnon#Lord",
        "Duelist",
        True,   # is_team_owner
        True    # is_active_roster
    ),
    (
        "59bae8f3-025c-5dcc-9a1c-c903279e4145",
        "systemctl",
        "systemctl start#4575",
        "IGL",
        False,
        True
    ),
    (
        "f91099e8-a14b-5913-b66b-13717562a6eb",
        "NoSheat",
        "NoSheat#6917",
        "Core",
        False,
        True
    ),
    (
        "9ac37245-e47a-5977-9785-7c2590e2dcda",
        "SC4R",
        "SC4R#LORD",
        "Core",
        False,
        True
    ),
    (
        "5775df6c-0f15-5e6c-8f00-3398dc77d351",
        "Garamhe",
        "GaramheGaramhe#ahhh",
        "Core",
        False,
        True
    )
]

schema = StructType([
    StructField("player_puuid", StringType(), False),
    StructField("player_nickname", StringType(), True),
    StructField("default_display_name", StringType(), True),
    StructField("team_role", StringType(), True),
    StructField("is_team_owner", BooleanType(), False),
    StructField("is_active_roster", BooleanType(), False)
])

staged_roster_df = spark.createDataFrame(roster_data, schema=schema).withColumn(
    "created_at", F.current_timestamp()
).withColumn(
    "updated_at", F.current_timestamp()
)

staged_roster_df.createOrReplaceTempView("staged_team_roster")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `dim_team_roster`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_team_roster AS source
ON target.player_puuid = source.player_puuid
WHEN MATCHED THEN
  UPDATE SET
    target.player_nickname = source.player_nickname,
    target.default_display_name = source.default_display_name,
    target.team_role = source.team_role,
    target.is_team_owner = source.is_team_owner,
    target.is_active_roster = source.is_active_roster,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    player_puuid,
    player_nickname,
    default_display_name,
    team_role,
    is_team_owner,
    is_active_roster,
    created_at,
    updated_at
  )
  VALUES (
    source.player_puuid,
    source.player_nickname,
    source.default_display_name,
    source.team_role,
    source.is_team_owner,
    source.is_active_roster,
    source.created_at,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
spark.sql(merge_sql).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification

# COMMAND ----------
roster_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Total roster members registered: {roster_count}")

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY is_team_owner DESC, default_display_name ASC"))
