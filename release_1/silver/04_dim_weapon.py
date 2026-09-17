# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Dimension: `dim_weapon`
# MAGIC 
# MAGIC - **Catalog:** `valorant`
# MAGIC - **Schema:** `silver`
# MAGIC - **Target Table:** `valorant.silver.dim_weapon`
# MAGIC - **Grain:** 1 row per unique weapon
# MAGIC - **Primary Key:** `weapon_name`
# MAGIC - **Purpose:** Master weapon catalog detailing tactical category (`Rifle`, `Sniper`, `Sidearm`, `SMG`, `Shotgun`, `Heavy`, `Melee`),
# MAGIC   standard purchase cost in credits, and official Riot CDN silhouette image URLs for native rendering in Databricks AI/BI Dashboards (SRS Section 18 & 23).
# MAGIC - **Write Strategy:** Delta `MERGE` (Upsert on `weapon_name`)

# COMMAND ----------
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Configuration
CATALOG = "valorant"
SILVER_SCHEMA = "silver"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_weapon"

print(f"Target Table: {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Silver Schema and `dim_weapon` Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    weapon_name STRING NOT NULL,
    weapon_category STRING,
    weapon_cost INT,
    weapon_icon_url STRING,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Master weapon dimension with category, standard credit cost, and CDN icon URLs'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Seed Official Valorant Weapons Dataset

# COMMAND ----------
weapon_seed_data = [
    ("Ares", "Heavy", 1600, "https://media.valorant-api.com/weapons/55d8a0f4-4274-ca67-fe2c-06ab45efdf58/displayicon.png"),
    ("Bandit", "Sidearm", 600, "https://media.valorant-api.com/weapons/410b2e0b-4ceb-1321-1727-20858f7f3477/displayicon.png"),
    ("Bucky", "Shotguns", 850, "https://media.valorant-api.com/weapons/910be174-449b-c412-ab22-d0873436b21b/displayicon.png"),
    ("Bulldog", "Rifles", 2050, "https://media.valorant-api.com/weapons/ae3de142-4d85-2547-dd26-4e90bed35cf7/displayicon.png"),
    ("Classic", "Sidearm", 0, "https://media.valorant-api.com/weapons/29a0cfab-485b-f5d5-779a-b59f85e204a8/displayicon.png"),
    ("Frenzy", "Sidearm", 450, "https://media.valorant-api.com/weapons/44d4e95c-4157-0037-81b2-17841bf2e8e3/displayicon.png"),
    ("Ghost", "Sidearm", 500, "https://media.valorant-api.com/weapons/1baa85b4-4c70-1284-64bb-6481dfc3bb4e/displayicon.png"),
    ("Guardian", "Rifles", 2250, "https://media.valorant-api.com/weapons/4ade7faa-4cf1-8376-95ef-39884480959b/displayicon.png"),
    ("Judge", "Shotguns", 1850, "https://media.valorant-api.com/weapons/ec845bf4-4f79-ddda-a3da-0db3774b2794/displayicon.png"),
    ("Marshal", "Sniper Rifles", 950, "https://media.valorant-api.com/weapons/c4883e50-4494-202c-3ec3-6b8a9284f00b/displayicon.png"),
    ("Melee", "Melee", 0, "https://media.valorant-api.com/weapons/2f59173c-4bed-b6c3-2191-dea9b58be9c7/displayicon.png"),
    ("Odin", "Heavy", 3200, "https://media.valorant-api.com/weapons/63e6c2b6-4a8e-869c-3d4c-e38355226584/displayicon.png"),
    ("Operator", "Sniper Rifles", 4700, "https://media.valorant-api.com/weapons/a03b24d3-4319-996d-0f8c-94bbfba1dfc7/displayicon.png"),
    ("Outlaw", "Sniper Rifles", 2400, "https://media.valorant-api.com/weapons/5f0aaf7a-4289-3998-d5ff-eb9a5cf7ef5c/displayicon.png"),
    ("Phantom", "Rifles", 2900, "https://media.valorant-api.com/weapons/ee8e8d15-496b-07ac-e5f6-8fae5d4c7b1a/displayicon.png"),
    ("Sheriff", "Sidearm", 800, "https://media.valorant-api.com/weapons/e336c6b8-418d-9340-d77f-7a9e4cfe0702/displayicon.png"),
    ("Shorty", "Sidearm", 300, "https://media.valorant-api.com/weapons/42da8ccc-40d5-affc-beec-15aa47b42eda/displayicon.png"),
    ("Spectre", "SMGs", 1600, "https://media.valorant-api.com/weapons/462080d1-4035-2937-7c09-27aa2a5c27a7/displayicon.png"),
    ("Stinger", "SMGs", 1100, "https://media.valorant-api.com/weapons/f7e1b454-4ad4-1063-ec0a-159e56b58941/displayicon.png"),
    ("Vandal", "Rifles", 2900, "https://media.valorant-api.com/weapons/9c82e19d-4575-0200-1a81-3eacf00cf872/displayicon.png"),
]

schema = StructType([
    StructField("weapon_name", StringType(), False),
    StructField("weapon_category", StringType(), True),
    StructField("weapon_cost", IntegerType(), True),
    StructField("weapon_icon_url", StringType(), True)
])

staged_weapon_df = spark.createDataFrame(weapon_seed_data, schema=schema).withColumn(
    "updated_at", F.current_timestamp()
)

staged_weapon_df.createOrReplaceTempView("staged_dim_weapon")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `dim_weapon`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_dim_weapon AS source
ON target.weapon_name = source.weapon_name
WHEN MATCHED THEN
  UPDATE SET
    target.weapon_category = source.weapon_category,
    target.weapon_cost = source.weapon_cost,
    target.weapon_icon_url = source.weapon_icon_url,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    weapon_name,
    weapon_category,
    weapon_cost,
    weapon_icon_url,
    updated_at
  )
  VALUES (
    source.weapon_name,
    source.weapon_category,
    source.weapon_cost,
    source.weapon_icon_url,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
spark.sql(merge_sql).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification & Category Breakdown

# COMMAND ----------
total_weapons = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"=== dim_weapon Summary ===")
print(f"Total weapons registered: {total_weapons}")

display(spark.sql(f"""
    SELECT 
        weapon_category, 
        COUNT(*) AS weapon_count, 
        CONCAT_WS(', ', COLLECT_LIST(weapon_name)) AS weapons
    FROM {TARGET_TABLE}
    GROUP BY weapon_category
    ORDER BY weapon_count DESC
"""))
