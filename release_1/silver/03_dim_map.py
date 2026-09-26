# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Dimension: `dim_map`
# MAGIC 
# MAGIC - **Catalog:** `valorant`
# MAGIC - **Schema:** `silver`
# MAGIC - **Target Table:** `valorant.silver.dim_map`
# MAGIC - **Grain:** 1 row per unique map
# MAGIC - **Primary Key:** `map_name`
# MAGIC - **Purpose:** Master map catalog detailing bomb sites (`A`, `B`, `C`), competitive rotation status,
# MAGIC   and official Riot CDN splash/radar URLs for native rendering in Databricks AI/BI Dashboards (SRS Section 6 & 15).
# MAGIC - **Write Strategy:** Delta `MERGE` (Upsert on `map_name`)

# COMMAND ----------
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, TimestampType

# Configuration
CATALOG = "valorant"
SILVER_SCHEMA = "silver"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_map"

print(f"Target Table: {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Silver Schema and `dim_map` Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    map_name STRING NOT NULL,
    site_count INT NOT NULL,
    sites ARRAY<STRING> NOT NULL,
    map_splash_url STRING,
    map_radar_url STRING,
    is_active_pool BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Master map dimension with bomb sites, pool status, and CDN image URLs'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Seed Official Valorant Maps Dataset

# COMMAND ----------
map_seed_data = [
    ("Abyss", 2, ['A', 'B'], "https://media.valorant-api.com/maps/224b0a95-48b9-f703-1bd8-67aca101a61f/splash.png", "https://media.valorant-api.com/maps/224b0a95-48b9-f703-1bd8-67aca101a61f/displayicon.png", True),
    ("Ascent", 2, ['A', 'B'], "https://media.valorant-api.com/maps/7eaecc1b-4337-bbf6-6ab9-04b8f06b3319/splash.png", "https://media.valorant-api.com/maps/7eaecc1b-4337-bbf6-6ab9-04b8f06b3319/displayicon.png", True),
    ("Bind", 2, ['A', 'B'], "https://media.valorant-api.com/maps/2c9d57ec-4431-9c5e-2939-8f9ef6dd5cba/splash.png", "https://media.valorant-api.com/maps/2c9d57ec-4431-9c5e-2939-8f9ef6dd5cba/displayicon.png", True),
    ("Breeze", 2, ['A', 'B'], "https://media.valorant-api.com/maps/2fb9a4fd-47b8-4e7d-a969-74b4046ebd53/splash.png", "https://media.valorant-api.com/maps/2fb9a4fd-47b8-4e7d-a969-74b4046ebd53/displayicon.png", True),
    ("Corrode", 2, ['A', 'B'], "https://media.valorant-api.com/maps/1c18ab1f-420d-0d8b-71d0-77ad3c439115/splash.png", "https://media.valorant-api.com/maps/1c18ab1f-420d-0d8b-71d0-77ad3c439115/displayicon.png", True),
    ("Fracture", 2, ['A', 'B'], "https://media.valorant-api.com/maps/b529448b-4d60-346e-e89e-00a4c527a405/splash.png", "https://media.valorant-api.com/maps/b529448b-4d60-346e-e89e-00a4c527a405/displayicon.png", True),
    ("Haven", 3, ['A', 'B', 'C'], "https://media.valorant-api.com/maps/2bee0dc9-4ffe-519b-1cbd-7fbe763a6047/splash.png", "https://media.valorant-api.com/maps/2bee0dc9-4ffe-519b-1cbd-7fbe763a6047/displayicon.png", True),
    ("Icebox", 2, ['A', 'B'], "https://media.valorant-api.com/maps/e2ad5c54-4114-a870-9641-8ea21279579a/splash.png", "https://media.valorant-api.com/maps/e2ad5c54-4114-a870-9641-8ea21279579a/displayicon.png", True),
    ("Lotus", 3, ['A', 'B', 'C'], "https://media.valorant-api.com/maps/2fe4ed3a-450a-948b-6d6b-e89a78e680a9/splash.png", "https://media.valorant-api.com/maps/2fe4ed3a-450a-948b-6d6b-e89a78e680a9/displayicon.png", True),
    ("Pearl", 2, ['A', 'B'], "https://media.valorant-api.com/maps/fd267378-4d1d-484f-ff52-77821ed10dc2/splash.png", "https://media.valorant-api.com/maps/fd267378-4d1d-484f-ff52-77821ed10dc2/displayicon.png", True),
    ("Split", 2, ['A', 'B'], "https://media.valorant-api.com/maps/d960549e-485c-e861-8d71-aa9d1aed12a2/splash.png", "https://media.valorant-api.com/maps/d960549e-485c-e861-8d71-aa9d1aed12a2/displayicon.png", True),
    ("Summit", 2, ['A', 'B'], "https://media.valorant-api.com/maps/756da597-416b-c0f2-f47b-afbdf28670bc/splash.png", "https://media.valorant-api.com/maps/756da597-416b-c0f2-f47b-afbdf28670bc/displayicon.png", True),
    ("Sunset", 2, ['A', 'B'], "https://media.valorant-api.com/maps/92584fbe-486a-b1b2-9faa-39b0f486b498/splash.png", "https://media.valorant-api.com/maps/92584fbe-486a-b1b2-9faa-39b0f486b498/displayicon.png", True),
]

schema = StructType([
    StructField("map_name", StringType(), False),
    StructField("site_count", IntegerType(), False),
    StructField("sites", ArrayType(StringType()), False),
    StructField("map_splash_url", StringType(), True),
    StructField("map_radar_url", StringType(), True),
    StructField("is_active_pool", BooleanType(), False)
])

staged_map_df = spark.createDataFrame(map_seed_data, schema=schema).withColumn(
    "updated_at", F.current_timestamp()
)

staged_map_df.createOrReplaceTempView("staged_dim_map")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `dim_map`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_dim_map AS source
ON target.map_name = source.map_name
WHEN MATCHED THEN
  UPDATE SET
    target.site_count = source.site_count,
    target.sites = source.sites,
    target.map_splash_url = source.map_splash_url,
    target.map_radar_url = source.map_radar_url,
    target.is_active_pool = source.is_active_pool,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    map_name,
    site_count,
    sites,
    map_splash_url,
    map_radar_url,
    is_active_pool,
    updated_at
  )
  VALUES (
    source.map_name,
    source.site_count,
    source.sites,
    source.map_splash_url,
    source.map_radar_url,
    source.is_active_pool,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
spark.sql(merge_sql).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification & Map Overview

# COMMAND ----------
total_maps = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"=== dim_map Summary ===")
print(f"Total maps registered: {total_maps}")

display(spark.sql(f"""
    SELECT 
        map_name, 
        site_count, 
        CONCAT_WS(', ', sites) AS available_sites,
        is_active_pool
    FROM {TARGET_TABLE}
    ORDER BY map_name ASC
"""))
