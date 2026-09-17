# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Dimension: `dim_agent`
# MAGIC 
# MAGIC - **Catalog:** `valorant`
# MAGIC - **Schema:** `silver`
# MAGIC - **Target Table:** `valorant.silver.dim_agent`
# MAGIC - **Grain:** 1 row per unique agent
# MAGIC - **Primary Key:** `agent_name`
# MAGIC - **Purpose:** Master agent catalog with tactical roles (`Duelist`, `Initiator`, `Controller`, `Sentinel`)
# MAGIC   and official Riot CDN image URLs for native rendering in Databricks AI/BI Dashboards and reports.
# MAGIC - **Write Strategy:** Delta `MERGE` (Upsert on `agent_name`)

# COMMAND ----------
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType

# Configuration
CATALOG = "valorant"
SILVER_SCHEMA = "silver"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_agent"

print(f"Target Table: {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Silver Schema and `dim_agent` Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    agent_name STRING NOT NULL,
    agent_role STRING,
    agent_icon_url STRING,
    agent_portrait_url STRING,
    is_active BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Master agent dimension with tactical roles, active status, and CDN image URLs'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Seed Official Valorant Agents Dataset

# COMMAND ----------
agent_seed_data = [
    ("Astra", "Controller", "https://media.valorant-api.com/agents/41fb69c1-4189-7b37-f117-bcaf1e96f1bf/displayicon.png", "https://media.valorant-api.com/agents/41fb69c1-4189-7b37-f117-bcaf1e96f1bf/fullportrait.png", True),
    ("Breach", "Initiator", "https://media.valorant-api.com/agents/5f8d3a7f-467b-97f3-062c-13acf203c006/displayicon.png", "https://media.valorant-api.com/agents/5f8d3a7f-467b-97f3-062c-13acf203c006/fullportrait.png", True),
    ("Brimstone", "Controller", "https://media.valorant-api.com/agents/9f0d8ba9-4140-b941-57d3-a7ad57c6b417/displayicon.png", "https://media.valorant-api.com/agents/9f0d8ba9-4140-b941-57d3-a7ad57c6b417/fullportrait.png", True),
    ("Chamber", "Sentinel", "https://media.valorant-api.com/agents/22697a3d-45bf-8dd7-4fec-84a9e28c69d7/displayicon.png", "https://media.valorant-api.com/agents/22697a3d-45bf-8dd7-4fec-84a9e28c69d7/fullportrait.png", True),
    ("Clove", "Controller", "https://media.valorant-api.com/agents/1dbf2edd-4729-0984-3115-daa5eed44993/displayicon.png", "https://media.valorant-api.com/agents/1dbf2edd-4729-0984-3115-daa5eed44993/fullportrait.png", True),
    ("Cypher", "Sentinel", "https://media.valorant-api.com/agents/117ed9e3-49f3-6512-3ccf-0cada7e3823b/displayicon.png", "https://media.valorant-api.com/agents/117ed9e3-49f3-6512-3ccf-0cada7e3823b/fullportrait.png", True),
    ("Deadlock", "Sentinel", "https://media.valorant-api.com/agents/cc8b64c8-4b25-4ff9-6e7f-37b4da43d235/displayicon.png", "https://media.valorant-api.com/agents/cc8b64c8-4b25-4ff9-6e7f-37b4da43d235/fullportrait.png", True),
    ("Fade", "Initiator", "https://media.valorant-api.com/agents/dade69b4-4f5a-8528-247b-219e5a1facd6/displayicon.png", "https://media.valorant-api.com/agents/dade69b4-4f5a-8528-247b-219e5a1facd6/fullportrait.png", True),
    ("Gekko", "Initiator", "https://media.valorant-api.com/agents/e370fa57-4757-3604-3648-499e1f642d3f/displayicon.png", "https://media.valorant-api.com/agents/e370fa57-4757-3604-3648-499e1f642d3f/fullportrait.png", True),
    ("Harbor", "Controller", "https://media.valorant-api.com/agents/95b78ed7-4637-86d9-7e41-71ba8c293152/displayicon.png", "https://media.valorant-api.com/agents/95b78ed7-4637-86d9-7e41-71ba8c293152/fullportrait.png", True),
    ("Iso", "Duelist", "https://media.valorant-api.com/agents/0e38b510-41a8-5780-5e8f-568b2a4f2d6c/displayicon.png", "https://media.valorant-api.com/agents/0e38b510-41a8-5780-5e8f-568b2a4f2d6c/fullportrait.png", True),
    ("Jett", "Duelist", "https://media.valorant-api.com/agents/add6443a-41bd-e414-f6ad-e58d267f4e95/displayicon.png", "https://media.valorant-api.com/agents/add6443a-41bd-e414-f6ad-e58d267f4e95/fullportrait.png", True),
    ("KAY/O", "Initiator", "https://media.valorant-api.com/agents/601dbbe7-43ce-be57-2a40-4abd24953621/displayicon.png", "https://media.valorant-api.com/agents/601dbbe7-43ce-be57-2a40-4abd24953621/fullportrait.png", True),
    ("Killjoy", "Sentinel", "https://media.valorant-api.com/agents/1e58de9c-4950-5125-93e9-a0aee9f98746/displayicon.png", "https://media.valorant-api.com/agents/1e58de9c-4950-5125-93e9-a0aee9f98746/fullportrait.png", True),
    ("Miks", "Controller", "https://media.valorant-api.com/agents/7c8a4701-4de6-9355-b254-e09bc2a34b72/displayicon.png", "https://media.valorant-api.com/agents/7c8a4701-4de6-9355-b254-e09bc2a34b72/fullportrait.png", True),
    ("Neon", "Duelist", "https://media.valorant-api.com/agents/bb2a4828-46eb-8cd1-e765-15848195d751/displayicon.png", "https://media.valorant-api.com/agents/bb2a4828-46eb-8cd1-e765-15848195d751/fullportrait.png", True),
    ("Omen", "Controller", "https://media.valorant-api.com/agents/8e253930-4c05-31dd-1b6c-968525494517/displayicon.png", "https://media.valorant-api.com/agents/8e253930-4c05-31dd-1b6c-968525494517/fullportrait.png", True),
    ("Phoenix", "Duelist", "https://media.valorant-api.com/agents/eb93336a-449b-9c1b-0a54-a891f7921d69/displayicon.png", "https://media.valorant-api.com/agents/eb93336a-449b-9c1b-0a54-a891f7921d69/fullportrait.png", True),
    ("Raze", "Duelist", "https://media.valorant-api.com/agents/f94c3b30-42be-e959-889c-5aa313dba261/displayicon.png", "https://media.valorant-api.com/agents/f94c3b30-42be-e959-889c-5aa313dba261/fullportrait.png", True),
    ("Reyna", "Duelist", "https://media.valorant-api.com/agents/a3bfb853-43b2-7238-a4f1-ad90e9e46bcc/displayicon.png", "https://media.valorant-api.com/agents/a3bfb853-43b2-7238-a4f1-ad90e9e46bcc/fullportrait.png", True),
    ("Sage", "Sentinel", "https://media.valorant-api.com/agents/569fdd95-4d10-43ab-ca70-79becc718b46/displayicon.png", "https://media.valorant-api.com/agents/569fdd95-4d10-43ab-ca70-79becc718b46/fullportrait.png", True),
    ("Skye", "Initiator", "https://media.valorant-api.com/agents/6f2a04ca-43e0-be17-7f36-b3908627744d/displayicon.png", "https://media.valorant-api.com/agents/6f2a04ca-43e0-be17-7f36-b3908627744d/fullportrait.png", True),
    ("Sova", "Initiator", "https://media.valorant-api.com/agents/320b2a48-4d9b-a075-30f1-1f93a9b638fa/displayicon.png", "https://media.valorant-api.com/agents/320b2a48-4d9b-a075-30f1-1f93a9b638fa/fullportrait.png", True),
    ("Tejo", "Initiator", "https://media.valorant-api.com/agents/b444168c-4e35-8076-db47-ef9bf368f384/displayicon.png", "https://media.valorant-api.com/agents/b444168c-4e35-8076-db47-ef9bf368f384/fullportrait.png", True),
    ("Veto", "Sentinel", "https://media.valorant-api.com/agents/92eeef5d-43b5-1d4a-8d03-b3927a09034b/displayicon.png", "https://media.valorant-api.com/agents/92eeef5d-43b5-1d4a-8d03-b3927a09034b/fullportrait.png", True),
    ("Viper", "Controller", "https://media.valorant-api.com/agents/707eab51-4836-f488-046a-cda6bf494859/displayicon.png", "https://media.valorant-api.com/agents/707eab51-4836-f488-046a-cda6bf494859/fullportrait.png", True),
    ("Vyse", "Sentinel", "https://media.valorant-api.com/agents/efba5359-4016-a1e5-7626-b1ae76895940/displayicon.png", "https://media.valorant-api.com/agents/efba5359-4016-a1e5-7626-b1ae76895940/fullportrait.png", True),
    ("Waylay", "Duelist", "https://media.valorant-api.com/agents/df1cb487-4902-002e-5c17-d28e83e78588/displayicon.png", "https://media.valorant-api.com/agents/df1cb487-4902-002e-5c17-d28e83e78588/fullportrait.png", True),
    ("Yoru", "Duelist", "https://media.valorant-api.com/agents/7f94d92c-4234-0a36-9646-3a87eb8b5c89/displayicon.png", "https://media.valorant-api.com/agents/7f94d92c-4234-0a36-9646-3a87eb8b5c89/fullportrait.png", True),
]

schema = StructType([
    StructField("agent_name", StringType(), False),
    StructField("agent_role", StringType(), True),
    StructField("agent_icon_url", StringType(), True),
    StructField("agent_portrait_url", StringType(), True),
    StructField("is_active", BooleanType(), False)
])

staged_agent_df = spark.createDataFrame(agent_seed_data, schema=schema).withColumn(
    "updated_at", F.current_timestamp()
)

staged_agent_df.createOrReplaceTempView("staged_dim_agent")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Upsert (MERGE) into `dim_agent`

# COMMAND ----------
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING staged_dim_agent AS source
ON target.agent_name = source.agent_name
WHEN MATCHED THEN
  UPDATE SET
    target.agent_role = source.agent_role,
    target.agent_icon_url = source.agent_icon_url,
    target.agent_portrait_url = source.agent_portrait_url,
    target.is_active = source.is_active,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    agent_name,
    agent_role,
    agent_icon_url,
    agent_portrait_url,
    is_active,
    updated_at
  )
  VALUES (
    source.agent_name,
    source.agent_role,
    source.agent_icon_url,
    source.agent_portrait_url,
    source.is_active,
    source.updated_at
  )
"""

print(f"Executing MERGE into {TARGET_TABLE}...")
spark.sql(merge_sql).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Verification & Role Breakdown

# COMMAND ----------
total_agents = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"=== dim_agent Summary ===")
print(f"Total unique agents registered: {total_agents}")

display(spark.sql(f"""
    SELECT 
        agent_role, 
        COUNT(*) AS agent_count, 
        CONCAT_WS(', ', COLLECT_LIST(agent_name)) AS agents
    FROM {TARGET_TABLE}
    GROUP BY agent_role
    ORDER BY agent_count DESC
"""))
