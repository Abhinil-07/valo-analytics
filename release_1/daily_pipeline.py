# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Daily Automated Pipeline: API -> Raw -> Landing -> Bronze
# MAGIC 
# MAGIC Orchestrates the daily end-to-end ingestion pipeline:
# MAGIC 1. **Step 1:** Ingest live match payloads from HenrikDev API into `/Volumes/valorant/raw/data/api/`
# MAGIC 2. **Step 2:** Normalize and land canonical match JSONs into `/Volumes/valorant/landing/data/matches/` with Delta control tracking
# MAGIC 3. **Step 3:** Incrementally transform and merge into all 9 Bronze Delta tables

# COMMAND ----------
from datetime import datetime, timezone

print("=" * 70)
print(f"VALORANT DAILY INGESTION PIPELINE STARTED: {datetime.now(timezone.utc).isoformat()}")
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Call HenrikDev API & Archive to Raw Volume

# COMMAND ----------
print("\n[STEP 1/3] Running Raw API Loader...")
# MAGIC %run ./02_raw_api_loader

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Normalize Unaltered Raw & Land Canonical JSONs

# COMMAND ----------
print("\n[STEP 2/3] Running Normalization & Canonical Landing...")
# MAGIC %run ./03_normalize_and_land

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Incrementally Merge into All 9 Bronze Delta Tables

# COMMAND ----------
print("\n[STEP 3/3] Running Incremental Bronze Ingestion (9 Tables)...")
# MAGIC %run ./bronze_ingestion

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Daily Pipeline Run Audit & Summary

# COMMAND ----------
CATALOG = "valorant"
BRONZE = f"{CATALOG}.bronze"
LANDING = f"{CATALOG}.landing"

total_landed = spark.sql(f"SELECT COUNT(*) FROM {LANDING}.ingestion_control WHERE ingestion_status = 'SUCCESS'").collect()[0][0]
total_matches = spark.sql(f"SELECT COUNT(*) FROM {BRONZE}.bronze_match").collect()[0][0]
total_players = spark.sql(f"SELECT COUNT(*) FROM {BRONZE}.bronze_player").collect()[0][0]
total_rounds = spark.sql(f"SELECT COUNT(*) FROM {BRONZE}.bronze_round").collect()[0][0]
total_kills = spark.sql(f"SELECT COUNT(*) FROM {BRONZE}.bronze_kill_event").collect()[0][0]

print("\n" + "=" * 70)
print(f"PIPELINE COMPLETED SUCCESSFULLY: {datetime.now(timezone.utc).isoformat()}")
print("=" * 70)
print(f"Total Successful Matches in Landing: {total_landed}")
print(f"Total Matches in Bronze (Match):     {total_matches}")
print(f"Total Players in Bronze (Player):    {total_players}")
print(f"Total Rounds in Bronze (Round):      {total_rounds}")
print(f"Total Kills in Bronze (Kill):        {total_kills}")
print("=" * 70)

# Display latest 5 landed matches in control table
display(spark.sql(f"SELECT match_id, match_date, source, load_type, ingestion_timestamp FROM {LANDING}.ingestion_control ORDER BY ingestion_timestamp DESC LIMIT 5"))
