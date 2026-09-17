# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table: `bronze_damage_event`
# MAGIC 
# MAGIC - **Grain:** 1 row per damage event
# MAGIC - **Composite Key:** `match_id` + `round_number` + `attacker_puuid` + `damage_event_sequence`
# MAGIC - **Source:** `rounds[].player_stats[].damage_events[]`
# MAGIC - **Target:** `<catalog>.<schema>.bronze_damage_event`
# MAGIC - **Round Index Rule:** `round_number = rounds[] array position + 1`
# MAGIC - **Sequence Rule:** `damage_event_sequence = damage_events[] 0-based array position`

# COMMAND ----------
import os
import glob
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

# Configuration
CATALOG = "valorant"
SCHEMA = "bronze"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/data/matches"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_damage_event"

print(f"Target Table: {TARGET_TABLE}")
print(f"Reading from: {LANDING_VOLUME_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: DDL — Ensure Table Exists

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    match_id STRING NOT NULL,
    round_number INT NOT NULL,
    damage_event_sequence INT NOT NULL,
    attacker_puuid STRING NOT NULL,
    attacker_display_name STRING,
    attacker_team STRING,
    receiver_puuid STRING,
    receiver_display_name STRING,
    receiver_team STRING,
    damage INT,
    bodyshots INT,
    headshots INT,
    legshots INT
)
USING DELTA
COMMENT 'Source-aligned granular damage events from rounds[].player_stats[].damage_events[]'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Identify Pending Matches from Ingestion Control (Incremental)

# COMMAND ----------
CONTROL_TABLE = f"{CATALOG}.landing.ingestion_control"

# Query matches already processed in this Bronze table
try:
    existing_bronze_df = spark.sql(f"SELECT DISTINCT match_id FROM {TARGET_TABLE}")
    already_processed_ids = {row["match_id"] for row in existing_bronze_df.collect()}
except Exception:
    already_processed_ids = set()

# Query successful landing files from ingestion_control
control_df = spark.sql(f"""
    SELECT match_id, landing_path 
    FROM {CONTROL_TABLE} 
    WHERE ingestion_status = 'SUCCESS'
""")
landing_records = control_df.collect()

# Filter to pending files only
pending_matches = [
    (row["match_id"], row["landing_path"])
    for row in landing_records
    if row["match_id"] not in already_processed_ids
]

print(f"Total Landing Matches: {len(landing_records)}")
print(f"Already in Bronze:     {len(already_processed_ids)}")
print(f"Pending to Ingest:     {len(pending_matches)}")

if not pending_matches:
    print(f"No pending matches for {TARGET_TABLE}. Bronze is up to date!")
    dbutils.notebook.exit("Success: 0 new matches")

matches_data = []
for mid, file_path in pending_matches:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)
            if isinstance(content, dict):
                if "data" in content and isinstance(content["data"], list):
                    matches_data.extend(content["data"])
                elif "metadata" in content:
                    matches_data.append(content)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

# Deduplicate by matchid
unique_matches = {}
for m in matches_data:
    mid = (m.get("metadata") or {}).get("matchid")
    if mid and mid not in unique_matches:
        unique_matches[mid] = m

print(f"Loaded {len(unique_matches)} pending match object(s) to process.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract `bronze_damage_event` Entities

# COMMAND ----------
dmg_rows = []

for m in unique_matches.values():
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    rounds_list = m.get("rounds") or []
    for r_idx, r in enumerate(rounds_list):
        round_number = r_idx + 1
        pstats = r.get("player_stats") or []

        for ps in pstats:
            attacker_puuid = ps.get("player_puuid")
            if not attacker_puuid:
                continue

            attacker_name = ps.get("player_display_name")
            attacker_team = ps.get("player_team")
            dmg_events = ps.get("damage_events") or []

            for d_seq, de in enumerate(dmg_events):
                dmg_rows.append({
                    "match_id": match_id,
                    "round_number": round_number,
                    "damage_event_sequence": d_seq,
                    "attacker_puuid": attacker_puuid,
                    "attacker_display_name": attacker_name,
                    "attacker_team": attacker_team,
                    "receiver_puuid": de.get("receiver_puuid"),
                    "receiver_display_name": de.get("receiver_display_name"),
                    "receiver_team": de.get("receiver_team"),
                    "damage": de.get("damage"),
                    "bodyshots": de.get("bodyshots"),
                    "headshots": de.get("headshots"),
                    "legshots": de.get("legshots")
                })

print(f"Extracted {len(dmg_rows)} rows for bronze_damage_event.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Create DataFrame & Delta MERGE (Idempotent Upsert)

# COMMAND ----------
schema_damage = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("damage_event_sequence", IntegerType(), False),
    StructField("attacker_puuid", StringType(), False),
    StructField("attacker_display_name", StringType(), True),
    StructField("attacker_team", StringType(), True),
    StructField("receiver_puuid", StringType(), True),
    StructField("receiver_display_name", StringType(), True),
    StructField("receiver_team", StringType(), True),
    StructField("damage", IntegerType(), True),
    StructField("bodyshots", IntegerType(), True),
    StructField("headshots", IntegerType(), True),
    StructField("legshots", IntegerType(), True)
])

df_dmg = spark.createDataFrame(dmg_rows, schema=schema_damage)
df_dmg.createOrReplaceTempView("tv_bronze_damage_event")

spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS target
USING tv_bronze_damage_event AS source
ON target.match_id = source.match_id
AND target.round_number = source.round_number
AND target.attacker_puuid = source.attacker_puuid
AND target.damage_event_sequence = source.damage_event_sequence
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")
print(f"Successfully merged into {TARGET_TABLE}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Data Quality & Reconciliation Assertions

# COMMAND ----------
stats = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT match_id, round_number, attacker_puuid, damage_event_sequence) AS distinct_keys,
    COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS null_matches,
    COUNT(CASE WHEN round_number < 1 OR round_number IS NULL THEN 1 END) AS invalid_rounds,
    COUNT(CASE WHEN attacker_puuid IS NULL THEN 1 END) AS null_attackers,
    COUNT(CASE WHEN damage_event_sequence IS NULL THEN 1 END) AS null_sequences
FROM {TARGET_TABLE}
""").collect()[0]

print(f"Total Rows:      {stats['total_rows']}")
print(f"Distinct Keys:   {stats['distinct_keys']}")
print(f"Null Matches:    {stats['null_matches']}")
print(f"Invalid Rounds:  {stats['invalid_rounds']}")
print(f"Null Attackers:  {stats['null_attackers']}")
print(f"Null Sequences:  {stats['null_sequences']}")

assert stats["null_matches"] == 0, "DQ ERROR: null match_id found!"
assert stats["invalid_rounds"] == 0, "DQ ERROR: invalid round_number found!"
assert stats["null_attackers"] == 0, "DQ ERROR: null attacker_puuid found!"
assert stats["null_sequences"] == 0, "DQ ERROR: null damage_event_sequence found!"
assert stats["total_rows"] == stats["distinct_keys"], "DQ ERROR: duplicate composite key found!"

# Source count reconciliation
expected_dmg_count = sum(
    len(ps.get("damage_events", []))
    for m in unique_matches.values()
    for r in m.get("rounds", [])
    for ps in r.get("player_stats", [])
)
assert stats["total_rows"] == expected_dmg_count, f"Source reconciliation failed! Expected {expected_dmg_count}, found {stats['total_rows']}"
print(f"Reconciliation SUCCESS: Exact match with source damage_events count ({expected_dmg_count} rows).")

# Damage reconciliation: SUM(damage_event.damage) vs player_stats.damage
spark.sql(f"""
SELECT 
    d.match_id,
    d.round_number,
    d.attacker_puuid,
    SUM(d.damage) AS sum_event_damage,
    p.damage AS round_stat_damage
FROM {TARGET_TABLE} d
JOIN {CATALOG}.{SCHEMA}.bronze_round_player_stats p
  ON d.match_id = p.match_id
  AND d.round_number = p.round_number
  AND d.attacker_puuid = p.player_puuid
GROUP BY d.match_id, d.round_number, d.attacker_puuid, p.damage
HAVING SUM(d.damage) != p.damage
""").createOrReplaceTempView("tv_damage_diffs")

diff_count = spark.sql("SELECT COUNT(*) FROM tv_damage_diffs").collect()[0][0]
print(f"Damage Event vs Player Stats Reconciliation: {diff_count} discrepancies.")

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 15"))
