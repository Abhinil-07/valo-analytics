# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Release 1: Valorant Landing Ingestion
# MAGIC 
# MAGIC Ingests raw matches directly from HenrikDev API into Databricks Unity Catalog Volumes:
# MAGIC - **Volume Path:** `/Volumes/<catalog>/<schema>/<volume>/matches/YYYY/MM/DD/<match_id>.json`
# MAGIC - **Delta Control Table:** `<catalog>.<schema>.ingestion_control`
# MAGIC - **Idempotency:** Skips matches that already have `SUCCESS` in `ingestion_control`

# COMMAND ----------
import os
import json
from datetime import datetime, timezone
import dateutil.parser
import requests

# ==============================================================================
# CONFIGURATION (Hardcoded for immediate execution)
# ==============================================================================
API_KEY = "HDEV-c897243f-3b2e-4962-b1f4-4f9f9e272b44"
PUUID = "59bae8f3-025c-5dcc-9a1c-c903279e4145"
REGION = "ap"

# Unity Catalog targets
CATALOG = "valorant"
SCHEMA = "landing"
VOLUME = "data"

# Query parameters
MODE = "competitive"
SIZE = 5

# Derived paths
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_control"
VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"Target Volume: {VOLUME_BASE_PATH}")
print(f"Control Table: {CONTROL_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Schema and Delta Control Table Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
    match_id STRING NOT NULL,
    match_date DATE,
    landing_path STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_status STRING,
    source STRING,
    error_message STRING
)
USING DELTA
COMMENT 'Tracks match ingestion state and ensures idempotency'
""")

print(f"Control table verified: {CONTROL_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Fetch Matches from HenrikDev API

# COMMAND ----------
url = f"https://api.henrikdev.xyz/valorant/v3/by-puuid/matches/{REGION}/{PUUID}"
headers = {
    "Authorization": API_KEY,
    "Accept": "application/json"
}
params = {
    "mode": MODE,
    "size": SIZE
}

print(f"Fetching matches from: {url}")
response = requests.get(url, params=params, headers=headers, timeout=30)

if response.status_code != 200:
    raise RuntimeError(f"HenrikDev API error {response.status_code}: {response.text}")

payload = response.json()
matches_list = payload.get("data", [])
print(f"Retrieved {len(matches_list)} matches from API.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Land New Matches into Volume & Record Ingestion Status

# COMMAND ----------
# Get match IDs already recorded as SUCCESS in control table
existing_df = spark.sql(f"SELECT match_id FROM {CONTROL_TABLE} WHERE ingestion_status = 'SUCCESS'")
successful_ids = {row["match_id"] for row in existing_df.collect()}
print(f"Found {len(successful_ids)} existing successful matches in control table.")

source_label = f"api:{REGION}/puuid:{PUUID}"
landed_count = 0
skipped_count = 0
failed_count = 0

for match in matches_list:
    metadata = match.get("metadata", {})
    match_id = metadata.get("matchid")
    
    if not match_id:
        print("Skipping record without matchid")
        continue

    # Derive UTC match date from game_start or game_start_patched
    game_start = metadata.get("game_start")
    game_start_patched = metadata.get("game_start_patched")
    
    match_date = None
    if game_start is not None:
        try:
            ts = float(game_start)
            if ts > 1e11:
                ts = ts / 1000.0
            match_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        except Exception:
            pass
            
    if match_date is None and game_start_patched:
        try:
            match_date = dateutil.parser.parse(game_start_patched).date()
        except Exception:
            pass

    if match_date is None:
        match_date = datetime.now(timezone.utc).date()

    # Idempotency: Skip if already landed successfully
    if match_id in successful_ids:
        print(f"Skipping match {match_id} (already ingested)")
        skipped_count += 1
        continue

    # Destination: /Volumes/<catalog>/<schema>/<volume>/matches/YYYY/MM/DD/<match_id>.json
    year_str = f"{match_date.year:04d}"
    month_str = f"{match_date.month:02d}"
    day_str = f"{match_date.day:02d}"
    target_dir = f"{VOLUME_BASE_PATH}/matches/{year_str}/{month_str}/{day_str}"
    target_file = f"{target_dir}/{match_id}.json"

    try:
        # Write JSON to Databricks Volume
        os.makedirs(target_dir, exist_ok=True)
        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(match, f, ensure_ascii=False, indent=2)

        # Record SUCCESS in Delta control table
        date_literal = f"DATE('{match_date.isoformat()}')"
        spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS target
        USING (SELECT '{match_id}' AS match_id) AS source
        ON target.match_id = source.match_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.match_date = {date_literal},
                target.landing_path = '{target_file}',
                target.ingestion_timestamp = current_timestamp(),
                target.ingestion_status = 'SUCCESS',
                target.source = '{source_label}',
                target.error_message = NULL
        WHEN NOT MATCHED THEN
            INSERT (match_id, match_date, landing_path, ingestion_timestamp, ingestion_status, source, error_message)
            VALUES ('{match_id}', {date_literal}, '{target_file}', current_timestamp(), 'SUCCESS', '{source_label}', NULL)
        """)

        print(f"Landed: {match_id} -> {target_file}")
        landed_count += 1
        successful_ids.add(match_id)

    except Exception as e:
        err_msg = str(e).replace("'", "''")
        print(f"Failed to land match {match_id}: {e}")
        failed_count += 1

        date_literal = f"DATE('{match_date.isoformat()}')" if match_date else "NULL"
        spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS target
        USING (SELECT '{match_id}' AS match_id) AS source
        ON target.match_id = source.match_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.match_date = {date_literal},
                target.landing_path = NULL,
                target.ingestion_timestamp = current_timestamp(),
                target.ingestion_status = 'FAILED',
                target.source = '{source_label}',
                target.error_message = '{err_msg}'
        WHEN NOT MATCHED THEN
            INSERT (match_id, match_date, landing_path, ingestion_timestamp, ingestion_status, source, error_message)
            VALUES ('{match_id}', {date_literal}, NULL, current_timestamp(), 'FAILED', '{source_label}', '{err_msg}')
        """)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Run Summary & Control Table Audit

# COMMAND ----------
print("=" * 60)
print("LANDING INGESTION SUMMARY")
print("=" * 60)
print(f"Total API Matches:   {len(matches_list)}")
print(f"Newly Landed:        {landed_count}")
print(f"Skipped (Duplicate): {skipped_count}")
print(f"Failed:              {failed_count}")
print("=" * 60)

display(spark.sql(f"SELECT * FROM {CONTROL_TABLE} ORDER BY ingestion_timestamp DESC LIMIT 10"))
