# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 2: HenrikDev Live API Raw Loader
# MAGIC 
# MAGIC Fetches match payloads from HenrikDev Valorant API and archives the **complete, unaltered API response** into Databricks Raw volume:
# MAGIC - **Destination:** `/Volumes/<catalog>/raw/<volume>/api/YYYY/MM/DD/api_response_<timestamp>_<hash>.json`
# MAGIC - Preserves complete multi-match response (`{"status": 200, "data": [...]}`) without splitting or modifying before archival.

# COMMAND ----------
import os
import json
import hashlib
from datetime import datetime, timezone
import requests

# ==============================================================================
# CONFIGURATION (Hardcoded for simple execution)
# ==============================================================================
API_KEY = "HDEV-c897243f-3b2e-4962-b1f4-4f9f9e272b44"
PUUID = "59bae8f3-025c-5dcc-9a1c-c903279e4145"
REGION = "ap"
MODE = "competitive"
SIZE = 5

CATALOG = "valorant"
RAW_SCHEMA = "raw"
VOLUME = "data"

RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"
API_TARGET_DIR = f"{RAW_VOLUME_PATH}/api"

print(f"Target Raw API Root: {API_TARGET_DIR}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Raw Schema & Volume Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Fetch Live Matches from HenrikDev API

# COMMAND ----------
url = f"https://api.henrikdev.xyz/valorant/v3/by-puuid/matches/{REGION}/{PUUID}"
headers = {
    "Authorization": API_KEY.strip(),
    "Accept": "application/json"
}
params = {
    "mode": MODE,
    "size": SIZE
}

print(f"Calling endpoint: {url}")
resp = requests.get(url, params=params, headers=headers, timeout=30)
if resp.status_code != 200:
    raise RuntimeError(f"HenrikDev API call failed with HTTP {resp.status_code}: {resp.text}")

raw_json_data = resp.json()
match_count = len(raw_json_data.get("data", [])) if isinstance(raw_json_data.get("data"), list) else 0
print(f"Retrieved response containing {match_count} match(es).")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Archive Unaltered API Response to Raw Volume

# COMMAND ----------
now_utc = datetime.now(timezone.utc)
date_folder = now_utc.strftime("%Y/%m/%d")
ts_str = now_utc.strftime("%Y%m%dT%H%M%SZ")

req_signature = f"{REGION}_{PUUID}_{MODE}_{SIZE}"
req_hash = hashlib.sha256(req_signature.encode("utf-8")).hexdigest()[:8]

target_folder = f"{API_TARGET_DIR}/{date_folder}"
target_filename = f"api_response_{ts_str}_{req_hash}.json"
target_filepath = f"{target_folder}/{target_filename}"

os.makedirs(target_folder, exist_ok=True)

raw_bytes = json.dumps(raw_json_data, ensure_ascii=False, indent=2).encode("utf-8")
temp_filepath = f"{target_filepath}.tmp"

with open(temp_filepath, "wb") as f:
    f.write(raw_bytes)
    f.flush()
    os.fsync(f.fileno())

os.replace(temp_filepath, target_filepath)

print("=" * 60)
print("RAW API ARCHIVAL SUCCESSFUL")
print("=" * 60)
print(f"Archived Path: {target_filepath}")
print(f"Bytes:         {len(raw_bytes)}")
print(f"Matches:       {match_count}")
print(f"SHA256:        {hashlib.sha256(raw_bytes).hexdigest()}")
print("=" * 60)
