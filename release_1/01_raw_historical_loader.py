# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 1: GitHub Historical Raw Loader
# MAGIC 
# MAGIC Downloads and archives unaltered historical match JSONs from the pinned GitHub commit:
# MAGIC - **Repository:** `Kunal-deve1oper/valorant-data-collector`
# MAGIC - **Pinned Commit:** `f2f983fce113aea9e02407e024439fd1f41fd8d2`
# MAGIC - **Source Path:** `raw_matches/`
# MAGIC - **Destination:** `/Volumes/<catalog>/raw/<volume>/historical/raw_matches/<filename>.json`
# MAGIC - **Idempotency:** Computes SHA256 content hashes to avoid redundant re-writes.

# COMMAND ----------
import os
import json
import hashlib
import requests

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CATALOG = "valorant"
RAW_SCHEMA = "raw"
VOLUME = "data"

GITHUB_REPO = "Kunal-deve1oper/valorant-data-collector"
PINNED_COMMIT = "f2f983fce113aea9e02407e024439fd1f41fd8d2"
RAW_MATCHES_DIR = "raw_matches"

# Max files to download (set to None for all 204 files, or an int like 10 for quick testing)
MAX_FILES = None

RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"
HISTORICAL_TARGET_DIR = f"{RAW_VOLUME_PATH}/historical/{RAW_MATCHES_DIR}"

print(f"Historical Destination: {HISTORICAL_TARGET_DIR}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Raw Schema & Volume Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME}")

print(f"Verified raw volume: /Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Discover Historical Files at Pinned Commit

# COMMAND ----------
tree_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/{PINNED_COMMIT}?recursive=1"
headers = {"User-Agent": "Valorant-Historical-Loader/1.0"}

print(f"Discovering files from GitHub tree: {tree_url}")
resp = requests.get(tree_url, headers=headers, timeout=30)
resp.raise_for_status()

tree_items = resp.json().get("tree", [])
raw_files = [
    item["path"] for item in tree_items
    if item.get("path", "").startswith(RAW_MATCHES_DIR)
    and item.get("path", "").endswith(".json")
    and item.get("type") == "blob"
]

if MAX_FILES:
    raw_files = raw_files[:MAX_FILES]

print(f"Discovered {len(raw_files)} historical JSON file(s) to process.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Download & Archive Unchanged Source JSONs

# COMMAND ----------
os.makedirs(HISTORICAL_TARGET_DIR, exist_ok=True)

ingested = 0
skipped = 0
failed = 0

for idx, rel_path in enumerate(raw_files, 1):
    filename = os.path.basename(rel_path)
    target_file = f"{HISTORICAL_TARGET_DIR}/{filename}"
    raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{PINNED_COMMIT}/{rel_path}"

    try:
        # Download exact raw bytes
        r = requests.get(raw_url, headers=headers, timeout=30)
        r.raise_for_status()
        raw_bytes = r.content
        content_hash = hashlib.sha256(raw_bytes).hexdigest()

        # Idempotency check: if file exists and hash matches, skip
        if os.path.exists(target_file):
            with open(target_file, "rb") as existing_f:
                if hashlib.sha256(existing_f.read()).hexdigest() == content_hash:
                    skipped += 1
                    continue

        # Write unaltered source JSON atomically
        temp_file = f"{target_file}.tmp"
        with open(temp_file, "wb") as f:
            f.write(raw_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, target_file)

        ingested += 1
        if idx % 20 == 0 or idx == len(raw_files):
            print(f"[{idx}/{len(raw_files)}] Ingested {filename}")

    except Exception as e:
        print(f"Error ingesting {rel_path}: {e}")
        failed += 1

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Run Summary

# COMMAND ----------
print("=" * 60)
print("HISTORICAL RAW INGESTION SUMMARY")
print("=" * 60)
print(f"Total Discovered: {len(raw_files)}")
print(f"Newly Ingested:   {ingested}")
print(f"Skipped (Same):   {skipped}")
print(f"Failed:           {failed}")
print("=" * 60)
