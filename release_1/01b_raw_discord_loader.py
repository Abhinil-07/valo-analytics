# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 1b: Discord Historical / Daily Match Raw Loader
# MAGIC 
# MAGIC Ingests raw Valorant match JSON attachments directly from a designated Discord channel:
# MAGIC - **Source:** Discord Channel message attachments (`.json`)
# MAGIC - **Destination:** `/Volumes/<catalog>/raw/<volume>/discord/<message_id>_<filename>.json`
# MAGIC - **Unaltered Archival:** Preserves exact original byte payloads before any parsing or normalization.
# MAGIC - **Idempotency:** Computes SHA256 content hashes to avoid redundant network writes and duplicate files.

# COMMAND ----------
import os
import json
import hashlib
import time
import requests

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CATALOG = "valorant"
RAW_SCHEMA = "raw"
VOLUME = "data"

# Discord Channel & Credentials
# Can be overridden via Databricks Widgets or dbutils.secrets
CHANNEL_ID = ""  # e.g., "123456789012345678"
DISCORD_TOKEN = ""  # Bot token (e.g. "Bot MTE...") or User Token

# Databricks Widgets support (optional, falls back to variables above)
try:
    import IPython
    ipython = IPython.get_ipython()
    if ipython and "dbutils" in ipython.user_ns:
        dbutils = ipython.user_ns["dbutils"]
        dbutils.widgets.text("channel_id", CHANNEL_ID, "Discord Channel ID")
        dbutils.widgets.text("discord_token", DISCORD_TOKEN, "Discord Token")
        CHANNEL_ID = dbutils.widgets.get("channel_id") or CHANNEL_ID
        DISCORD_TOKEN = dbutils.widgets.get("discord_token") or DISCORD_TOKEN
except Exception:
    pass

RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"
DISCORD_TARGET_DIR = f"{RAW_VOLUME_PATH}/discord"

print(f"Target Raw Discord Directory: {DISCORD_TARGET_DIR}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Raw Schema & Volume Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME}")

os.makedirs(DISCORD_TARGET_DIR, exist_ok=True)
print(f"Verified raw volume: {DISCORD_TARGET_DIR}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Fetch Attachments from Discord Channel History & Archive to Raw

# COMMAND ----------
if not CHANNEL_ID or not DISCORD_TOKEN:
    raise ValueError("Please provide both CHANNEL_ID and DISCORD_TOKEN to proceed.")

headers = {
    "Authorization": DISCORD_TOKEN.strip(),
    "User-Agent": "Databricks-Discord-Raw-Loader/1.0"
}

base_url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
params = {"limit": 100}

total_discovered = 0
newly_ingested = 0
skipped_existing = 0
failed_count = 0

print(f"Starting message crawl for channel: {CHANNEL_ID}...")

while True:
    res = requests.get(base_url, headers=headers, params=params, timeout=30)
    
    # Handle Discord API rate limits gracefully
    if res.status_code == 429:
        retry_after = res.json().get("retry_after", 2.0)
        print(f"Rate limited by Discord. Waiting {retry_after} seconds...")
        time.sleep(float(retry_after))
        continue

    if res.status_code != 200:
        print(f"Failed to fetch messages from Discord: {res.status_code} - {res.text}")
        break

    messages = res.json()
    if not messages:
        print("Reached beginning of channel history.")
        break

    for msg in messages:
        attachments = msg.get("attachments", [])
        for att in attachments:
            filename = att.get("filename", "")
            download_url = att.get("url", "")

            # Filter for match JSON files
            if filename.lower().endswith(".json"):
                total_discovered += 1
                clean_filename = f"{msg['id']}_{filename}"
                target_file = f"{DISCORD_TARGET_DIR}/{clean_filename}"

                try:
                    # Download exact raw payload
                    file_resp = requests.get(download_url, timeout=30)
                    file_resp.raise_for_status()
                    raw_bytes = file_resp.content
                    content_hash = hashlib.sha256(raw_bytes).hexdigest()

                    # Idempotency check: if file exists and hash matches, skip
                    if os.path.exists(target_file):
                        with open(target_file, "rb") as existing_f:
                            if hashlib.sha256(existing_f.read()).hexdigest() == content_hash:
                                skipped_existing += 1
                                continue

                    # Write unaltered source JSON atomically
                    temp_file = f"{target_file}.tmp"
                    with open(temp_file, "wb") as f:
                        f.write(raw_bytes)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(temp_file, target_file)

                    newly_ingested += 1
                    if newly_ingested % 10 == 0:
                        print(f"Archived {newly_ingested} files to Raw Volume...")

                except Exception as e:
                    print(f"Error archiving attachment {clean_filename}: {e}")
                    failed_count += 1

                time.sleep(0.1)  # Throttle CDN requests

    # Paginate backwards using Discord message ID
    last_message_id = messages[-1]["id"]
    params["before"] = last_message_id
    time.sleep(0.4)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Discord Raw Ingestion Summary

# COMMAND ----------
print("=" * 60)
print("DISCORD RAW INGESTION SUMMARY")
print("=" * 60)
print(f"Total Discovered:     {total_discovered}")
print(f"Newly Archived (Raw): {newly_ingested}")
print(f"Skipped (Unchanged):  {skipped_existing}")
print(f"Failed:               {failed_count}")
print(f"Raw Volume Location:  {DISCORD_TARGET_DIR}")
print("=" * 60)
