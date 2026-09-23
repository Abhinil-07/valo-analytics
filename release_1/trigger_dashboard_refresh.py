# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 99: Trigger Evidence Dashboard Rebuild on Render & Notify Discord
# MAGIC 
# MAGIC Automated deployment hook trigger and notification system for the production Evidence dashboard:
# MAGIC 1. **Trigger:** Dispatches an HTTP POST request to Render Deploy Hook with retry mechanism.
# MAGIC 2. **Audit:** Retrieves match count from Databricks Gold and verifies Render response.
# MAGIC 3. **Notification:** Sends a rich status card embed to Discord via webhook.

# COMMAND ----------
import time
import json
import logging
from datetime import datetime, timezone
import requests

# Set up clean logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RenderDeployTrigger")

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEPLOY_HOOK_URL = "https://api.render.com/deploy/srv-danfbljtqb8s73bntar0?key=DsSDTt2ji5Q"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1552391228753191012/aHg1wdL7i9AiKc4GSKJPIWB30dgRD2t0yXCA6PfLU3Qvw2_bFyWqeEFA6byX5361n3mq"
DASHBOARD_URL = "https://valo-analytics.onrender.com"

# Mask secret keys for secure operational logging
masked_hook = DEPLOY_HOOK_URL.split("?key=")[0] + "?key=***" if "?key=" in DEPLOY_HOOK_URL else DEPLOY_HOOK_URL

print("=" * 70)
print("EVIDENCE DASHBOARD DEPLOY TRIGGER & DISCORD NOTIFIER")
print("=" * 70)
print(f"Trigger Timestamp: {datetime.now(timezone.utc).isoformat()}")
print(f"Target Endpoint:   {masked_hook}")
print(f"Dashboard URL:     {DASHBOARD_URL}")
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Send Deploy Hook with Retry Mechanism

# COMMAND ----------
MAX_RETRIES = 3
INITIAL_BACKOFF = 2.0  # seconds
TIMEOUT = 30  # seconds

deploy_success = False
response_data = None
last_error = None

for attempt in range(1, MAX_RETRIES + 1):
    try:
        print(f"\n[Attempt {attempt}/{MAX_RETRIES}] Dispatching POST request to Render Deploy Hook...")
        
        start_time = time.time()
        response = requests.post(
            DEPLOY_HOOK_URL,
            headers={
                "User-Agent": "Databricks-Valorant-Analytics-Deployer/1.0",
                "Content-Type": "application/json"
            },
            timeout=TIMEOUT
        )
        duration = round(time.time() - start_time, 2)
        
        print(f"HTTP Status Code: {response.status_code} ({response.reason}) in {duration}s")
        
        # Render returns 200 or 201 on successful deploy hook receipt
        if response.status_code in (200, 201):
            deploy_success = True
            try:
                response_data = response.json()
            except Exception:
                response_data = {"raw_text": response.text}
            
            print("Render deploy hook accepted successfully!")
            break
        elif response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", INITIAL_BACKOFF * attempt))
            print(f"Rate limited by Render. Waiting {retry_after}s before retry...")
            time.sleep(retry_after)
        else:
            print(f"Render returned non-success status: {response.status_code} - {response.text}")
            time.sleep(INITIAL_BACKOFF * attempt)
            
    except requests.exceptions.RequestException as req_err:
        last_error = req_err
        print(f"Network error communicating with Render: {req_err}")
        time.sleep(INITIAL_BACKOFF * attempt)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Fetch Pipeline Metrics & Send Discord Notification

# COMMAND ----------
# 1. Fetch current metrics from Databricks Gold / Landing
total_matches = "N/A"
total_players = "N/A"

try:
    total_matches = spark.sql("SELECT COUNT(*) FROM valorant.gold.gold_match_summary").collect()[0][0]
    total_players = spark.sql("SELECT COUNT(DISTINCT player_puuid) FROM valorant.silver.dim_player").collect()[0][0]
except Exception:
    try:
        total_matches = spark.sql("SELECT COUNT(*) FROM valorant.landing.ingestion_control WHERE ingestion_status = 'SUCCESS'").collect()[0][0]
    except Exception:
        pass

print(f"\nAudit Metrics: Total Matches = {total_matches}, Total Players = {total_players}")

# 2. Build Discord Payload
if deploy_success:
    deploy_id = None
    if isinstance(response_data, dict):
        deploy_obj = response_data.get("deploy", response_data)
        deploy_id = deploy_obj.get("id") or deploy_obj.get("deployId")

    discord_payload = {
        "username": "Valorant Data Platform",
        "avatar_url": "https://img.icons8.com/color/512/valorant.png",
        "embeds": [
            {
                "title": "🟢 Valorant Daily Pipeline & Dashboard Refresh",
                "description": "The daily data pipeline has executed end-to-end and triggered a production rebuild of the Evidence analytics dashboard on Render.",
                "url": DASHBOARD_URL,
                "color": 3066993,  # Emerald Green
                "fields": [
                    {
                        "name": "Pipeline Status",
                        "value": "✅ **COMPLETED**",
                        "inline": True
                    },
                    {
                        "name": "Render Deployment",
                        "value": f"🚀 **TRIGGERED**" + (f" (`{deploy_id}`)" if deploy_id else ""),
                        "inline": True
                    },
                    {
                        "name": "Matches Processed",
                        "value": f"📊 **{total_matches} matches**",
                        "inline": True
                    },
                    {
                        "name": "Layers Updated",
                        "value": "`Bronze` ➔ `Silver` ➔ `Gold` ➔ `Render Parquet`",
                        "inline": False
                    },
                    {
                        "name": "Live Analytics Dashboard",
                        "value": f"[👉 **Open Dashboard on Render**]({DASHBOARD_URL})",
                        "inline": False
                    }
                ],
                "footer": {
                    "text": "Databricks Medallion Architecture • Free Edition",
                    "icon_url": "https://img.icons8.com/color/512/databricks.png"
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        ]
    }
else:
    discord_payload = {
        "username": "Valorant Data Platform",
        "avatar_url": "https://img.icons8.com/color/512/valorant.png",
        "embeds": [
            {
                "title": "🔴 Valorant Dashboard Rebuild Failed",
                "description": f"The daily pipeline encountered an error while triggering the Render deployment hook:\n```\n{last_error}\n```",
                "color": 15158332,  # Red
                "fields": [
                    {
                        "name": "Pipeline Status",
                        "value": "⚠️ **DEPLOY FAILED**",
                        "inline": True
                    },
                    {
                        "name": "Retries Attempted",
                        "value": f"{MAX_RETRIES} attempts",
                        "inline": True
                    }
                ],
                "footer": {
                    "text": "Databricks Medallion Architecture • Error Notification",
                    "icon_url": "https://img.icons8.com/color/512/databricks.png"
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        ]
    }

# 3. Post to Discord Webhook
try:
    print("Dispatching status card to Discord...")
    webhook_res = requests.post(DISCORD_WEBHOOK_URL, json=discord_payload, timeout=10)
    if webhook_res.status_code in (200, 204):
        print("✅ Discord status notification delivered successfully!")
    else:
        print(f"⚠️ Discord webhook returned status {webhook_res.status_code}: {webhook_res.text}")
except Exception as wh_err:
    print(f"⚠️ Failed to send Discord webhook: {wh_err}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Deployment Verification & Pipeline Status

# COMMAND ----------
print("\n" + "=" * 70)
print("DEPLOYMENT TRIGGER AUDIT SUMMARY")
print("=" * 70)

if deploy_success:
    print("STATUS:            SUCCESS (Render rebuild scheduled & Discord notified)")
    if deploy_id:
        print(f"Render Deploy ID:  {deploy_id}")
    print(f"Raw Response:      {json.dumps(response_data, indent=2)}")
    print("=" * 70)
    print("The Evidence dashboard is now rebuilding with the latest Databricks Gold data.")
    print(f"Visit {DASHBOARD_URL} in 1-2 minutes to view updated analytics.")
    print("=" * 70)
    
    # Return success payload for parent orchestrator
    try:
        dbutils.notebook.exit(json.dumps({
            "status": "SUCCESS",
            "deploy_id": deploy_id,
            "total_matches": total_matches,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
    except Exception:
        pass

else:
    error_msg = f"Failed to trigger Render deploy after {MAX_RETRIES} attempts. Error: {last_error}"
    print("STATUS: FAILED")
    print(f"Error Detail: {error_msg}")
    print("=" * 70)
    
    try:
        dbutils.notebook.exit(json.dumps({
            "status": "FAILED",
            "error": error_msg,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
    except Exception:
        pass
        
    raise RuntimeError(error_msg)
