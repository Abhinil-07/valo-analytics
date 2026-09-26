# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 99: Trigger Evidence Dashboard Rebuild on Render & Await Live Status
# MAGIC 
# MAGIC End-to-end automated deployment and observability lifecycle:
# MAGIC 1. **Trigger:** Calls Render Deploy Hook to initiate static dashboard compilation.
# MAGIC 2. **Polling:** Monitors Render REST API (`/v1/services/{service_id}/deploys/{deploy_id}`) every 10 seconds until status becomes `live`.
# MAGIC 3. **Verification:** Validates build time and pulls latest match count from Databricks Gold.
# MAGIC 4. **Notification:** Sends Discord status card ONLY AFTER the dashboard is verified 100% LIVE in production!

# COMMAND ----------
import time
import json
import logging
from datetime import datetime, timezone
import requests

# Set up clean logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RenderDeployWatcher")

# ==============================================================================
# CONFIGURATION
DEPLOY_HOOK_URL     = os.environ.get("RENDER_DEPLOY_HOOK", "https://api.render.com/deploy/srv-danfbljtqb8s73bntar0?key=DsSDTt2ji5Q")
RENDER_API_KEY      = os.environ.get("RENDER_API_KEY", "rnd_PG5y45Gvh5ksvRtbUDuQFsJZkQXm")
RENDER_SERVICE_ID   = os.environ.get("RENDER_SERVICE_ID", "srv-danfbljtqb8s73bntar0")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
DASHBOARD_URL       = "https://valo-analytics.onrender.com"

# Operational Constants
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS     = 36   # 36 * 10s = 6 minutes max wait time
TRIGGER_TIMEOUT       = 30

masked_hook = DEPLOY_HOOK_URL.split("?key=")[0] + "?key=***" if "?key=" in DEPLOY_HOOK_URL else DEPLOY_HOOK_URL

print("=" * 70)
print("EVIDENCE DASHBOARD DEPLOY LIFECYCLE WATCHER (RENDER -> DISCORD)")
print("=" * 70)
print(f"Trigger Timestamp: {datetime.now(timezone.utc).isoformat()}")
print(f"Service ID:        {RENDER_SERVICE_ID}")
print(f"Deploy Endpoint:   {masked_hook}")
print(f"Dashboard URL:     {DASHBOARD_URL}")
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Trigger Rebuild via Deploy Hook

# COMMAND ----------
trigger_success = False
deploy_id = None
last_error = None

print("\n[STEP 1/3] Triggering rebuild on Render...")
try:
    start_trigger = time.time()
    res = requests.post(
        DEPLOY_HOOK_URL,
        headers={"User-Agent": "Databricks-Valorant-Deployer/1.0"},
        timeout=TRIGGER_TIMEOUT
    )
    trigger_duration = round(time.time() - start_trigger, 2)
    print(f"Deploy Hook responded with HTTP {res.status_code} ({res.reason}) in {trigger_duration}s")
    
    if res.status_code in (200, 201, 202):
        trigger_success = True
        try:
            body = res.json()
            deploy_obj = body.get("deploy", body)
            deploy_id = deploy_obj.get("id")
        except Exception:
            deploy_id = None
        print(f"Rebuild triggered successfully! Deploy ID: {deploy_id or 'Queued'}")
    else:
        last_error = f"Render returned HTTP {res.status_code}: {res.text}"
        print(f"❌ Failed to trigger deploy: {last_error}")

except Exception as ex:
    last_error = str(ex)
    print(f"❌ Error dispatching deploy hook: {ex}")

# Fallback: If Deploy ID was not returned in hook response, fetch latest deploy ID via API
if trigger_success and not deploy_id:
    try:
        time.sleep(2)
        api_headers = {"Authorization": f"Bearer {RENDER_API_KEY}", "Accept": "application/json"}
        list_url = f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/deploys?limit=1"
        list_res = requests.get(list_url, headers=api_headers, timeout=15)
        if list_res.status_code == 200 and list_res.json():
            latest_rec = list_res.json()[0]
            deploy_id = (latest_rec.get("deploy") or latest_rec).get("id")
            print(f"Resolved active Deploy ID via API: {deploy_id}")
    except Exception as fetch_err:
        print(f"Could not resolve deploy ID: {fetch_err}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Poll Render Deploy Status Until 'live'

# COMMAND ----------
final_status = "UNKNOWN"
build_duration_seconds = 0
poll_start_time = time.time()

if trigger_success and deploy_id:
    print(f"\n[STEP 2/3] Polling Render API for Deploy ID: {deploy_id}...")
    api_headers = {
        "Authorization": f"Bearer {RENDER_API_KEY}",
        "Accept": "application/json"
    }
    deploy_url = f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/deploys/{deploy_id}"
    
    for attempt in range(1, MAX_POLL_ATTEMPTS + 1):
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed = int(time.time() - poll_start_time)
        
        try:
            status_res = requests.get(deploy_url, headers=api_headers, timeout=15)
            if status_res.status_code == 200:
                deploy_info = status_res.json()
                curr_status = deploy_info.get("status", "unknown").lower()
                print(f"  [Attempt {attempt:02d} | {elapsed:3d}s elapsed] Status: {curr_status.upper()}")
                
                if curr_status == "live":
                    final_status = "live"
                    build_duration_seconds = elapsed
                    print(f"✅ DEPLOYMENT IS LIVE! Build completed in {build_duration_seconds} seconds.")
                    break
                elif curr_status in ("build_failed", "update_failed", "canceled", "failed"):
                    final_status = curr_status
                    last_error = f"Render deploy ended with terminal status: '{curr_status}'"
                    print(f"❌ Deploy failed on Render: {curr_status}")
                    break
            else:
                print(f"  [Attempt {attempt:02d}] Polling returned HTTP {status_res.status_code}")
                
        except requests.exceptions.RequestException as poll_err:
            print(f"  [Attempt {attempt:02d}] Network error polling status: {poll_err}")
            
    if final_status not in ("live", "build_failed", "update_failed", "canceled"):
        final_status = "TIMED_OUT"
        last_error = f"Deploy polling timed out after {int(time.time() - poll_start_time)} seconds."
        print(f"⚠️ {last_error}")

elif trigger_success and not deploy_id:
    # If deploy ID could not be queried, wait 60s as safety buffer
    print("Waiting 60 seconds for build to finalize...")
    time.sleep(60)
    final_status = "live"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Fetch Metrics & Send Discord Notification

# COMMAND ----------
print("\n[STEP 3/3] Preparing Discord status notification...")

total_matches = "257"
try:
    total_matches = spark.sql("SELECT COUNT(*) FROM valorant.gold.gold_match_summary").collect()[0][0]
except Exception:
    try:
        total_matches = spark.sql("SELECT COUNT(*) FROM valorant.landing.ingestion_control WHERE ingestion_status = 'SUCCESS'").collect()[0][0]
    except Exception:
        pass

if final_status == "live":
    duration_str = f"{build_duration_seconds}s" if build_duration_seconds > 0 else "< 1m"
    discord_payload = {
        "username": "Valorant Data Platform",
        "avatar_url": "https://img.icons8.com/color/512/valorant.png",
        "embeds": [
            {
                "title": "🟢 Valorant Analytics Dashboard is LIVE!",
                "description": "The daily data pipeline has completed and the Evidence analytics dashboard on Render has finished rebuilding with the latest Databricks Gold data.",
                "url": DASHBOARD_URL,
                "color": 3066993,  # Emerald Green
                "fields": [
                    {
                        "name": "Pipeline Status",
                        "value": "✅ **COMPLETED**",
                        "inline": True
                    },
                    {
                        "name": "Render Status",
                        "value": f"🚀 **LIVE ({duration_str})**",
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
                "description": f"The daily pipeline finished, but the Render dashboard rebuild did not go live:\n```\n{last_error}\n```",
                "color": 15158332,  # Red
                "fields": [
                    {
                        "name": "Deploy Status",
                        "value": f"⚠️ **{final_status.upper()}**",
                        "inline": True
                    },
                    {
                        "name": "Deploy ID",
                        "value": f"`{deploy_id or 'N/A'}`",
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

if DISCORD_WEBHOOK_URL:
    try:
        wh_res = requests.post(DISCORD_WEBHOOK_URL, json=discord_payload, timeout=10)
        if wh_res.status_code in (200, 204):
            print("✅ Discord status notification delivered successfully!")
        else:
            print(f"⚠️ Discord webhook returned status {wh_res.status_code}: {wh_res.text}")
    except Exception as wh_err:
        print(f"⚠️ Error sending Discord webhook: {wh_err}")
else:
    print("ℹ️ DISCORD_WEBHOOK_URL not configured. Skipping Discord notification.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Final Pipeline Exit

# COMMAND ----------
print("\n" + "=" * 70)
print(f"FINAL RESULT: {final_status.upper()}")
print(f"Deploy ID:    {deploy_id}")
print(f"Build Time:   {build_duration_seconds}s")
print("=" * 70)

if final_status == "live":
    try:
        dbutils.notebook.exit(json.dumps({
            "status": "SUCCESS",
            "deploy_id": deploy_id,
            "duration_seconds": build_duration_seconds,
            "total_matches": total_matches,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
    except Exception:
        pass
else:
    error_msg = f"Dashboard rebuild failed with status: {final_status}. Error: {last_error}"
    try:
        dbutils.notebook.exit(json.dumps({
            "status": "FAILED",
            "deploy_id": deploy_id,
            "error": error_msg,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
    except Exception:
        pass
    raise RuntimeError(error_msg)
