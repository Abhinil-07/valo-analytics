# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 99: Trigger Evidence Dashboard Rebuild on Render
# MAGIC 
# MAGIC Automated deployment hook trigger for the production Evidence dashboard hosted on Render.
# MAGIC - **Target:** Render Static Site Deploy Hook
# MAGIC - **Trigger:** HTTP POST request with automatic retry logic and exponential backoff
# MAGIC - **Execution:** Called as the final task of the daily / Gold pipeline to refresh Parquet datasets

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
DEFAULT_DEPLOY_HOOK = "https://api.render.com/deploy/srv-danfbljtqb8s73bntar0?key=DsSDTt2ji5Q"

# Optional Databricks Widget to override hook URL dynamically
try:
    import IPython
    ipython = IPython.get_ipython()
    if ipython and "dbutils" in ipython.user_ns:
        dbutils = ipython.user_ns["dbutils"]
        dbutils.widgets.text("render_deploy_hook", DEFAULT_DEPLOY_HOOK, "Render Deploy Hook URL")
        DEPLOY_HOOK_URL = dbutils.widgets.get("render_deploy_hook").strip() or DEFAULT_DEPLOY_HOOK
    else:
        DEPLOY_HOOK_URL = DEFAULT_DEPLOY_HOOK
except Exception:
    DEPLOY_HOOK_URL = DEFAULT_DEPLOY_HOOK

# Mask secret key for secure operational logging
masked_hook = DEPLOY_HOOK_URL.split("?key=")[0] + "?key=***" if "?key=" in DEPLOY_HOOK_URL else DEPLOY_HOOK_URL

print("=" * 70)
print("EVIDENCE DASHBOARD DEPLOY TRIGGER (RENDER)")
print("=" * 70)
print(f"Trigger Timestamp: {datetime.now(timezone.utc).isoformat()}")
print(f"Target Endpoint:   {masked_hook}")
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
        
        # Render typically returns 200 or 201 on successful deploy hook receipt
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
# MAGIC ### Step 2: Deployment Verification & Pipeline Status

# COMMAND ----------
print("\n" + "=" * 70)
print("DEPLOYMENT TRIGGER AUDIT SUMMARY")
print("=" * 70)

if deploy_success:
    deploy_id = None
    if isinstance(response_data, dict):
        deploy_obj = response_data.get("deploy", response_data)
        deploy_id = deploy_obj.get("id") or deploy_obj.get("deployId")

    print("STATUS:            SUCCESS (Render rebuild scheduled)")
    if deploy_id:
        print(f"Render Deploy ID:  {deploy_id}")
    print(f"Raw Response:      {json.dumps(response_data, indent=2)}")
    print("=" * 70)
    print("The Evidence dashboard is now rebuilding with the latest Databricks Gold data.")
    print("Visit your Render URL in 1-2 minutes to view updated analytics.")
    print("=" * 70)
    
    # Return success payload for parent orchestrator
    try:
        dbutils.notebook.exit(json.dumps({
            "status": "SUCCESS",
            "deploy_id": deploy_id,
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
