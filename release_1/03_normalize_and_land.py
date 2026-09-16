# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 3: Normalization & Canonical Landing
# MAGIC 
# MAGIC Normalizes raw JSON files from both `raw/historical/` and `raw/api/` into the exact **Canonical Landing Contract**:
# MAGIC 
# MAGIC ```text
# MAGIC Raw Files (GitHub Historical & API)
# MAGIC                 │
# MAGIC                 ▼
# MAGIC         Format Detection & Unwrap
# MAGIC                 │
# MAGIC                 ▼
# MAGIC       Canonical Normalization (Rules A-H)
# MAGIC                 │
# MAGIC                 ▼
# MAGIC             Validation
# MAGIC                 │
# MAGIC                 ▼
# MAGIC       Write to Landing Volume
# MAGIC   /Volumes/<catalog>/landing/<volume>/matches/YYYY/MM/DD/<match_id>.json
# MAGIC                 │
# MAGIC                 ▼
# MAGIC     Delta Ingestion Control MERGE
# MAGIC ```

# COMMAND ----------
import os
import glob
import json
import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CATALOG = "valorant"
RAW_SCHEMA = "raw"
LANDING_SCHEMA = "landing"
VOLUME = "data"

RAW_HISTORICAL_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}/historical"
RAW_API_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}/api"
LANDING_BASE_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{VOLUME}/matches"
CONTROL_TABLE = f"{CATALOG}.{LANDING_SCHEMA}.ingestion_control"

print(f"Historical Raw Path: {RAW_HISTORICAL_PATH}")
print(f"API Raw Path:        {RAW_API_PATH}")
print(f"Canonical Landing:   {LANDING_BASE_PATH}")
print(f"Control Table:       {CONTROL_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Landing Schema & Control Table Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}.{VOLUME}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
    match_id STRING NOT NULL,
    match_date DATE,
    landing_path STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_status STRING,
    source STRING,
    load_type STRING,
    source_file STRING,
    provenance STRING,
    error_message STRING
)
USING DELTA
COMMENT 'Tracks canonical match landing state, provenance, and idempotency'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Normalization Logic Functions (Rules A-H)

# COMMAND ----------
def detect_and_unwrap(raw_json: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """Detects wrapper structure and returns (source_type, list_of_raw_matches)."""
    if "matchData" in raw_json and isinstance(raw_json["matchData"], dict):
        inner = raw_json["matchData"].get("data")
        matches = [inner] if isinstance(inner, dict) else (inner if isinstance(inner, list) else [])
        return "github", matches
    elif "status" in raw_json and "data" in raw_json:
        inner = raw_json.get("data")
        matches = inner if isinstance(inner, list) else ([inner] if isinstance(inner, dict) else [])
        return "api", matches
    elif "metadata" in raw_json and "matchid" in (raw_json.get("metadata") or {}):
        return "bare_match", [raw_json]
    return "unknown", []


def normalize_match_payload(raw_match_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Applies canonical normalization rules A-H without calculating KPIs."""
    match = copy.deepcopy(raw_match_dict)

    # Strip wrapper fields
    for k in ("hasWon", "matchData", "status", "errors"):
        match.pop(k, None)

    # Rule C: is_available
    is_available = match.get("is_available") if "is_available" in match else None

    # Metadata
    metadata = match.get("metadata")
    if not isinstance(metadata, dict) or not metadata.get("matchid"):
        raise ValueError("Missing valid metadata.matchid")

    # Rule D & E: Players normalization
    players = match.get("players")
    if isinstance(players, dict):
        for grp in ("all_players", "red", "blue"):
            p_list = players.get(grp)
            if isinstance(p_list, list):
                for p in p_list:
                    if not isinstance(p, dict):
                        continue
                    # Rule D: behaviour -> behavior
                    if "behaviour" in p:
                        b = p.pop("behaviour")
                        if "behavior" not in p:
                            p["behavior"] = b
                    # Rule E: Ability casts (c_cast -> c_casts)
                    casts = p.get("ability_casts")
                    if isinstance(casts, dict):
                        for sing, plur in (("c_cast", "c_casts"), ("q_cast", "q_casts"),
                                           ("e_cast", "e_casts"), ("x_cast", "x_casts")):
                            if sing in casts:
                                v = casts.pop(sing)
                                if plur not in casts:
                                    casts[plur] = v

    # Rule F: Teams normalization (roaster -> roster)
    teams = match.get("teams")
    if isinstance(teams, dict):
        for side in ("red", "blue"):
            t = teams.get(side)
            if isinstance(t, dict) and "roaster" in t:
                r_val = t.pop("roaster")
                if "roster" not in t:
                    t["roster"] = r_val

    # Rule G: Kills normalization
    existing_kills = match.get("kills")
    if existing_kills is not None and isinstance(existing_kills, list):
        canonical_kills = existing_kills
    else:
        canonical_kills = []
        seen_sigs: Set[Tuple[Any, ...]] = set()
        rounds_list = match.get("rounds", [])
        if isinstance(rounds_list, list):
            for r_idx, r in enumerate(rounds_list):
                if not isinstance(r, dict):
                    continue
                for ps in r.get("player_stats", []):
                    if not isinstance(ps, dict):
                        continue
                    for ke in ps.get("kill_events", []):
                        if not isinstance(ke, dict):
                            continue
                        ke_c = copy.deepcopy(ke)
                        if "round" not in ke_c or ke_c["round"] is None:
                            ke_c["round"] = r_idx
                        sig = (ke_c.get("kill_time_in_match"), ke_c.get("kill_time_in_round"),
                               ke_c.get("round"), ke_c.get("killer_puuid"), ke_c.get("victim_puuid"))
                        if sig not in seen_sigs:
                            seen_sigs.add(sig)
                            canonical_kills.append(ke_c)

    # Canonical Object
    canonical = {
        "is_available": is_available,
        "metadata": metadata,
        "players": players if players is not None else {},
        "observers": match.get("observers", []),
        "coaches": match.get("coaches", []),
        "teams": teams if teams is not None else {},
        "rounds": match.get("rounds", []),
        "kills": canonical_kills,
    }
    for k, v in match.items():
        if k not in canonical and k not in ("hasWon", "matchData", "status", "errors"):
            canonical[k] = v

    return canonical

print("Normalization functions compiled.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Discover Raw Source Files

# COMMAND ----------
historical_files = glob.glob(f"{RAW_HISTORICAL_PATH}/**/*.json", recursive=True)
api_files = glob.glob(f"{RAW_API_PATH}/**/*.json", recursive=True)

all_raw_files = [(f, "github", "historical") for f in historical_files] + \
                [(f, "api", "incremental") for f in api_files]

print(f"Discovered {len(historical_files)} historical raw file(s).")
print(f"Discovered {len(api_files)} live API raw file(s).")
print(f"Total raw source files to process: {len(all_raw_files)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Execute Normalization & Idempotent Landing

# COMMAND ----------
# Fetch match IDs already successfully landed
existing_df = spark.sql(f"SELECT match_id FROM {CONTROL_TABLE} WHERE ingestion_status = 'SUCCESS'")
successful_ids = {row["match_id"] for row in existing_df.collect()}
print(f"Found {len(successful_ids)} existing successful match(es) in control table.")

landed_count = 0
skipped_count = 0
failed_count = 0

for file_path, default_source, load_type in all_raw_files:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_content = json.load(f)

        src_type, raw_matches = detect_and_unwrap(raw_content)
        final_source = src_type if src_type != "unknown" else default_source

        for raw_m in raw_matches:
            try:
                canonical_m = normalize_match_payload(raw_m)
                meta = canonical_m["metadata"]
                match_id = meta["matchid"]

                # Derive UTC date from game_start
                game_start = meta.get("game_start")
                ts = float(game_start) if game_start else 0.0
                if ts > 1e11:
                    ts = ts / 1000.0
                match_dt = datetime.fromtimestamp(ts, tz=timezone.utc) if ts > 0 else datetime.now(timezone.utc)
                match_date = match_dt.date()

                # Idempotency check
                if match_id in successful_ids:
                    skipped_count += 1
                    continue

                # Target landing path: landing/matches/YYYY/MM/DD/<match_id>.json
                date_path = match_dt.strftime("%Y/%m/%d")
                target_dir = f"{LANDING_BASE_PATH}/{date_path}"
                target_file = f"{target_dir}/{match_id}.json"

                os.makedirs(target_dir, exist_ok=True)
                temp_file = f"{target_file}.tmp"
                with open(temp_file, "w", encoding="utf-8") as out_f:
                    json.dump(canonical_m, out_f, ensure_ascii=False, indent=2)
                    out_f.flush()
                    os.fsync(out_f.fileno())
                os.replace(temp_file, target_file)

                # Record SUCCESS in Delta control table
                prov = json.dumps({"source": final_source, "source_file": file_path}).replace("'", "''")
                spark.sql(f"""
                MERGE INTO {CONTROL_TABLE} AS target
                USING (SELECT '{match_id}' AS match_id) AS source
                ON target.match_id = source.match_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.match_date = DATE('{match_date.isoformat()}'),
                        target.landing_path = '{target_file}',
                        target.ingestion_timestamp = current_timestamp(),
                        target.ingestion_status = 'SUCCESS',
                        target.source = '{final_source}',
                        target.load_type = '{load_type}',
                        target.source_file = '{file_path}',
                        target.provenance = '{prov}',
                        target.error_message = NULL
                WHEN NOT MATCHED THEN
                    INSERT (match_id, match_date, landing_path, ingestion_timestamp, ingestion_status, source, load_type, source_file, provenance, error_message)
                    VALUES ('{match_id}', DATE('{match_date.isoformat()}'), '{target_file}', current_timestamp(), 'SUCCESS', '{final_source}', '{load_type}', '{file_path}', '{prov}', NULL)
                """)

                landed_count += 1
                successful_ids.add(match_id)

            except Exception as match_err:
                failed_count += 1
                print(f"Error normalizing match from {file_path}: {match_err}")

    except Exception as file_err:
        failed_count += 1
        print(f"Error reading file {file_path}: {file_err}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Audit Summary & View Control Table

# COMMAND ----------
print("=" * 60)
print("CANONICAL LANDING INGESTION SUMMARY")
print("=" * 60)
print(f"Total Raw Files:      {len(all_raw_files)}")
print(f"Newly Landed Matches: {landed_count}")
print(f"Skipped (Duplicate):  {skipped_count}")
print(f"Failed Matches:       {failed_count}")
print("=" * 60)

display(spark.sql(f"SELECT * FROM {CONTROL_TABLE} ORDER BY ingestion_timestamp DESC LIMIT 20"))
