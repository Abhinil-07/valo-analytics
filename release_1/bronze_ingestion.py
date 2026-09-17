# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Release 1: Bronze Layer Ingestion (All 9 Tables)
# MAGIC 
# MAGIC Ingests canonical match JSONs from Landing Volume into all nine Bronze Delta tables:
# MAGIC 
# MAGIC **Core Entity Tables:**
# MAGIC 1. **`bronze_match`** (Grain: 1 row per match, Key: `match_id`)
# MAGIC 2. **`bronze_team`** (Grain: 1 row per team per match, Composite Key: `match_id` + `team_side`)
# MAGIC 3. **`bronze_player`** (Grain: 1 row per player per match, Composite Key: `match_id` + `player_puuid`)
# MAGIC 4. **`bronze_round`** (Grain: 1 row per round per match, Composite Key: `match_id` + `round_number`)
# MAGIC 
# MAGIC **Round-Level & Event Tables:**
# MAGIC 5. **`bronze_round_player_stats`** (Grain: 1 row per player per round per match, Composite Key: `match_id` + `round_number` + `player_puuid`)
# MAGIC 6. **`bronze_kill_event`** (Grain: 1 row per kill event, Composite Key: `match_id` + `kill_event_sequence`)
# MAGIC 7. **`bronze_damage_event`** (Grain: 1 row per damage event, Composite Key: `match_id` + `round_number` + `attacker_puuid` + `damage_event_sequence`)
# MAGIC 8. **`bronze_plant_event`** (Grain: 1 row per spike plant event per round, Composite Key: `match_id` + `round_number` + `plant_event_sequence`)
# MAGIC 9. **`bronze_defuse_event`** (Grain: 1 row per spike defuse event per round, Composite Key: `match_id` + `round_number` + `defuse_event_sequence`)
# MAGIC 
# MAGIC All writes use **Delta MERGE** for strict idempotency.

# COMMAND ----------
import os
import glob
import json
from typing import Any, Dict, List, Optional
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, BooleanType, DoubleType, ArrayType
)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CATALOG = "valorant"
BRONZE_SCHEMA = "bronze"
LANDING_SCHEMA = "landing"
LANDING_VOLUME = "data"

LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/matches"

print(f"Catalog:        {CATALOG}")
print(f"Bronze Schema:  {BRONZE_SCHEMA}")
print(f"Landing Volume: {LANDING_VOLUME_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Bronze Schema & All 9 Delta Tables Exist

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# 1. bronze_match
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_match (
    match_id STRING NOT NULL,
    is_available BOOLEAN,
    map STRING,
    game_version STRING,
    game_length INT,
    game_start BIGINT,
    game_start_patched STRING,
    rounds_played INT,
    mode STRING,
    mode_id STRING,
    queue STRING,
    season_id STRING,
    platform STRING,
    premier_tournament_id STRING,
    premier_matchup_id STRING,
    region STRING,
    cluster STRING
)
USING DELTA
COMMENT 'Source-aligned match metadata: 1 row per match'
""")

# 2. bronze_team
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_team (
    match_id STRING NOT NULL,
    team_side STRING NOT NULL,
    has_won BOOLEAN,
    rounds_won INT,
    rounds_lost INT
)
USING DELTA
COMMENT 'Raw source-aligned team results: 1 row per team per match'
""")

# 3. bronze_player
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_player (
    match_id STRING NOT NULL,
    player_puuid STRING NOT NULL,
    player_name STRING,
    player_tag STRING,
    team STRING,
    level INT,
    character STRING,
    current_tier INT,
    current_tier_patched STRING,
    player_card STRING,
    player_title STRING,
    party_id STRING,
    session_playtime_minutes INT,
    session_playtime_seconds INT,
    session_playtime_milliseconds INT,
    afk_rounds DOUBLE,
    friendly_fire_incoming DOUBLE,
    friendly_fire_outgoing DOUBLE,
    rounds_in_spawn DOUBLE,
    platform_type STRING,
    os_name STRING,
    os_version STRING,
    x_casts INT,
    e_casts INT,
    q_casts INT,
    c_casts INT,
    score INT,
    kills INT,
    deaths INT,
    assists INT,
    bodyshots INT,
    headshots INT,
    legshots INT,
    spent_overall INT,
    spent_average DOUBLE,
    loadout_value_overall INT,
    loadout_value_average DOUBLE,
    damage_made INT,
    damage_received INT
)
USING DELTA
COMMENT 'Raw source-aligned player stats: 1 row per player per match'
""")

# 4. bronze_round
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_round (
    match_id STRING NOT NULL,
    round_number INT NOT NULL,
    winning_team STRING,
    end_type STRING,
    bomb_planted BOOLEAN,
    bomb_defused BOOLEAN
)
USING DELTA
COMMENT 'Raw source-aligned round summaries: 1 row per round per match'
""")

# 5. bronze_round_player_stats
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_round_player_stats (
    match_id STRING NOT NULL,
    round_number INT NOT NULL,
    player_puuid STRING NOT NULL,
    player_display_name STRING,
    player_team STRING,
    kills INT,
    deaths INT,
    assists INT,
    score INT,
    damage INT,
    bodyshots INT,
    headshots INT,
    legshots INT,
    loadout_value INT,
    remaining_credits INT,
    spent_credits INT,
    weapon_id STRING,
    weapon_name STRING,
    armor_id STRING,
    armor_name STRING,
    c_casts INT,
    q_casts INT,
    e_casts INT,
    x_casts INT,
    was_afk BOOLEAN,
    was_penalized BOOLEAN,
    stayed_in_spawn BOOLEAN,
    was_alive BOOLEAN
)
USING DELTA
COMMENT 'Source-aligned round-level player statistics: 1 row per player per round per match'
""")

# 6. bronze_kill_event
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_kill_event (
    match_id STRING NOT NULL,
    kill_event_sequence INT NOT NULL,
    round_number INT,
    kill_time_in_round INT,
    kill_time_in_match INT,
    killer_puuid STRING,
    killer_display_name STRING,
    killer_team STRING,
    victim_puuid STRING,
    victim_display_name STRING,
    victim_team STRING,
    victim_death_location STRUCT<x: DOUBLE, y: DOUBLE>,
    damage_weapon_id STRING,
    damage_weapon_name STRING,
    secondary_fire_mode BOOLEAN,
    assist_puuids ARRAY<STRING>,
    assistants ARRAY<STRUCT<assistant_puuid: STRING, assistant_display_name: STRING, assistant_team: STRING>>,
    player_locations ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>>
)
USING DELTA
COMMENT 'Source-aligned kill events from canonical kills[] array'
""")

# 7. bronze_damage_event
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_damage_event (
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

# 8. bronze_plant_event
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_plant_event (
    match_id STRING NOT NULL,
    round_number INT NOT NULL,
    plant_event_sequence INT NOT NULL,
    player_puuid STRING,
    player_display_name STRING,
    player_team STRING,
    plant_site STRING,
    plant_time_in_round INT,
    plant_location STRUCT<x: DOUBLE, y: DOUBLE>,
    player_locations_on_plant ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>>
)
USING DELTA
COMMENT 'Source-aligned spike plant events from rounds[].plant_events'
""")

# 9. bronze_defuse_event
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.bronze_defuse_event (
    match_id STRING NOT NULL,
    round_number INT NOT NULL,
    defuse_event_sequence INT NOT NULL,
    player_puuid STRING,
    player_display_name STRING,
    player_team STRING,
    defuse_site STRING,
    defuse_time_in_round INT,
    defuse_location STRUCT<x: DOUBLE, y: DOUBLE>,
    player_locations_on_defuse ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>>
)
USING DELTA
COMMENT 'Source-aligned spike defuse events from rounds[].defuse_events'
""")

print("Successfully verified all 9 Bronze Delta table DDLs.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Load Landing Match JSONs

# COMMAND ----------
json_files = glob.glob(f"{LANDING_VOLUME_PATH}/*/*/*/*.json")
if not json_files:
    json_files = glob.glob(f"{LANDING_VOLUME_PATH}/**/*.json", recursive=True)

print(f"Discovered {len(json_files)} landing match file(s).")

matches_data = []
for file_path in json_files:
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

unique_matches = {}
for m in matches_data:
    mid = (m.get("metadata") or {}).get("matchid")
    if mid and mid not in unique_matches:
        unique_matches[mid] = m

matches_to_process = list(unique_matches.values())
print(f"Loaded {len(matches_to_process)} unique match(es) for Bronze transformation.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Extract Entities According to Strict Grains

# COMMAND ----------
match_rows = []
team_rows = []
player_rows = []
round_rows = []
pstat_rows = []
kill_rows = []
damage_rows = []
plant_rows = []
defuse_rows = []

for m in matches_to_process:
    meta = m.get("metadata") or {}
    match_id = meta.get("matchid")
    if not match_id:
        continue

    # 1. bronze_match row
    premier_info = meta.get("premier_info") or {}
    match_rows.append({
        "match_id": match_id,
        "is_available": m.get("is_available"),
        "map": meta.get("map"),
        "game_version": meta.get("game_version"),
        "game_length": meta.get("game_length"),
        "game_start": meta.get("game_start"),
        "game_start_patched": meta.get("game_start_patched"),
        "rounds_played": meta.get("rounds_played"),
        "mode": meta.get("mode"),
        "mode_id": meta.get("mode_id"),
        "queue": meta.get("queue"),
        "season_id": meta.get("season_id"),
        "platform": meta.get("platform"),
        "premier_tournament_id": premier_info.get("tournament_id"),
        "premier_matchup_id": premier_info.get("matchup_id"),
        "region": meta.get("region"),
        "cluster": meta.get("cluster")
    })

    # 2. bronze_team rows (Red and Blue)
    teams_dict = m.get("teams") or {}
    for side in ["red", "blue"]:
        t_info = teams_dict.get(side) or {}
        team_rows.append({
            "match_id": match_id,
            "team_side": side.capitalize(),
            "has_won": t_info.get("has_won"),
            "rounds_won": t_info.get("rounds_won"),
            "rounds_lost": t_info.get("rounds_lost")
        })

    # 3. bronze_player rows (from all_players[])
    all_players = (m.get("players") or {}).get("all_players") or []
    for p in all_players:
        puuid = p.get("puuid")
        if not puuid:
            continue

        session_pt = p.get("session_playtime") or {}
        behavior = p.get("behavior") or {}
        friendly_fire = behavior.get("friendly_fire") or {}
        platform = p.get("platform") or {}
        os_info = platform.get("os") or {}
        ability_casts = p.get("ability_casts") or {}
        stats = p.get("stats") or {}
        economy = p.get("economy") or {}
        spent = economy.get("spent") or {}
        loadout_val = economy.get("loadout_value") or {}

        def get_cast(key_singular: str, key_plural: str):
            val = ability_casts.get(key_singular)
            return val if val is not None else ability_casts.get(key_plural)

        player_rows.append({
            "match_id": match_id,
            "player_puuid": puuid,
            "player_name": p.get("name"),
            "player_tag": p.get("tag"),
            "team": p.get("team"),
            "level": p.get("level"),
            "character": p.get("character"),
            "current_tier": p.get("currenttier"),
            "current_tier_patched": p.get("currenttier_patched"),
            "player_card": p.get("player_card"),
            "player_title": p.get("player_title"),
            "party_id": p.get("party_id"),
            "session_playtime_minutes": session_pt.get("minutes"),
            "session_playtime_seconds": session_pt.get("seconds"),
            "session_playtime_milliseconds": session_pt.get("milliseconds"),
            "afk_rounds": float(behavior.get("afk_rounds")) if behavior.get("afk_rounds") is not None else None,
            "friendly_fire_incoming": float(friendly_fire.get("incoming")) if friendly_fire.get("incoming") is not None else None,
            "friendly_fire_outgoing": float(friendly_fire.get("outgoing")) if friendly_fire.get("outgoing") is not None else None,
            "rounds_in_spawn": float(behavior.get("rounds_in_spawn")) if behavior.get("rounds_in_spawn") is not None else None,
            "platform_type": platform.get("type"),
            "os_name": os_info.get("name"),
            "os_version": os_info.get("version"),
            "x_casts": get_cast("x_cast", "x_casts"),
            "e_casts": get_cast("e_cast", "e_casts"),
            "q_casts": get_cast("q_cast", "q_casts"),
            "c_casts": get_cast("c_cast", "c_casts"),
            "score": stats.get("score"),
            "kills": stats.get("kills"),
            "deaths": stats.get("deaths"),
            "assists": stats.get("assists"),
            "bodyshots": stats.get("bodyshots"),
            "headshots": stats.get("headshots"),
            "legshots": stats.get("legshots"),
            "spent_overall": spent.get("overall"),
            "spent_average": float(spent.get("average")) if spent.get("average") is not None else None,
            "loadout_value_overall": loadout_val.get("overall"),
            "loadout_value_average": float(loadout_val.get("average")) if loadout_val.get("average") is not None else None,
            "damage_made": p.get("damage_made"),
            "damage_received": p.get("damage_received")
        })

    # 4. bronze_round, bronze_round_player_stats, bronze_damage_event, bronze_plant_event, bronze_defuse_event
    rounds_list = m.get("rounds") or []
    for r_idx, r in enumerate(rounds_list):
        round_number = r_idx + 1

        # bronze_round
        round_rows.append({
            "match_id": match_id,
            "round_number": round_number,
            "winning_team": r.get("winning_team"),
            "end_type": r.get("end_type"),
            "bomb_planted": r.get("bomb_planted"),
            "bomb_defused": r.get("bomb_defused")
        })

        # 5. bronze_round_player_stats & 7. bronze_damage_event
        pstats = r.get("player_stats") or []
        for ps in pstats:
            p_puuid = ps.get("player_puuid")
            if not p_puuid:
                continue

            econ = ps.get("economy") or {}
            w_info = econ.get("weapon") or {}
            arm_info = econ.get("armor") or {}
            casts = ps.get("ability_casts") or {}

            pstat_rows.append({
                "match_id": match_id,
                "round_number": round_number,
                "player_puuid": p_puuid,
                "player_display_name": ps.get("player_display_name"),
                "player_team": ps.get("player_team"),
                "kills": ps.get("kills"),
                "deaths": ps.get("deaths"),
                "assists": ps.get("assists"),
                "score": ps.get("score"),
                "damage": ps.get("damage"),
                "bodyshots": ps.get("bodyshots"),
                "headshots": ps.get("headshots"),
                "legshots": ps.get("legshots"),
                "loadout_value": econ.get("loadout_value"),
                "remaining_credits": econ.get("remaining"),
                "spent_credits": econ.get("spent"),
                "weapon_id": w_info.get("id"),
                "weapon_name": w_info.get("name"),
                "armor_id": arm_info.get("id"),
                "armor_name": arm_info.get("name"),
                "c_casts": casts.get("c_casts"),
                "q_casts": casts.get("q_casts"),
                "e_casts": casts.get("e_casts"),
                "x_casts": casts.get("x_casts"),
                "was_afk": ps.get("was_afk"),
                "was_penalized": ps.get("was_penalized"),
                "stayed_in_spawn": ps.get("stayed_in_spawn"),
                "was_alive": ps.get("was_alive")
            })

            # damage events for this attacker in this round
            for d_seq, de in enumerate(ps.get("damage_events") or []):
                damage_rows.append({
                    "match_id": match_id,
                    "round_number": round_number,
                    "damage_event_sequence": d_seq,
                    "attacker_puuid": p_puuid,
                    "attacker_display_name": ps.get("player_display_name"),
                    "attacker_team": ps.get("player_team"),
                    "receiver_puuid": de.get("receiver_puuid"),
                    "receiver_display_name": de.get("receiver_display_name"),
                    "receiver_team": de.get("receiver_team"),
                    "damage": de.get("damage"),
                    "bodyshots": de.get("bodyshots"),
                    "headshots": de.get("headshots"),
                    "legshots": de.get("legshots")
                })

        # 8. bronze_plant_event
        pe_raw = r.get("plant_events")
        if pe_raw:
            events_list = pe_raw if isinstance(pe_raw, list) else [pe_raw]
            for p_seq, pe in enumerate(events_list):
                if isinstance(pe, dict):
                    planted_by = pe.get("planted_by")
                    if planted_by or pe.get("plant_site") or pe.get("plant_time_in_round") is not None:
                        p_by = planted_by if isinstance(planted_by, dict) else {}
                        loc_raw = pe.get("plant_location") or {}
                        plant_loc = {
                            "x": float(loc_raw["x"]) if "x" in loc_raw and loc_raw["x"] is not None else None,
                            "y": float(loc_raw["y"]) if "y" in loc_raw and loc_raw["y"] is not None else None
                        } if loc_raw else None

                        plocs_raw = pe.get("player_locations_on_plant") or []
                        plocs = []
                        for pl in plocs_raw:
                            if isinstance(pl, dict):
                                l_item = pl.get("location") or {}
                                plocs.append({
                                    "player_puuid": pl.get("player_puuid"),
                                    "player_display_name": pl.get("player_display_name"),
                                    "player_team": pl.get("player_team"),
                                    "location": {
                                        "x": float(l_item["x"]) if "x" in l_item and l_item["x"] is not None else None,
                                        "y": float(l_item["y"]) if "y" in l_item and l_item["y"] is not None else None
                                    },
                                    "view_radians": float(pl["view_radians"]) if "view_radians" in pl and pl["view_radians"] is not None else None
                                })

                        plant_rows.append({
                            "match_id": match_id,
                            "round_number": round_number,
                            "plant_event_sequence": p_seq,
                            "player_puuid": p_by.get("puuid"),
                            "player_display_name": p_by.get("display_name"),
                            "player_team": p_by.get("team"),
                            "plant_site": pe.get("plant_site"),
                            "plant_time_in_round": pe.get("plant_time_in_round"),
                            "plant_location": plant_loc,
                            "player_locations_on_plant": plocs
                        })

        # 9. bronze_defuse_event
        de_raw = r.get("defuse_events")
        if de_raw:
            events_list = de_raw if isinstance(de_raw, list) else [de_raw]
            for d_seq, de in enumerate(events_list):
                if isinstance(de, dict):
                    defused_by = de.get("defused_by")
                    if defused_by or de.get("defuse_site") or de.get("defuse_time_in_round") is not None:
                        d_by = defused_by if isinstance(defused_by, dict) else {}
                        loc_raw = de.get("defuse_location") or {}
                        defuse_loc = {
                            "x": float(loc_raw["x"]) if "x" in loc_raw and loc_raw["x"] is not None else None,
                            "y": float(loc_raw["y"]) if "y" in loc_raw and loc_raw["y"] is not None else None
                        } if loc_raw else None

                        plocs_raw = de.get("player_locations_on_defuse") or []
                        plocs = []
                        for pl in plocs_raw:
                            if isinstance(pl, dict):
                                l_item = pl.get("location") or {}
                                plocs.append({
                                    "player_puuid": pl.get("player_puuid"),
                                    "player_display_name": pl.get("player_display_name"),
                                    "player_team": pl.get("player_team"),
                                    "location": {
                                        "x": float(l_item["x"]) if "x" in l_item and l_item["x"] is not None else None,
                                        "y": float(l_item["y"]) if "y" in l_item and l_item["y"] is not None else None
                                    },
                                    "view_radians": float(pl["view_radians"]) if "view_radians" in pl and pl["view_radians"] is not None else None
                                })

                        defuse_rows.append({
                            "match_id": match_id,
                            "round_number": round_number,
                            "defuse_event_sequence": d_seq,
                            "player_puuid": d_by.get("puuid"),
                            "player_display_name": d_by.get("display_name"),
                            "player_team": d_by.get("team"),
                            "defuse_site": de.get("defuse_site"),
                            "defuse_time_in_round": de.get("defuse_time_in_round"),
                            "defuse_location": defuse_loc,
                            "player_locations_on_defuse": plocs
                        })

    # 6. bronze_kill_event (from canonical top-level kills[])
    for k_seq, k in enumerate(m.get("kills") or []):
        r_field = k.get("round")
        round_number = (int(r_field) + 1) if r_field is not None else None

        assistants_raw = k.get("assistants") or []
        assist_puuids = [
            a.get("assistant_puuid")
            for a in assistants_raw
            if isinstance(a, dict) and a.get("assistant_puuid")
        ]
        assistants_structs = [
            {
                "assistant_puuid": a.get("assistant_puuid"),
                "assistant_display_name": a.get("assistant_display_name"),
                "assistant_team": a.get("assistant_team")
            }
            for a in assistants_raw if isinstance(a, dict)
        ]

        player_locs_raw = k.get("player_locations_on_kill") or []
        player_locs = []
        for pl in player_locs_raw:
            if isinstance(pl, dict):
                loc = pl.get("location") or {}
                player_locs.append({
                    "player_puuid": pl.get("player_puuid"),
                    "player_display_name": pl.get("player_display_name"),
                    "player_team": pl.get("player_team"),
                    "location": {
                        "x": float(loc["x"]) if "x" in loc and loc["x"] is not None else None,
                        "y": float(loc["y"]) if "y" in loc and loc["y"] is not None else None
                    },
                    "view_radians": float(pl["view_radians"]) if "view_radians" in pl and pl["view_radians"] is not None else None
                })

        v_loc = k.get("victim_death_location") or {}
        death_loc = {
            "x": float(v_loc["x"]) if "x" in v_loc and v_loc["x"] is not None else None,
            "y": float(v_loc["y"]) if "y" in v_loc and v_loc["y"] is not None else None
        } if v_loc else None

        kill_rows.append({
            "match_id": match_id,
            "kill_event_sequence": k_seq,
            "round_number": round_number,
            "kill_time_in_round": k.get("kill_time_in_round"),
            "kill_time_in_match": k.get("kill_time_in_match"),
            "killer_puuid": k.get("killer_puuid"),
            "killer_display_name": k.get("killer_display_name"),
            "killer_team": k.get("killer_team"),
            "victim_puuid": k.get("victim_puuid"),
            "victim_display_name": k.get("victim_display_name"),
            "victim_team": k.get("victim_team"),
            "victim_death_location": death_loc,
            "damage_weapon_id": k.get("damage_weapon_id"),
            "damage_weapon_name": k.get("damage_weapon_name"),
            "secondary_fire_mode": k.get("secondary_fire_mode"),
            "assist_puuids": assist_puuids,
            "assistants": assistants_structs,
            "player_locations": player_locs
        })

print(f"Extracted: {len(match_rows)} matches, {len(team_rows)} teams, {len(player_rows)} players, {len(round_rows)} rounds, {len(pstat_rows)} round_player_stats, {len(kill_rows)} kills, {len(damage_rows)} damage_events, {len(plant_rows)} plants, {len(defuse_rows)} defuses.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Define Schemas & Create DataFrames

# COMMAND ----------
loc_struct = StructType([
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True)
])

player_loc_struct = StructType([
    StructField("player_puuid", StringType(), True),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("location", loc_struct, True),
    StructField("view_radians", DoubleType(), True)
])

assistant_struct = StructType([
    StructField("assistant_puuid", StringType(), True),
    StructField("assistant_display_name", StringType(), True),
    StructField("assistant_team", StringType(), True)
])

schema_match = StructType([
    StructField("match_id", StringType(), False),
    StructField("is_available", BooleanType(), True),
    StructField("map", StringType(), True),
    StructField("game_version", StringType(), True),
    StructField("game_length", IntegerType(), True),
    StructField("game_start", LongType(), True),
    StructField("game_start_patched", StringType(), True),
    StructField("rounds_played", IntegerType(), True),
    StructField("mode", StringType(), True),
    StructField("mode_id", StringType(), True),
    StructField("queue", StringType(), True),
    StructField("season_id", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("premier_tournament_id", StringType(), True),
    StructField("premier_matchup_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("cluster", StringType(), True)
])

schema_team = StructType([
    StructField("match_id", StringType(), False),
    StructField("team_side", StringType(), False),
    StructField("has_won", BooleanType(), True),
    StructField("rounds_won", IntegerType(), True),
    StructField("rounds_lost", IntegerType(), True)
])

schema_player = StructType([
    StructField("match_id", StringType(), False),
    StructField("player_puuid", StringType(), False),
    StructField("player_name", StringType(), True),
    StructField("player_tag", StringType(), True),
    StructField("team", StringType(), True),
    StructField("level", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("current_tier", IntegerType(), True),
    StructField("current_tier_patched", StringType(), True),
    StructField("player_card", StringType(), True),
    StructField("player_title", StringType(), True),
    StructField("party_id", StringType(), True),
    StructField("session_playtime_minutes", IntegerType(), True),
    StructField("session_playtime_seconds", IntegerType(), True),
    StructField("session_playtime_milliseconds", IntegerType(), True),
    StructField("afk_rounds", DoubleType(), True),
    StructField("friendly_fire_incoming", DoubleType(), True),
    StructField("friendly_fire_outgoing", DoubleType(), True),
    StructField("rounds_in_spawn", DoubleType(), True),
    StructField("platform_type", StringType(), True),
    StructField("os_name", StringType(), True),
    StructField("os_version", StringType(), True),
    StructField("x_casts", IntegerType(), True),
    StructField("e_casts", IntegerType(), True),
    StructField("q_casts", IntegerType(), True),
    StructField("c_casts", IntegerType(), True),
    StructField("score", IntegerType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("bodyshots", IntegerType(), True),
    StructField("headshots", IntegerType(), True),
    StructField("legshots", IntegerType(), True),
    StructField("spent_overall", IntegerType(), True),
    StructField("spent_average", DoubleType(), True),
    StructField("loadout_value_overall", IntegerType(), True),
    StructField("loadout_value_average", DoubleType(), True),
    StructField("damage_made", IntegerType(), True),
    StructField("damage_received", IntegerType(), True)
])

schema_round = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("winning_team", StringType(), True),
    StructField("end_type", StringType(), True),
    StructField("bomb_planted", BooleanType(), True),
    StructField("bomb_defused", BooleanType(), True)
])

schema_pstats = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("player_puuid", StringType(), False),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("score", IntegerType(), True),
    StructField("damage", IntegerType(), True),
    StructField("bodyshots", IntegerType(), True),
    StructField("headshots", IntegerType(), True),
    StructField("legshots", IntegerType(), True),
    StructField("loadout_value", IntegerType(), True),
    StructField("remaining_credits", IntegerType(), True),
    StructField("spent_credits", IntegerType(), True),
    StructField("weapon_id", StringType(), True),
    StructField("weapon_name", StringType(), True),
    StructField("armor_id", StringType(), True),
    StructField("armor_name", StringType(), True),
    StructField("c_casts", IntegerType(), True),
    StructField("q_casts", IntegerType(), True),
    StructField("e_casts", IntegerType(), True),
    StructField("x_casts", IntegerType(), True),
    StructField("was_afk", BooleanType(), True),
    StructField("was_penalized", BooleanType(), True),
    StructField("stayed_in_spawn", BooleanType(), True),
    StructField("was_alive", BooleanType(), True)
])

schema_kill = StructType([
    StructField("match_id", StringType(), False),
    StructField("kill_event_sequence", IntegerType(), False),
    StructField("round_number", IntegerType(), True),
    StructField("kill_time_in_round", IntegerType(), True),
    StructField("kill_time_in_match", IntegerType(), True),
    StructField("killer_puuid", StringType(), True),
    StructField("killer_display_name", StringType(), True),
    StructField("killer_team", StringType(), True),
    StructField("victim_puuid", StringType(), True),
    StructField("victim_display_name", StringType(), True),
    StructField("victim_team", StringType(), True),
    StructField("victim_death_location", loc_struct, True),
    StructField("damage_weapon_id", StringType(), True),
    StructField("damage_weapon_name", StringType(), True),
    StructField("secondary_fire_mode", BooleanType(), True),
    StructField("assist_puuids", ArrayType(StringType()), True),
    StructField("assistants", ArrayType(assistant_struct), True),
    StructField("player_locations", ArrayType(player_loc_struct), True)
])

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

schema_plant = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("plant_event_sequence", IntegerType(), False),
    StructField("player_puuid", StringType(), True),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("plant_site", StringType(), True),
    StructField("plant_time_in_round", IntegerType(), True),
    StructField("plant_location", loc_struct, True),
    StructField("player_locations_on_plant", ArrayType(player_loc_struct), True)
])

schema_defuse = StructType([
    StructField("match_id", StringType(), False),
    StructField("round_number", IntegerType(), False),
    StructField("defuse_event_sequence", IntegerType(), False),
    StructField("player_puuid", StringType(), True),
    StructField("player_display_name", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("defuse_site", StringType(), True),
    StructField("defuse_time_in_round", IntegerType(), True),
    StructField("defuse_location", loc_struct, True),
    StructField("player_locations_on_defuse", ArrayType(player_loc_struct), True)
])

spark.createDataFrame(match_rows, schema=schema_match).createOrReplaceTempView("tv_bronze_match")
spark.createDataFrame(team_rows, schema=schema_team).createOrReplaceTempView("tv_bronze_team")
spark.createDataFrame(player_rows, schema=schema_player).createOrReplaceTempView("tv_bronze_player")
spark.createDataFrame(round_rows, schema=schema_round).createOrReplaceTempView("tv_bronze_round")
spark.createDataFrame(pstat_rows, schema=schema_pstats).createOrReplaceTempView("tv_bronze_round_player_stats")
spark.createDataFrame(kill_rows, schema=schema_kill).createOrReplaceTempView("tv_bronze_kill_event")
spark.createDataFrame(damage_rows, schema=schema_damage).createOrReplaceTempView("tv_bronze_damage_event")
spark.createDataFrame(plant_rows, schema=schema_plant).createOrReplaceTempView("tv_bronze_plant_event")
spark.createDataFrame(defuse_rows, schema=schema_defuse).createOrReplaceTempView("tv_bronze_defuse_event")

print("Created temp views with explicit schemas for all 9 tables.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Execute Idempotent Delta MERGE for All Nine Tables

# COMMAND ----------
# 1. MERGE bronze_match
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_match AS target
USING tv_bronze_match AS source
ON target.match_id = source.match_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_match successfully.")

# 2. MERGE bronze_team
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_team AS target
USING tv_bronze_team AS source
ON target.match_id = source.match_id 
AND target.team_side = source.team_side
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_team successfully.")

# 3. MERGE bronze_player
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_player AS target
USING tv_bronze_player AS source
ON target.match_id = source.match_id 
AND target.player_puuid = source.player_puuid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_player successfully.")

# 4. MERGE bronze_round
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_round AS target
USING tv_bronze_round AS source
ON target.match_id = source.match_id 
AND target.round_number = source.round_number
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_round successfully.")

# 5. MERGE bronze_round_player_stats
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_round_player_stats AS target
USING tv_bronze_round_player_stats AS source
ON target.match_id = source.match_id 
AND target.round_number = source.round_number
AND target.player_puuid = source.player_puuid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_round_player_stats successfully.")

# 6. MERGE bronze_kill_event
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_kill_event AS target
USING tv_bronze_kill_event AS source
ON target.match_id = source.match_id 
AND target.kill_event_sequence = source.kill_event_sequence
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_kill_event successfully.")

# 7. MERGE bronze_damage_event
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_damage_event AS target
USING tv_bronze_damage_event AS source
ON target.match_id = source.match_id 
AND target.round_number = source.round_number
AND target.attacker_puuid = source.attacker_puuid
AND target.damage_event_sequence = source.damage_event_sequence
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_damage_event successfully.")

# 8. MERGE bronze_plant_event
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_plant_event AS target
USING tv_bronze_plant_event AS source
ON target.match_id = source.match_id 
AND target.round_number = source.round_number
AND target.plant_event_sequence = source.plant_event_sequence
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_plant_event successfully.")

# 9. MERGE bronze_defuse_event
spark.sql(f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.bronze_defuse_event AS target
USING tv_bronze_defuse_event AS source
ON target.match_id = source.match_id 
AND target.round_number = source.round_number
AND target.defuse_event_sequence = source.defuse_event_sequence
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Merged bronze_defuse_event successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Data Quality & Uniqueness Validation Checks

# COMMAND ----------
print("=" * 60)
print("DATA QUALITY AUDIT & ASSERTIONS (ALL 9 TABLES)")
print("=" * 60)

# Check 1: bronze_match
m_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_match").collect()[0]
assert m_stats["nulls"] == 0 and m_stats["tot"] == m_stats["dist"], "DQ ERROR in bronze_match"

# Check 2: bronze_team
t_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, team_side) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_team").collect()[0]
assert t_stats["nulls"] == 0 and t_stats["tot"] == t_stats["dist"], "DQ ERROR in bronze_team"

# Check 3: bronze_player
p_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, player_puuid) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_player").collect()[0]
assert p_stats["nulls"] == 0 and p_stats["tot"] == p_stats["dist"], "DQ ERROR in bronze_player"

# Check 4: bronze_round
r_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, round_number) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_round").collect()[0]
assert r_stats["nulls"] == 0 and r_stats["tot"] == r_stats["dist"], "DQ ERROR in bronze_round"

# Check 5: bronze_round_player_stats
ps_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, round_number, player_puuid) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_round_player_stats").collect()[0]
assert ps_stats["nulls"] == 0 and ps_stats["tot"] == ps_stats["dist"], "DQ ERROR in bronze_round_player_stats"

# Check 6: bronze_kill_event
k_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, kill_event_sequence) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_kill_event").collect()[0]
assert k_stats["nulls"] == 0 and k_stats["tot"] == k_stats["dist"], "DQ ERROR in bronze_kill_event"

# Check 7: bronze_damage_event
d_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, round_number, attacker_puuid, damage_event_sequence) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_damage_event").collect()[0]
assert d_stats["nulls"] == 0 and d_stats["tot"] == d_stats["dist"], "DQ ERROR in bronze_damage_event"

# Check 8: bronze_plant_event
pl_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, round_number, plant_event_sequence) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_plant_event").collect()[0]
assert pl_stats["nulls"] == 0 and pl_stats["tot"] == pl_stats["dist"], "DQ ERROR in bronze_plant_event"

# Check 9: bronze_defuse_event
df_stats = spark.sql(f"SELECT COUNT(*) AS tot, COUNT(DISTINCT match_id, round_number, defuse_event_sequence) AS dist, COUNT(CASE WHEN match_id IS NULL THEN 1 END) AS nulls FROM {CATALOG}.{BRONZE_SCHEMA}.bronze_defuse_event").collect()[0]
assert df_stats["nulls"] == 0 and df_stats["tot"] == df_stats["dist"], "DQ ERROR in bronze_defuse_event"

print("ALL DATA QUALITY & COMPOSITE KEY UNIQUENESS ASSERTIONS PASSED (9/9 TABLES)!")
