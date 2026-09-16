"""Unit and data quality tests for the first four Bronze Delta tables."""

import json
import os
import sqlite3
import pytest


def extract_bronze_entities(matches_data):
    """Extracts rows for bronze_match, bronze_team, bronze_player, bronze_round."""
    match_rows = []
    team_rows = []
    player_rows = []
    round_rows = []

    for m in matches_data:
        meta = m.get("metadata") or {}
        match_id = meta.get("matchid")
        if not match_id:
            continue

        # 1. bronze_match
        premier = meta.get("premier_info") or {}
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
            "premier_tournament_id": premier.get("tournament_id"),
            "premier_matchup_id": premier.get("matchup_id"),
            "region": meta.get("region"),
            "cluster": meta.get("cluster")
        })

        # 2. bronze_team
        teams = m.get("teams") or {}
        for side in ["red", "blue"]:
            t = teams.get(side) or {}
            team_rows.append({
                "match_id": match_id,
                "team_side": side.capitalize(),
                "has_won": t.get("has_won"),
                "rounds_won": t.get("rounds_won"),
                "rounds_lost": t.get("rounds_lost")
            })

        # 3. bronze_player
        players = (m.get("players") or {}).get("all_players") or []
        for p in players:
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

        # 4. bronze_round
        rounds = m.get("rounds") or []
        for idx, r in enumerate(rounds):
            round_rows.append({
                "match_id": match_id,
                "round_number": idx + 1,
                "winning_team": r.get("winning_team"),
                "end_type": r.get("end_type"),
                "bomb_planted": r.get("bomb_planted"),
                "bomb_defused": r.get("bomb_defused")
            })

    return match_rows, team_rows, player_rows, round_rows


@pytest.fixture
def raw_matches():
    path = os.path.join(os.path.dirname(__file__), "..", "..", "matches.json")
    if not os.path.exists(path):
        pytest.skip("matches.json not found")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["data"]


def test_bronze_extraction_cardinality(raw_matches):
    match_rows, team_rows, player_rows, round_rows = extract_bronze_entities(raw_matches)

    assert len(match_rows) == 2, "Expected 1 row per match in bronze_match"
    assert len(team_rows) == 4, "Expected 2 rows per match in bronze_team"
    assert len(player_rows) == 20, "Expected 10 rows per match in bronze_player"

    # Match 0 had 18 rounds, Match 1 had 14 rounds -> 32 total
    expected_rounds = sum(m["metadata"]["rounds_played"] for m in raw_matches)
    assert len(round_rows) == expected_rounds == 32, "Expected rounds_played rows per match in bronze_round"


def test_bronze_data_quality_assertions(raw_matches):
    match_rows, team_rows, player_rows, round_rows = extract_bronze_entities(raw_matches)

    # 1. bronze_match assertions
    match_ids = [r["match_id"] for r in match_rows]
    assert all(mid is not None and len(mid.strip()) > 0 for mid in match_ids)
    assert len(match_ids) == len(set(match_ids)), "match_id must be unique"

    # 2. bronze_team assertions
    for t in team_rows:
        assert t["match_id"] is not None
        assert t["team_side"] in ("Red", "Blue"), f"Invalid team_side: {t['team_side']}"
    team_keys = [(t["match_id"], t["team_side"]) for t in team_rows]
    assert len(team_keys) == len(set(team_keys)), "Composite key (match_id, team_side) must be unique"

    # 3. bronze_player assertions
    for p in player_rows:
        assert p["match_id"] is not None
        assert p["player_puuid"] is not None
        assert p["team"] in ("Red", "Blue")
    player_keys = [(p["match_id"], p["player_puuid"]) for p in player_rows]
    assert len(player_keys) == len(set(player_keys)), "Composite key (match_id, player_puuid) must be unique"

    # 4. bronze_round assertions
    for r in round_rows:
        assert r["match_id"] is not None
        assert r["round_number"] >= 1
        assert r["winning_team"] in ("Red", "Blue")
    round_keys = [(r["match_id"], r["round_number"]) for r in round_rows]
    assert len(round_keys) == len(set(round_keys)), "Composite key (match_id, round_number) must be unique"


def test_bronze_idempotency_simulation(raw_matches):
    """Simulates loading twice into tables with primary keys to verify idempotency."""
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    # DDL
    cur.execute("CREATE TABLE bronze_match (match_id TEXT PRIMARY KEY, map TEXT, rounds_played INT)")
    cur.execute("CREATE TABLE bronze_team (match_id TEXT, team_side TEXT, rounds_won INT, PRIMARY KEY(match_id, team_side))")
    cur.execute("CREATE TABLE bronze_player (match_id TEXT, player_puuid TEXT, player_name TEXT, kills INT, PRIMARY KEY(match_id, player_puuid))")
    cur.execute("CREATE TABLE bronze_round (match_id TEXT, round_number INT, winning_team TEXT, PRIMARY KEY(match_id, round_number))")

    match_rows, team_rows, player_rows, round_rows = extract_bronze_entities(raw_matches)

    def run_upsert():
        for m in match_rows:
            cur.execute("INSERT INTO bronze_match VALUES (?, ?, ?) ON CONFLICT(match_id) DO UPDATE SET map=excluded.map",
                        (m["match_id"], m["map"], m["rounds_played"]))
        for t in team_rows:
            cur.execute("INSERT INTO bronze_team VALUES (?, ?, ?) ON CONFLICT(match_id, team_side) DO UPDATE SET rounds_won=excluded.rounds_won",
                        (t["match_id"], t["team_side"], t["rounds_won"]))
        for p in player_rows:
            cur.execute("INSERT INTO bronze_player VALUES (?, ?, ?, ?) ON CONFLICT(match_id, player_puuid) DO UPDATE SET kills=excluded.kills",
                        (p["match_id"], p["player_puuid"], p["player_name"], p["kills"]))
        for r in round_rows:
            cur.execute("INSERT INTO bronze_round VALUES (?, ?, ?) ON CONFLICT(match_id, round_number) DO UPDATE SET winning_team=excluded.winning_team",
                        (r["match_id"], r["round_number"], r["winning_team"]))
        conn.commit()

    # Run 1
    run_upsert()
    c_m1 = cur.execute("SELECT COUNT(*) FROM bronze_match").fetchone()[0]
    c_t1 = cur.execute("SELECT COUNT(*) FROM bronze_team").fetchone()[0]
    c_p1 = cur.execute("SELECT COUNT(*) FROM bronze_player").fetchone()[0]
    c_r1 = cur.execute("SELECT COUNT(*) FROM bronze_round").fetchone()[0]

    assert c_m1 == 2
    assert c_t1 == 4
    assert c_p1 == 20
    assert c_r1 == 32

    # Run 2 (duplicate source processed)
    run_upsert()
    c_m2 = cur.execute("SELECT COUNT(*) FROM bronze_match").fetchone()[0]
    c_t2 = cur.execute("SELECT COUNT(*) FROM bronze_team").fetchone()[0]
    c_p2 = cur.execute("SELECT COUNT(*) FROM bronze_player").fetchone()[0]
    c_r2 = cur.execute("SELECT COUNT(*) FROM bronze_round").fetchone()[0]

    # Row counts must remain strictly identical
    assert c_m1 == c_m2 == 2
    assert c_t1 == c_t2 == 4
    assert c_p1 == c_p2 == 20
    assert c_r1 == c_r2 == 32
