"""Normalization engine converting diverse raw Valorant match formats into canonical Landing JSON."""

import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple


class NormalizationError(Exception):
    """Raised when a match cannot be normalized or fails canonical validation."""
    pass


def detect_source_format(raw_json: Any) -> str:
    """Detects whether a raw JSON object is a historical wrapper, API envelope, or bare match.
    
    Returns:
        'historical_wrapper' | 'api_envelope' | 'bare_match' | 'unknown'
    """
    if not isinstance(raw_json, dict):
        return "unknown"

    if "matchData" in raw_json and isinstance(raw_json["matchData"], dict):
        return "historical_wrapper"

    if "status" in raw_json and "data" in raw_json:
        return "api_envelope"

    if "metadata" in raw_json and "matchid" in (raw_json.get("metadata") or {}):
        return "bare_match"

    return "unknown"


def unwrap_matches(raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Unwraps the raw JSON container to extract individual raw match dictionaries.
    
    - 'historical_wrapper': extracts matchData.data
    - 'api_envelope': extracts data[] (or data if single dict)
    - 'bare_match': returns [raw_json]
    """
    fmt = detect_source_format(raw_json)

    if fmt == "historical_wrapper":
        inner_data = raw_json.get("matchData", {}).get("data")
        if isinstance(inner_data, dict):
            return [inner_data]
        elif isinstance(inner_data, list):
            return inner_data
        raise NormalizationError(f"Historical wrapper matchData.data is missing or invalid: {type(inner_data)}")

    elif fmt == "api_envelope":
        data = raw_json.get("data")
        if isinstance(data, list):
            return [m for m in data if isinstance(m, dict)]
        elif isinstance(data, dict):
            return [data]
        raise NormalizationError(f"API envelope data field must be a list or dict, got: {type(data)}")

    elif fmt == "bare_match":
        return [raw_json]

    keys_info = list(raw_json.keys()) if isinstance(raw_json, dict) else type(raw_json).__name__
    raise NormalizationError(f"Unsupported source format detected: {keys_info}")


def normalize_match(raw_match_dict: Dict[str, Any], source_type: Optional[str] = None) -> Dict[str, Any]:
    """Normalizes an individual raw match dictionary into the canonical Landing contract.
    
    Rules applied:
    - Rule A/B: Excludes source wrappers (hasWon, matchData, status, errors).
    - Rule C: Preserves is_available if present; sets to None (null) if missing.
              Never infers is_available from HTTP status.
    - Rule D: Renames player 'behaviour' -> 'behavior' (no duplicate keys).
    - Rule E: Normalizes player 'c_cast', 'q_cast', 'e_cast', 'x_cast' ->
              'c_casts', 'q_casts', 'e_casts', 'x_casts'.
    - Rule F: Normalizes team 'roaster' -> 'roster' (preserving value/null).
    - Rule G: Ensures top-level 'kills[]' exists:
              - If top-level 'kills' already present, preserves it.
              - If missing, extracts from rounds[].player_stats[].kill_events[],
                deduplicating events and deriving 0-based 'round' index if missing.
    - Rule H: Preserves all other source data without arbitrary changes or KPI calculations.
    """
    if not isinstance(raw_match_dict, dict):
        raise NormalizationError(f"Expected match to be a dict, got {type(raw_match_dict).__name__}")

    # Deep-copy to avoid mutating input
    match = copy.deepcopy(raw_match_dict)

    # Strip any top-level wrapper artifacts that might have leaked
    for key in ("hasWon", "matchData", "status", "errors"):
        match.pop(key, None)

    # Rule C: is_available
    if "is_available" in match:
        is_available = match["is_available"]
    else:
        is_available = None

    # Metadata
    metadata = match.get("metadata")
    if not isinstance(metadata, dict):
        raise NormalizationError("Missing or invalid 'metadata' dictionary in match")

    match_id = metadata.get("matchid")
    if not match_id or not isinstance(match_id, str) or not match_id.strip():
        raise NormalizationError("Match metadata is missing a valid 'matchid'")

    # Rule D & E: Players normalization
    players = match.get("players")
    if isinstance(players, dict):
        for player_group_key in ("all_players", "red", "blue"):
            group_list = players.get(player_group_key)
            if isinstance(group_list, list):
                for p in group_list:
                    if not isinstance(p, dict):
                        continue
                    # Rule D: behaviour -> behavior
                    if "behaviour" in p:
                        behavior_val = p.pop("behaviour")
                        if "behavior" not in p:
                            p["behavior"] = behavior_val
                    # Rule E: Ability casts (c_cast -> c_casts, etc.)
                    ability_casts = p.get("ability_casts")
                    if isinstance(ability_casts, dict):
                        for singular, plural in (
                            ("c_cast", "c_casts"),
                            ("q_cast", "q_casts"),
                            ("e_cast", "e_casts"),
                            ("x_cast", "x_casts"),
                        ):
                            if singular in ability_casts:
                                val = ability_casts.pop(singular)
                                if plural not in ability_casts:
                                    ability_casts[plural] = val

    # Rule F: Teams normalization (roaster -> roster)
    teams = match.get("teams")
    if isinstance(teams, dict):
        for side_key in ("red", "blue"):
            team_info = teams.get(side_key)
            if isinstance(team_info, dict):
                if "roaster" in team_info:
                    roaster_val = team_info.pop("roaster")
                    if "roster" not in team_info:
                        team_info["roster"] = roaster_val

    # Rule G: Kills normalization
    existing_kills = match.get("kills")
    if existing_kills is not None and isinstance(existing_kills, list):
        canonical_kills = existing_kills
    else:
        # Extract and flatten from rounds[].player_stats[].kill_events[]
        canonical_kills = []
        seen_kill_signatures: Set[Tuple[Any, ...]] = set()

        rounds_list = match.get("rounds")
        if isinstance(rounds_list, list):
            for round_idx, r in enumerate(rounds_list):
                if not isinstance(r, dict):
                    continue
                player_stats = r.get("player_stats")
                if not isinstance(player_stats, list):
                    continue
                for ps in player_stats:
                    if not isinstance(ps, dict):
                        continue
                    kill_events = ps.get("kill_events")
                    if not isinstance(kill_events, list):
                        continue
                    for ke in kill_events:
                        if not isinstance(ke, dict):
                            continue
                        ke_copy = copy.deepcopy(ke)

                        # Derive round from parent round if missing or null (0-based matching current API)
                        if "round" not in ke_copy or ke_copy["round"] is None:
                            ke_copy["round"] = round_idx

                        # Deduplicate by signature
                        sig = (
                            ke_copy.get("kill_time_in_match"),
                            ke_copy.get("kill_time_in_round"),
                            ke_copy.get("round"),
                            ke_copy.get("killer_puuid"),
                            ke_copy.get("victim_puuid"),
                        )
                        if sig not in seen_kill_signatures:
                            seen_kill_signatures.add(sig)
                            canonical_kills.append(ke_copy)

    # Construct canonical top-level object
    canonical_match = {
        "is_available": is_available,
        "metadata": metadata,
        "players": players if players is not None else {},
        "observers": match.get("observers", []),
        "coaches": match.get("coaches", []),
        "teams": teams if teams is not None else {},
        "rounds": match.get("rounds", []),
        "kills": canonical_kills,
    }

    # Preserve any other non-wrapper top-level fields present in source (e.g. premier_info)
    for k, v in match.items():
        if k not in canonical_match and k not in ("hasWon", "matchData", "status", "errors"):
            canonical_match[k] = v

    return canonical_match


def validate_canonical_match(match: Any) -> Tuple[bool, List[str]]:
    """Validates that a normalized match adheres to the Canonical Landing Contract.
    
    Returns:
        (is_valid: bool, errors: List[str])
    """
    errors = []

    if not isinstance(match, dict):
        return False, [f"Match must be a dictionary, got {type(match).__name__}"]

    # Prohibited wrapper leakage
    for forbidden in ("hasWon", "matchData", "status"):
        if forbidden in match:
            errors.append(f"Forbidden source wrapper field '{forbidden}' found at top level")

    # Required metadata
    metadata = match.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("Missing 'metadata' dictionary")
    else:
        matchid = metadata.get("matchid")
        if not matchid or not isinstance(matchid, str) or not matchid.strip():
            errors.append("Invalid or missing 'metadata.matchid'")
        game_start = metadata.get("game_start")
        if game_start is None:
            errors.append("Missing 'metadata.game_start'")

    # Canonical structures
    if "rounds" in match and not isinstance(match["rounds"], list):
        errors.append("'rounds' must be an array")
    if "kills" in match and not isinstance(match["kills"], list):
        errors.append("'kills' must be an array")
    if "players" in match and not isinstance(match["players"], dict):
        errors.append("'players' must be an object")
    if "teams" in match and not isinstance(match["teams"], dict):
        errors.append("'teams' must be an object")

    # Ensure no duplicate kill events exist
    kills = match.get("kills", [])
    if isinstance(kills, list):
        seen_sigs = set()
        for idx, k in enumerate(kills):
            if isinstance(k, dict):
                sig = (
                    k.get("kill_time_in_match"),
                    k.get("kill_time_in_round"),
                    k.get("round"),
                    k.get("killer_puuid"),
                    k.get("victim_puuid"),
                )
                if sig in seen_sigs and any(x is not None for x in sig):
                    errors.append(f"Duplicate kill event detected at kills[{idx}]: {sig}")
                seen_sigs.add(sig)

    return (len(errors) == 0, errors)
