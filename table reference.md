# Valorant Analytics Platform — Data Table Reference

This document serves as the single source of truth for all data tables currently in the Medallion platform. It outlines the schema, grain, keys, and descriptions for the **Landing Control Table** and all 9 **Bronze Source-Aligned Tables**.

As we implement the **Silver** and **Gold** layers, their schemas, grain, and lineage will be added to this document.

---

## Architecture Overview

`
Databricks Medallion Catalog: alorant
├── Schema: landing
│   └── Table: ingestion_control
│
├── Schema: ronze
│   ├── Table: bronze_match
│   ├── Table: bronze_team
│   ├── Table: bronze_player
│   ├── Table: bronze_round
│   ├── Table: bronze_round_player_stats
│   ├── Table: bronze_kill_event
│   ├── Table: bronze_damage_event
│   ├── Table: bronze_plant_event
│   └── Table: bronze_defuse_event
│
├── Schema: silver (Next Milestone)
└── Schema: gold   (Downstream Milestone)
`

---

## 1. Control & Metadata Layer (alorant.landing)

### alorant.landing.ingestion_control
* **Purpose:** Ensures match-level idempotency and incremental processing. Tracks whether every raw JSON file in Landing was successfully registered or encountered an error.
* **Grain:** 1 row per unique match.
* **Primary Key:** match_id

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| match_id | STRING | NO | Unique canonical match GUID (PK) |
| match_date | DATE | YES | Date of the match derived from game_start_patched (used for volume partitioning) |
| landing_path | STRING | YES | Absolute path to the canonical match JSON file in Unity Catalog Volume |
| ingestion_timestamp | TIMESTAMP | YES | UTC timestamp when the match was landed |
| ingestion_status | STRING | YES | Processing status: 'SUCCESS' or 'FAILED' |
| source | STRING | YES | Ingestion origin: 'api' or 'historical' |
| load_type | STRING | YES | Ingestion run type (e.g., 'batch', 'daily') |
| error_message | STRING | YES | Error stack or failure reason if status is 'FAILED' |

---

## 2. Bronze Source-Aligned Layer (alorant.bronze)

The Bronze layer preserves the exact structure and grain of the raw match payloads without applying business-specific derivations or filtering.

### 2.1 alorant.bronze.bronze_match
* **Purpose:** High-level match metadata, server cluster, queue mode, and duration.
* **Grain:** 1 row per match.
* **Primary Key:** match_id
* **Source Path:** data[].metadata

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| is_available | BOOLEAN | Whether full match stats are available in API |
| map | STRING | Map name (e.g., 'Bind', 'Ascent', 'Haven') |
| game_version | STRING | Valorant client/engine release version |
| game_length | INT | Total match duration in seconds |
| game_start | BIGINT | Epoch millisecond timestamp of match start |
| game_start_patched | STRING | Human-readable UTC timestamp of match start |
| 
ounds_played | INT | Total rounds played in the match |
| mode | STRING | Game mode (e.g., 'Competitive', 'Custom') |
| mode_id | STRING | Internal Riot mode identifier |
| queue | STRING | Matchmaking queue name |
| season_id | STRING | Riot Act/Episode season UUID |
| platform | STRING | Platform played on (e.g., 'PC') |
| premier_tournament_id | STRING | Premier tournament ID if applicable |
| premier_matchup_id | STRING | Premier matchup ID if applicable |
| 
egion | STRING | Riot region (e.g., 'ap', 'na') |
| cluster | STRING | Specific server cluster/datacenter (e.g., 'Mumbai') |

---

### 2.2 alorant.bronze.bronze_team
* **Purpose:** Team-level outcomes (win/loss and final score) for Red and Blue sides.
* **Grain:** 1 row per team per match (2 rows per match).
* **Composite Key:** match_id + 	eam_side
* **Source Path:** data[].teams.red, data[].teams.blue

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| 	eam_side | STRING | Source team color: 'Red' or 'Blue' |
| has_won | BOOLEAN | True if this team won the match |
| 
ounds_won | INT | Number of rounds won by this team |
| 
ounds_lost | INT | Number of rounds lost by this team |

---

### 2.3 alorant.bronze.bronze_player
* **Purpose:** Cumulative match performance summary for every player in the match.
* **Grain:** 1 row per player per match (10 rows per match in standard 5v5).
* **Composite Key:** match_id + player_puuid
* **Source Path:** data[].players.all_players[]

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| player_puuid | STRING | Player's unique, persistent Riot PUUID |
| player_name | STRING | Player in-game username at match time |
| player_tag | STRING | Player tag line (e.g., 'IND') |
| 	eam | STRING | Team side: 'Red' or 'Blue' |
| level | INT | Account level at time of match |
| character | STRING | Agent played (e.g., 'Jett', 'Omen') |
| current_tier | INT | Rank tier integer index |
| current_tier_patched | STRING | Rank name (e.g., 'Gold 2', 'Platinum 1') |
| player_card | STRING | Equipped player card asset UUID |
| player_title | STRING | Equipped player title UUID |
| party_id | STRING | Party UUID (identifies premade groups) |
| session_playtime_minutes | INT | Total session playtime in minutes |
| session_playtime_seconds | INT | Total session playtime in seconds |
| session_playtime_milliseconds| INT | Total session playtime in ms |
| fk_rounds | DOUBLE | Rounds marked as AFK |
| riendly_fire_incoming | DOUBLE | Friendly fire damage received |
| riendly_fire_outgoing | DOUBLE | Friendly fire damage dealt |
| 
ounds_in_spawn | DOUBLE | Rounds spent staying in spawn |
| platform_type | STRING | OS platform (e.g., 'PC') |
| os_name | STRING | Operating system name |
| os_version | STRING | Operating system build version |
| x_casts | INT | Ultimate ability casts |
| e_casts | INT | Signature ability (E) casts |
| q_casts | INT | Primary ability (Q) casts |
| c_casts | INT | Secondary ability (C) casts |
| score | INT | Total combat score in match |
| kills | INT | Total kills in match |
| deaths | INT | Total deaths in match |
| ssists | INT | Total assists in match |
| odyshots | INT | Total bodyshots landed |
| headshots | INT | Total headshots landed |
| legshots | INT | Total legshots landed |
| spent_overall | INT | Total credits spent across all rounds |
| spent_average | DOUBLE | Average credits spent per round |
| loadout_value_overall | INT | Total loadout value across all rounds |
| loadout_value_average | DOUBLE | Average loadout value per round |
| damage_made | INT | Total damage dealt across all rounds |
| damage_received | INT | Total damage received across all rounds |

---

### 2.4 alorant.bronze.bronze_round
* **Purpose:** Round-by-round summary of winners, round end conditions, and spike plants/defuses.
* **Grain:** 1 row per round per match.
* **Composite Key:** match_id + 
ound_number
* **Source Path:** data[].rounds[] (1-indexed round numbering)

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| 
ound_number | INT | Round number (1, 2, 3, ...) |
| winning_team | STRING | Winning team side: 'Red' or 'Blue' |
| end_type | STRING | How round ended ('Eliminated', 'Bomb defused', 'Bomb detonated', 'Round timer expired') |
| omb_planted | BOOLEAN | True if the spike was planted in this round |
| omb_defused | BOOLEAN | True if the spike was successfully defused |

---

### 2.5 alorant.bronze.bronze_round_player_stats
* **Purpose:** Granular round-level statistics per player (credits, equipped weapon, armor, round damage, score, survival).
* **Grain:** 1 row per player per round per match.
* **Composite Key:** match_id + 
ound_number + player_puuid
* **Source Path:** data[].rounds[].player_stats[]

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| 
ound_number | INT | Round number |
| player_puuid | STRING | Player's unique Riot PUUID |
| player_display_name | STRING | Player display name at round time |
| player_team | STRING | Team side: 'Red' or 'Blue' |
| kills | INT | Kills secured in this specific round |
| deaths | INT | 1 if player died this round, else 0 |
| ssists | INT | Assists credited in this round |
| score | INT | Combat score earned in this round |
| damage | INT | Total damage dealt in this round |
| odyshots | INT | Bodyshots landed in this round |
| headshots | INT | Headshots landed in this round |
| legshots | INT | Legshots landed in this round |
| loadout_value | INT | Value of player equipment in this round |
| 
emaining_credits | INT | Banked credits remaining after purchase |
| spent_credits | INT | Credits spent in this round's buy phase |
| weapon_id | STRING | Equipped weapon UUID |
| weapon_name | STRING | Equipped weapon name (e.g., 'Vandal', 'Phantom', 'Classic') |
| rmor_id | STRING | Equipped armor UUID |
| rmor_name | STRING | Equipped armor name (e.g., 'Heavy Shield', 'Light Shield') |
| c_casts | INT | C ability casts this round |
| q_casts | INT | Q ability casts this round |
| e_casts | INT | E ability casts this round |
| x_casts | INT | Ultimate casts this round |
| was_afk | BOOLEAN | True if player was AFK during round |
| was_penalized | BOOLEAN | True if penalized during round |
| stayed_in_spawn | BOOLEAN | True if player never left spawn area |
| was_alive | BOOLEAN | True if player survived until round conclusion |

---

### 2.6 alorant.bronze.bronze_kill_event
* **Purpose:** Chronological kill feed events detailing who killed whom, weapon used, round time, assists, and spatial positions.
* **Grain:** 1 row per kill event per match.
* **Composite Key:** match_id + kill_event_sequence
* **Source Path:** data[].kills[]

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| kill_event_sequence | INT | Chronological kill order index in match |
| 
ound_number | INT | Round number where kill took place |
| kill_time_in_round | INT | Milliseconds into round when kill occurred |
| kill_time_in_match | INT | Milliseconds into entire match |
| killer_puuid | STRING | Killer's unique Riot PUUID |
| killer_display_name | STRING | Killer's display name |
| killer_team | STRING | Killer's team: 'Red' or 'Blue' |
| ictim_puuid | STRING | Victim's unique Riot PUUID |
| ictim_display_name | STRING | Victim's display name |
| ictim_team | STRING | Victim's team: 'Red' or 'Blue' |
| ictim_death_location | STRUCT<x: DOUBLE, y: DOUBLE> | In-game 2D coordinate of victim's death |
| damage_weapon_id | STRING | Weapon UUID that delivered final blow |
| damage_weapon_name | STRING | Weapon name (e.g., 'Operator', 'Sheriff') |
| secondary_fire_mode | BOOLEAN | True if alternate fire mode was used |
| ssist_puuids | ARRAY<STRING> | List of PUUIDs credited with assist |
| ssistants | ARRAY<STRUCT<assistant_puuid: STRING, assistant_display_name: STRING, assistant_team: STRING>> | Detailed assistant metadata |
| player_locations | ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>> | Spatial positions of all players at kill time |

---

### 2.7 alorant.bronze.bronze_damage_event
* **Purpose:** Granular round-level damage interactions between each attacker and receiver.
* **Grain:** 1 row per attacker-receiver interaction per round.
* **Composite Key:** match_id + 
ound_number + damage_event_sequence
* **Source Path:** data[].rounds[].player_stats[].damage_events[]

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| 
ound_number | INT | Round number |
| damage_event_sequence | INT | Sequence index within the round |
| ttacker_puuid | STRING | Player dealing the damage |
| ttacker_display_name | STRING | Attacker display name |
| ttacker_team | STRING | Attacker team: 'Red' or 'Blue' |
| 
eceiver_puuid | STRING | Player taking the damage |
| 
eceiver_display_name | STRING | Receiver display name |
| 
eceiver_team | STRING | Receiver team: 'Red' or 'Blue' |
| damage | INT | Total damage dealt in this exchange |
| odyshots | INT | Bodyshots landed in this exchange |
| headshots | INT | Headshots landed in this exchange |
| legshots | INT | Legshots landed in this exchange |

---

### 2.8 alorant.bronze.bronze_plant_event
* **Purpose:** Details of spike plant events (site, planter, timing, spatial coordinates).
* **Grain:** 1 row per plant event per match (at most 1 per round).
* **Composite Key:** match_id + 
ound_number + plant_event_sequence
* **Source Path:** data[].rounds[].plant_events

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| 
ound_number | INT | Round number |
| plant_event_sequence | INT | Plant event sequence number |
| player_puuid | STRING | PUUID of player who planted the spike |
| player_display_name | STRING | Display name of planter |
| player_team | STRING | Team side of planter ('Red' or 'Blue') |
| plant_site | STRING | Site where spike was planted ('A', 'B', 'C') |
| plant_time_in_round | INT | Milliseconds into the round when plant finished |
| plant_location | STRUCT<x: DOUBLE, y: DOUBLE> | In-game 2D coordinate where spike was placed |
| player_locations_on_plant | ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>> | Spatial positions of all surviving players at plant time |

---

### 2.9 alorant.bronze.bronze_defuse_event
* **Purpose:** Details of spike defuse events (defuser, timing, spatial coordinates).
* **Grain:** 1 row per defuse event per match (at most 1 per round).
* **Composite Key:** match_id + 
ound_number + defuse_event_sequence
* **Source Path:** data[].rounds[].defuse_events

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| match_id | STRING | Unique match GUID |
| 
ound_number | INT | Round number |
| defuse_event_sequence | INT | Defuse event sequence number |
| player_puuid | STRING | PUUID of player who successfully defused |
| player_display_name | STRING | Display name of defuser |
| player_team | STRING | Team side of defuser ('Red' or 'Blue') |
| defuse_site | STRING | Site where spike was defused ('A', 'B', 'C') |
| defuse_time_in_round | INT | Milliseconds into the round when defuse finished |
| defuse_location | STRUCT<x: DOUBLE, y: DOUBLE> | In-game 2D coordinate where spike was defused |
| player_locations_on_defuse | ARRAY<STRUCT<player_puuid: STRING, player_display_name: STRING, player_team: STRING, location: STRUCT<x: DOUBLE, y: DOUBLE>, view_radians: DOUBLE>> | Spatial positions of all surviving players at defuse time |

---

## 3. Silver Layer (alorant.silver)

### 3.0 alorant.silver.dim_team_roster
* **Purpose:** Official team roster configuration representing your core squad (SRS Section 24 & 27). Establishes who belongs to your team, assigns tactical roles and friendly nicknames, and drives automated OUR_TEAM resolution.
* **Grain:** 1 row per team roster member.
* **Primary Key:** player_puuid
* **Write Strategy:** Delta MERGE (Upsert on player_puuid)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| player_puuid | STRING | NO | Unique Riot player PUUID (Primary Key) |
| player_nickname | STRING | YES | Real-life name / Friendly callsign (e.g. 'Agamemnon') |
| default_display_name | STRING | YES | Default in-game name and tag |
| 	eam_role | STRING | YES | In-game role (e.g. 'Duelist', 'IGL', 'Core') |
| is_team_owner | BOOLEAN | NO | True for team owner/captain (Agamemnon) |
| is_active_roster | BOOLEAN | NO | True if currently an active squad member |
| created_at | TIMESTAMP | NO | Creation timestamp |
| updated_at | TIMESTAMP | NO | Record ETL update timestamp |

---
### 3.1 alorant.silver.dim_player
* **Purpose:** Master player directory providing stable player identity (SRS Section 25 & 28). Deduplicates players by persistent player_puuid, maintains the latest in-game name/tag, tracks account level, and flags core team members (is_core_team).
* **Grain:** 1 row per unique player (player_puuid).
* **Primary Key:** player_puuid
* **Source Tables:** alorant.bronze.bronze_player, alorant.bronze.bronze_match
* **Write Strategy:** Delta MERGE (Upsert on player_puuid)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| player_puuid | STRING | NO | Immutable Riot player PUUID (Primary Key) |
| current_display_name | STRING | YES | Most recent known 
ame#tag (e.g., 'Hiroshi#nohar') |
| player_name | STRING | YES | Most recent in-game name |
| player_tag | STRING | YES | Most recent tagline |
| ccount_level | INT | YES | Most recent account level |
| platform_type | STRING | YES | Platform (e.g., 'PC') |
| is_core_team | BOOLEAN | NO | True if player is part of our core tracked roster |
| irst_seen_timestamp | TIMESTAMP | YES | Match timestamp when player first appeared |
| last_seen_timestamp | TIMESTAMP | YES | Match timestamp when player was most recently seen |
| 	otal_matches_recorded| INT | YES | Total matches played by this player in our data |
| updated_at | TIMESTAMP | NO | ETL pipeline processing timestamp |

---

### 3.2 alorant.silver.dim_agent
* **Purpose:** Master agent dimension providing tactical roles (Duelist, Initiator, Controller, Sentinel), active rotation status, and official Riot CDN image URLs for native rendering in Databricks AI/BI Dashboards and reports (SRS Section 7 & 22).
* **Grain:** 1 row per unique agent.
* **Primary Key:** gent_name
* **Write Strategy:** Delta MERGE (Upsert on gent_name)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| gent_name | STRING | NO | Official agent name (e.g., 'Jett', 'Omen') (Primary Key) |
| gent_role | STRING | YES | Tactical role: 'Duelist', 'Initiator', 'Controller', 'Sentinel' |
| gent_icon_url | STRING | YES | Riot CDN thumbnail icon URL (renders in Databricks Table visuals) |
| gent_portrait_url| STRING | YES | Riot CDN full portrait URL (renders in Databricks Hero/Detail cards) |
| is_active | BOOLEAN | NO | True if in active competitive rotation |
| updated_at | TIMESTAMP | NO | Record ETL update timestamp |

---

### 3.3 alorant.silver.dim_map
* **Purpose:** Master map dimension cataloging spike plant sites (A, B, C), competitive map pool rotation status, and official Riot CDN splash/radar image URLs for native rendering in Databricks AI/BI Dashboards (SRS Section 6 & 15).
* **Grain:** 1 row per unique map.
* **Primary Key:** map_name
* **Write Strategy:** Delta MERGE (Upsert on map_name)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| map_name | STRING | NO | Official map name (e.g., 'Ascent', 'Bind', 'Haven') (Primary Key) |
| site_count | INT | NO | Number of bomb sites (2 or 3) |
| sites | ARRAY<STRING>| NO | Available bomb sites (e.g., ['A', 'B'] or ['A', 'B', 'C']) |
| map_splash_url | STRING | YES | High-res map background image for Databricks dashboard cards |
| map_radar_url | STRING | YES | Minimap 2D radar image URL |
| is_active_pool | BOOLEAN | NO | True if in active competitive pool |
| updated_at | TIMESTAMP | NO | Record ETL update timestamp |

---

### 3.4 alorant.silver.dim_weapon
* **Purpose:** Master weapon dimension providing tactical category (Rifle, Sniper, Sidearm, SMG, Shotgun, Heavy, Melee), standard credit purchase cost, and official Riot CDN silhouette image URLs for native rendering in Databricks AI/BI Dashboards (SRS Section 18 & 23).
* **Grain:** 1 row per unique weapon.
* **Primary Key:** weapon_name
* **Write Strategy:** Delta MERGE (Upsert on weapon_name)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| weapon_name | STRING | NO | Official weapon name (e.g., 'Vandal', 'Phantom') (Primary Key) |
| weapon_category | STRING | YES | Tactical category ('Rifle', 'Sniper', 'Sidearm', 'SMG', etc.) |
| weapon_cost | INT | YES | Standard purchase price in credits (e.g., 2900 for Vandal, 4700 for Operator) |
| weapon_icon_url | STRING | YES | High-res 2D weapon image from Riot CDN |
| updated_at | TIMESTAMP | NO | Record ETL update timestamp |

---

### 3.5 alorant.silver.dim_match
* **Purpose:** Canonical match dimension providing clean calendar dates, durations, server clusters, and automated team perspective outcomes (our_team_side, our_team_rounds_won, opponent_rounds_won, match_outcome) derived dynamically from dim_team_roster (SRS Section 2, 24 & 27).
* **Grain:** 1 row per unique match.
* **Primary Key:** match_id
* **Write Strategy:** Incremental Delta MERGE (Upsert on match_id)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| match_id | STRING | NO | Unique match GUID (Primary Key) |
| match_date | DATE | NO | Calendar date match was played |
| match_start_timestamp | TIMESTAMP | NO | UTC match start timestamp |
| map_name | STRING | NO | Map played (joins to dim_map) |
| game_duration_seconds | INT | YES | Match duration in seconds |
| game_duration_minutes | DOUBLE | YES | Match duration in minutes |
| 
ounds_played | INT | NO | Total rounds played in match |
| is_overtime | BOOLEAN | NO | True if match extended past 24 rounds |
| our_team_side | STRING | YES | Which side your squad played on ('Red' or 'Blue') |
| our_team_rounds_won | INT | YES | Rounds won by your team |
| opponent_rounds_won | INT | YES | Rounds won by opponent team |
| 
ound_differential | INT | YES | Round differential (+/-) |
| match_outcome | STRING | YES | Match result: 'VICTORY', 'DEFEAT', or 'DRAW' |
| is_our_team_win | BOOLEAN | YES | True if your squad won the match |
| server_cluster | STRING | YES | Datacenter cluster (e.g. 'Mumbai') |
| game_version | STRING | YES | Valorant game patch release |
| updated_at | TIMESTAMP | NO | Record ETL update timestamp |

---

### Remaining Planned Silver Tables
5. **act_round** — Round outcomes enriched with ttack_team, defense_team, our_team_side, and is_our_team_win.
6. **act_match_player** — Match-level player scorecard (K/D, ADR, Headshot %, Combat Score).
7. **act_round_player** — Round-level player stats with assigned side (Attack/Defense), weapon buy classification, and survival status.
8. **act_kill_event** — Kill timeline with chronological is_opening_kill and is_opening_death flags.
9. **act_damage_event** — Attacker-to-receiver damage exchanges with team role tagging.
10. **act_spike_event** — Unified plant and defuse facts for post-plant analytics.
