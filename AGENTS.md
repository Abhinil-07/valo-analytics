# AGENTS.md

## Project: Valorant Team Performance Analytics

This repository contains a personal data engineering and analytics project that ingests Valorant match data from the HenrikDev Valorant API, processes it through a Databricks medallion architecture, and serves team/player performance analytics through Gold datasets and Power BI.

The project is intended to demonstrate production-style data engineering practices while remaining practical and storage-conscious because it is being developed using Databricks Free Edition.

## 1. Project Objective

Build an end-to-end analytics platform that converts raw Valorant match data into useful team-performance insights.

Primary business questions include:
- How is the team performing over time?
- How are individual players performing?
- Which maps and agents perform better or worse?
- What happened in a specific match?
- What factors contributed to wins and losses?
- How does performance change over time?
- How do attack/defense, combat, economy, and spike/objective performance vary?

The final architecture should support Power BI dashboards without coupling Bronze/Silver ingestion logic directly to dashboard requirements.

## 2. Architecture

Use a Databricks-first medallion architecture:

```text
HenrikDev Valorant API
        |
        v
Python API Ingestion
        |
        v
Landing - Databricks Volume
        |
        v
Bronze - Raw/Source-Aligned Delta Tables
        |
        v
Silver - Cleaned/Normalized Data Model
        |
        v
Gold - Business/Analytics Tables
        |
        v
Power BI
```

Technologies: Python, Requests, Databricks, Unity Catalog, Databricks Volumes, Delta Lake, PySpark/Spark SQL, Databricks SQL, Power BI, HenrikDev Valorant API.

Do not introduce additional technologies unless there is a clear requirement.

## 3. Design Principles

### Source-oriented Bronze
Bronze should represent the structure and grain of the source data. Do not design Bronze tables around Power BI visuals. Business transformations and derived KPIs belong primarily in Silver/Gold.

### Preserve raw match data
Each successfully ingested match should have one raw JSON file in the Landing Volume. Do not store every repeated consolidated API response.

Target structure:

```text
/Volumes/<catalog>/<schema>/<volume>/
    matches/
        YYYY/
            MM/
                DD/
                    <match_id>.json
```

Use the actual match date from the match metadata, not ingestion date.

### Match-level idempotency
`match_id` is the natural unique identifier for a match. If a match already has a successful ingestion record, skip it. New matches are written once. A previous FAILED ingestion may be retried.

### Ingestion control
Maintain a dedicated Delta table named `ingestion_control` with at least:
- `match_id`
- `match_date`
- `landing_path`
- `ingestion_timestamp`
- `ingestion_status`
- `source`
- `error_message`

Only successful ingestion should make a match idempotently complete.

### Raw API responses
Do not persist the entire consolidated API response by default. If the API returns C/D/E/F and C/D already exist, preserve only the new match JSONs in Landing; the in-memory consolidated response can be discarded after extraction.

## 4. Current API Response Structure

The API returns multiple matches under `data[]`:

```json
{
  "status": 200,
  "data": [
    {
      "is_available": true,
      "metadata": {...},
      "players": {...},
      "observers": [],
      "coaches": [],
      "teams": {...},
      "rounds": [...]
    }
  ]
}
```

Match metadata includes fields such as `map`, `game_version`, `game_length`, `game_start`, `game_start_patched`, `rounds_played`, `mode`, `mode_id`, `queue`, `season_id`, `platform`, `premier_info`, `region`, `cluster`, and `matchid`.

The source field is `matchid`; our normalized column should normally be `match_id`.

## 5. Landing Layer

Landing is responsible for:
1. Calling the API.
2. Receiving the response.
3. Extracting individual match objects.
4. Validating a usable `matchid`.
5. Checking `ingestion_control`.
6. Writing only new matches to the Databricks Volume.
7. Recording ingestion status.

Landing should not perform analytical transformations.

### Volume structure
Use a Unity Catalog Volume. Do not hard-code final catalog/schema/volume names until configured.

```text
/Volumes/<catalog>/<schema>/<volume>/
    matches/
        YYYY/
            MM/
                DD/
                    <match_id>.json
```

### File naming
Use `<match_id>.json`.

### Idempotency
- Existing SUCCESS: skip.
- Existing FAILED: allow retry.
- New match: write JSON and record SUCCESS.

Avoid inconsistent states where the control table says SUCCESS but the file is missing. If exact atomicity between Volume files and Delta metadata cannot be guaranteed, implement reconciliation/detection for incomplete states.

## 6. Landing Implementation Direction

Use Python for API extraction and orchestration.

```text
load configuration/secrets
        |
        v
call API
        |
        v
validate HTTP response
        |
        v
validate response structure
        |
        v
iterate data[]
        |
        v
extract metadata.matchid
        |
        v
determine match date
        |
        v
check ingestion_control
        |
        +---- SUCCESS exists --> skip
        |
        +---- otherwise ------> write match JSON
                                      |
                                      v
                              record ingestion result
```

Use environment variables or Databricks-supported secret management for API credentials. Never hard-code or commit API keys.

## 7. Bronze Layer

Create Bronze only after Landing ingestion is working and tested.

Current proposed Bronze tables:
1. `bronze_match`
2. `bronze_team`
3. `bronze_player`
4. `bronze_round`
5. `bronze_round_player_stats`
6. `bronze_kill_event`
7. `bronze_damage_event`
8. `bronze_plant_event`
9. `bronze_defuse_event`
10. `bronze_player_event_location` (optional; only if justified by actual source data and analytics requirements)

Before implementing each table, explicitly establish:
1. Table grain.
2. Primary/business key.
3. Columns.
4. Data types.
5. Source JSON path.
6. Example source record.
7. Why each column exists.
8. Downstream Silver usage.

Do not create Bronze tables simply because a field exists. Each table needs a clear grain and purpose.

## 8. Bronze Grain Principles

The source contains multiple grains:
- Match
- Team within match
- Player within match
- Round within match
- Player within round
- Kill event
- Damage event
- Plant event
- Defuse event

Do not flatten all of these into one Bronze table. Keep event-level and entity-level data at their natural grain to avoid duplication and cartesian/many-to-many explosions.

## 9. Silver Direction

Current dimensions:
- `dim_match`
- `dim_player`
- `dim_agent`
- `dim_map`
- `dim_weapon`
- `dim_armor`
- `dim_team`

Current facts:
- `fact_match_player`
- `fact_round`
- `fact_round_player`
- `fact_kill_event`
- `fact_damage_event`
- `fact_spike_event`

Silver handles type normalization, null handling, deduplication where required, consistent keys, business-friendly naming, reusable derived fields, and referential relationships.

Do not prematurely implement Silver until Bronze source mapping is understood.

## 10. Gold Direction

Current target tables:
- `gold_match_summary`
- `gold_player_match_performance`
- `gold_round_summary`
- `gold_team_performance`
- `gold_map_performance`
- `gold_agent_performance`
- `gold_player_trend`
- `gold_attack_defense_performance`
- `gold_economy_performance`
- `gold_combat_performance`
- `gold_spike_performance`
- `gold_match_insights`

Gold should contain reusable business metrics rather than forcing Power BI to reconstruct complex logic from raw event tables.

## 11. Dashboard Direction

Potential pages:
- Team Overview
- Match History
- Match Analysis
- Player Performance
- Map Analysis
- Agent Analysis
- Round Analysis
- Combat Analysis
- Economy Analysis
- Attack vs Defense
- Spike/Objectives
- Player Consistency

These are downstream requirements. Do not let dashboard requirements distort the source-aligned Bronze model.

## 12. Data Quality

Landing checks should include HTTP status, response structure, `data` presence/type, `matchid`, match date, duplicate detection, file-write validation, and ingestion status.

Bronze checks should include required keys, natural-grain duplicates, malformed records, null handling, and source-to-Bronze reconciliation.

Silver checks should include referential integrity, valid entity/event relationships, duplicate facts, and data types.

Gold checks should include metric sanity and aggregate reconciliation.

## 13. Incremental Processing

The project should be incremental:

```text
API
 ↓
identify match IDs
 ↓
compare against ingestion/control state
 ↓
land new matches
 ↓
process newly landed data
 ↓
update downstream layers
```

Use Delta `MERGE` only where the target grain and matching keys justify it.

## 14. Coding Standards

### Python
- Use small, clearly responsible functions.
- Use meaningful names and type hints where useful.
- Handle HTTP and JSON errors explicitly.
- Do not swallow exceptions.
- Log useful operational information.
- Never print secrets.
- Separate configuration from business logic.

### Spark/SQL
- Prefer readable transformations.
- Avoid unnecessary `.collect()` and full-table scans.
- Define keys/grains explicitly.
- Use Delta appropriately.
- Avoid unnecessary intermediate datasets.

### Security
Do not hard-code API keys, passwords, tokens, or other secrets. Use environment variables or Databricks secret mechanisms.

## 15. Storage Consciousness

Databricks Free Edition is storage-constrained. Prefer:
- one raw JSON per unique match
- Delta tables only where they provide analytical/engineering value
- no duplicated raw API response archives
- incremental processing
- minimal unnecessary intermediate datasets

Correctness and clear grain take priority over premature micro-optimization.

## 16. Source-of-Truth Rule

Landing match JSON is the raw source-of-truth for each ingested match.

Bronze is the source-aligned structured representation.

Silver is the cleaned/normalized analytical foundation.

Gold is the business-facing analytical layer.

## 17. Verification Before Implementation

Before writing or modifying code:
1. Inspect the actual repository and API response/schema available.
2. Do not invent fields.
3. Verify source field names.
4. Confirm table grain.
5. Confirm idempotency behavior.
6. Consider failure/retry behavior.
7. Keep Landing source data unchanged.
8. Test against the known two-match example before assuming broader API behavior.

Known example match IDs:
- `ab4a85fb-8f9e-4298-b22e-b2a3648cb58b`
- `1391571f-c2bc-4589-9dac-01b02b3563bc`

These are development/test examples and must not be hard-coded into application logic.

## 18. Development Order

### Phase 1 — Landing
- Configure Databricks Volume.
- Configure API credentials securely.
- Implement API extraction.
- Implement response validation.
- Implement match-level extraction.
- Implement match-date folder generation.
- Implement `ingestion_control`.
- Implement idempotency.
- Implement retries/error handling.
- Test new/existing/failed matches.

### Phase 2 — Bronze
Design and implement one table at a time, starting with `bronze_match`.

### Phase 3 — Silver
Build dimensions/facts from Bronze.

### Phase 4 — Gold
Build business-facing analytical tables.

### Phase 5 — Power BI
Connect dashboards to Gold.

## 19. Working Style for Codex

When making changes:
- First inspect the existing repository.
- Understand current implementation before changing it.
- Do not rewrite working code unnecessarily.
- Keep changes small and reviewable.
- Explain important design decisions.
- Add tests for ingestion/idempotency behavior.
- Run relevant tests after changes.
- Do not fabricate successful test results.
- If something cannot be verified locally, state that clearly.
- Do not add dependencies without checking necessity.
- Do not commit secrets or generated credentials.
- Do not implement future Bronze/Silver/Gold functionality unless explicitly requested.

When requirements are ambiguous, preserve the established architecture and ask for clarification rather than silently making a major architectural change.

## 20. Immediate Task

The first implementation task is Landing ingestion.

Desired behavior:

```text
HenrikDev API
     ↓
receive response containing multiple matches
     ↓
for each match:
    extract matchid
    derive match date
    check ingestion_control
    if SUCCESS exists:
        skip
    else:
        write:
        /Volumes/<catalog>/<schema>/<volume>/matches/YYYY/MM/DD/<match_id>.json
        record ingestion status
```

Do not implement Bronze yet.

Once Landing is implemented and tested, stop and review the Landing design before proceeding to `bronze_match`.
