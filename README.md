# Valorant Team Performance Analytics — Databricks Medallion Architecture

A data engineering platform ingesting Valorant match data from the [HenrikDev Valorant API](https://api.henrikdev.xyz), processing it through a Databricks Medallion architecture (Landing → Bronze → Silver → Gold).

## Project Structure

```text
valo_analytics/
├── release_1/                        # Simple, fast Databricks-first notebooks
│   ├── landing_ingestion.py         # Self-contained landing ingestion notebook
│   ├── bronze_ingestion.py          # All-in-one bronze tables ingestion notebook
│   └── bronze/                      # Dedicated per-table Bronze notebooks
│       ├── 01_bronze_match.py       # Match metadata (1 row per match)
│       ├── 02_bronze_team.py        # Team performance (1 row per team per match)
│       ├── 03_bronze_player.py      # Player statistics (1 row per player per match)
│       ├── 04_bronze_round.py       # Round summaries (1 row per round per match)
│       └── run_all_bronze.py        # Pipeline orchestrator to run all 4 in order
│
├── release_2/                        # Enterprise modular architecture
│   ├── configs/                     # Config dataclasses & path resolvers
│   ├── src/                         # Modular packages (common, landing)
│   ├── notebooks/                   # Databricks runners with widgets
│   └── tests/                       # Automated pytest suite (26 unit/integration tests)
│
├── AGENTS.md                        # Medallion design rules and architecture spec
├── requirements.txt
└── README.md
```

---

## Bronze Layer Notebooks (`release_1/bronze/`)

You can run each notebook independently in Databricks or run them sequentially via `run_all_bronze.py`:

| Notebook | Table Name | Grain | Primary Key |
|---|---|---|---|
| [`01_bronze_match.py`](release_1/bronze/01_bronze_match.py) | `valorant.bronze.bronze_match` | 1 row per match | `match_id` |
| [`02_bronze_team.py`](release_1/bronze/02_bronze_team.py) | `valorant.bronze.bronze_team` | 1 row per team per match | `match_id` + `team_side` |
| [`03_bronze_player.py`](release_1/bronze/03_bronze_player.py) | `valorant.bronze.bronze_player` | 1 row per player per match | `match_id` + `player_puuid` |
| [`04_bronze_round.py`](release_1/bronze/04_bronze_round.py) | `valorant.bronze.bronze_round` | 1 row per round per match | `match_id` + `round_number` |
| [`run_all_bronze.py`](release_1/bronze/run_all_bronze.py) | *Pipeline Orchestrator* | Runs all 4 notebooks | N/A |

All Bronze notebooks use **Delta MERGE** for strict idempotency and execute built-in data quality assertions.
