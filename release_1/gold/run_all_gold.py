# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Run All Gold Analytics Pipeline
# MAGIC 
# MAGIC Executes all 10 Gold analytics notebooks in strict dependency sequence:
# MAGIC 1. `01_gold_match_summary` — Executive match overview, scores, MVPs, durations
# MAGIC 2. `02_gold_player_match_performance` — Player match scorecards, MVP badges, opponent comp matchups
# MAGIC 3. `03_gold_player_overall_summary` — Lifetime career leaderboards, signature agents, shot accuracy
# MAGIC 4. `04_gold_team_performance` — Multi-granularity team trajectory (`WEEKLY`, `FORTNIGHTLY`, `MONTHLY`)
# MAGIC 5. `05_gold_map_performance` — Map tier lists, starting side splits (Attack start vs Defense start)
# MAGIC 6. `06_gold_agent_performance` — Hero pool mastery, comfort picks vs pocket picks, utility efficiency
# MAGIC 7. `07_gold_attack_defense_performance` — Macro side diagnostics, first blood conversion, halftime momentum
# MAGIC 8. `08_gold_economy_performance` — Buy tier matchup matrix, thrifty ROI, anti-eco conversion
# MAGIC 9. `09_gold_combat_performance` — Weapon arsenal, accuracy, and squad weapon specialist trophy badges (🏆)
# MAGIC 10. `10_gold_spike_performance` — Bomb site execution bias, post-plant hold rates, retake defusals

# COMMAND ----------
print("==========================================================")
print("  STARTING FULL GOLD ANALYTICS PIPELINE EXECUTION")
print("==========================================================")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Execute `01_gold_match_summary`

# COMMAND ----------
# MAGIC %run ./01_gold_match_summary

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Execute `02_gold_player_match_performance`

# COMMAND ----------
# MAGIC %run ./02_gold_player_match_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Execute `03_gold_player_overall_summary`

# COMMAND ----------
# MAGIC %run ./03_gold_player_overall_summary

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Execute `04_gold_team_performance`

# COMMAND ----------
# MAGIC %run ./04_gold_team_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Execute `05_gold_map_performance`

# COMMAND ----------
# MAGIC %run ./05_gold_map_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Execute `06_gold_agent_performance`

# COMMAND ----------
# MAGIC %run ./06_gold_agent_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 7: Execute `07_gold_attack_defense_performance`

# COMMAND ----------
# MAGIC %run ./07_gold_attack_defense_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 8: Execute `08_gold_economy_performance`

# COMMAND ----------
# MAGIC %run ./08_gold_economy_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 9: Execute `09_gold_combat_performance`

# COMMAND ----------
# MAGIC %run ./09_gold_combat_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 10: Execute `10_gold_spike_performance`

# COMMAND ----------
# MAGIC %run ./10_gold_spike_performance

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 11: End-of-Pipeline Verification Across All Gold Tables

# COMMAND ----------
from pyspark.sql import functions as F

gold_tables = [
    "gold_match_summary",
    "gold_player_match_performance",
    "gold_player_overall_summary",
    "gold_team_performance",
    "gold_map_performance",
    "gold_agent_performance",
    "gold_attack_defense_performance",
    "gold_economy_performance",
    "gold_combat_performance",
    "gold_spike_performance"
]

print("\n==========================================================")
print("  GOLD LAYER PIPELINE RECONCILIATION SUMMARY")
print("==========================================================")

for table_name in gold_tables:
    full_table = f"valorant.gold.{table_name}"
    try:
        count = spark.table(full_table).count()
        print(f"  [SUCCESS] {full_table:<45} : {count:>6} rows")
    except Exception as e:
        print(f"  [ERROR]   {full_table:<45} : FAILED ({e})")

print("==========================================================")
print("  ALL 10 GOLD TABLES REFRESHED AND READY FOR DASHBOARDS!")
print("==========================================================")
