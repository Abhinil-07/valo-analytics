# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Run All Bronze Tables Ingestion Pipeline
# MAGIC 
# MAGIC Executes all nine individual Bronze table notebooks in dependency sequence:
# MAGIC 1. `01_bronze_match`
# MAGIC 2. `02_bronze_team`
# MAGIC 3. `03_bronze_player`
# MAGIC 4. `04_bronze_round`
# MAGIC 5. `05_bronze_round_player_stats`
# MAGIC 6. `06_bronze_kill_event`
# MAGIC 7. `07_bronze_damage_event`
# MAGIC 8. `08_bronze_plant_event`
# MAGIC 9. `09_bronze_defuse_event`

# COMMAND ----------
print("Starting Full Bronze Pipeline Execution (9 Tables)...")

# COMMAND ----------
# MAGIC %run ./01_bronze_match

# COMMAND ----------
# MAGIC %run ./02_bronze_team

# COMMAND ----------
# MAGIC %run ./03_bronze_player

# COMMAND ----------
# MAGIC %run ./04_bronze_round

# COMMAND ----------
# MAGIC %run ./05_bronze_round_player_stats

# COMMAND ----------
# MAGIC %run ./06_bronze_kill_event

# COMMAND ----------
# MAGIC %run ./07_bronze_damage_event

# COMMAND ----------
# MAGIC %run ./08_bronze_plant_event

# COMMAND ----------
# MAGIC %run ./09_bronze_defuse_event

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pipeline Complete — Verification Summary

# COMMAND ----------
CATALOG = "valorant"
SCHEMA = "bronze"

match_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_match").collect()[0][0]
team_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_team").collect()[0][0]
player_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_player").collect()[0][0]
round_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_round").collect()[0][0]
pstat_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_round_player_stats").collect()[0][0]
kill_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_kill_event").collect()[0][0]
dmg_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_damage_event").collect()[0][0]
plant_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_plant_event").collect()[0][0]
defuse_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_defuse_event").collect()[0][0]

print("=" * 60)
print("BRONZE PIPELINE COMPLETED (9 TABLES)")
print("=" * 60)
print(f"{CATALOG}.{SCHEMA}.bronze_match:              {match_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_team:               {team_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_player:             {player_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_round:              {round_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_round_player_stats: {pstat_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_kill_event:          {kill_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_damage_event:        {dmg_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_plant_event:         {plant_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_defuse_event:        {defuse_cnt} rows")
print("=" * 60)
