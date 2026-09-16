# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Run All Bronze Tables Ingestion Pipeline
# MAGIC 
# MAGIC Executes the four individual Bronze table notebooks in sequence:
# MAGIC 1. `01_bronze_match`
# MAGIC 2. `02_bronze_team`
# MAGIC 3. `03_bronze_player`
# MAGIC 4. `04_bronze_round`

# COMMAND ----------
print("Starting Bronze Pipeline Execution...")

# COMMAND ----------
# MAGIC %run ./01_bronze_match

# COMMAND ----------
# MAGIC %run ./02_bronze_team

# COMMAND ----------
# MAGIC %run ./03_bronze_player

# COMMAND ----------
# MAGIC %run ./04_bronze_round

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

print("=" * 50)
print("BRONZE PIPELINE COMPLETED")
print("=" * 50)
print(f"{CATALOG}.{SCHEMA}.bronze_match:  {match_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_team:   {team_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_player: {player_cnt} rows")
print(f"{CATALOG}.{SCHEMA}.bronze_round:  {round_cnt} rows")
print("=" * 50)
