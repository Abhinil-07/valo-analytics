# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Run All Silver Dimensions Pipeline
# MAGIC 
# MAGIC Executes all Silver Dimension notebooks in strict dependency sequence:
# MAGIC 1. `00_dim_team_roster` — Seeds core squad & assigns roles (Agamemnon, systemctl, Ned, etc.)
# MAGIC 2. `01_dim_player` — Master player directory (joins dynamically with roster for `is_core_team`)
# MAGIC 3. `02_dim_agent` — Agent catalog with roles and Riot CDN icons/portraits
# MAGIC 4. `03_dim_map` — Map catalog with bomb sites and minimap radar URLs
# MAGIC 5. `04_dim_weapon` — Weapon catalog with categories, costs, and silhouettes
# MAGIC 6. `05_dim_match` — Canonical match dimension with automated `OUR_TEAM` resolution and scores

# COMMAND ----------
print("==========================================================")
print("  STARTING FULL SILVER DIMENSIONS PIPELINE EXECUTION")
print("==========================================================")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Execute `00_dim_team_roster`

# COMMAND ----------
# MAGIC %run ./00_dim_team_roster

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Execute `01_dim_player`

# COMMAND ----------
# MAGIC %run ./01_dim_player

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Execute `02_dim_agent`

# COMMAND ----------
# MAGIC %run ./02_dim_agent

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Execute `03_dim_map`

# COMMAND ----------
# MAGIC %run ./03_dim_map

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Execute `04_dim_weapon`

# COMMAND ----------
# MAGIC %run ./04_dim_weapon

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Execute `05_dim_match`

# COMMAND ----------
# MAGIC %run ./05_dim_match

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pipeline Complete — Verification Summary Across All Silver Dimensions

# COMMAND ----------
CATALOG = "valorant"
SCHEMA = "silver"

roster_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_team_roster").collect()[0][0]
player_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_player").collect()[0][0]
core_cnt   = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_player WHERE is_core_team = true").collect()[0][0]
agent_cnt  = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_agent").collect()[0][0]
map_cnt    = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_map").collect()[0][0]
weapon_cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_weapon").collect()[0][0]
match_cnt  = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_match").collect()[0][0]
wins_cnt   = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_match WHERE match_outcome = 'VICTORY'").collect()[0][0]
loss_cnt   = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.dim_match WHERE match_outcome = 'DEFEAT'").collect()[0][0]

print("==========================================================")
print("  SILVER DIMENSIONS PIPELINE EXECUTION SUMMARY")
print("==========================================================")
print(f"  dim_team_roster : {roster_cnt} roster members")
print(f"  dim_player      : {player_cnt} unique players ({core_cnt} core squad)")
print(f"  dim_agent       : {agent_cnt} agents")
print(f"  dim_map         : {map_cnt} maps")
print(f"  dim_weapon      : {weapon_cnt} weapons")
print(f"  dim_match       : {match_cnt} matches processed ({wins_cnt} Wins, {loss_cnt} Losses)")
print("==========================================================")
