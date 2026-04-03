# Databricks notebook source
# Cell 1 — Governance Configuration
# -------------------------------------------------------
# This notebook documents all tables with:
# 1. Table-level descriptions
# 2. Column-level comments  
# 3. Access control grants
# -------------------------------------------------------

CATALOG       = "ipl_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

print("✅ Governance notebook ready")
print(f"Catalog : {CATALOG}")
print(f"Schemas : {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")

# COMMAND ----------

# Cell 2 — Bronze Layer Documentation
# -------------------------------------------------------
# Table comments tell anyone who opens Unity Catalog
# exactly what this table is, where data came from,
# and what layer it belongs to
# -------------------------------------------------------

# Bronze matches table description
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{BRONZE_SCHEMA}.matches IS
    'Bronze layer: Raw IPL match data ingested from matches.csv.
     Contains one row per match from 2008-2020.
     Source: Kaggle IPL Complete Dataset.
     No transformations applied - faithful copy of source data.'
""")
print("✅ bronze.matches documented")

# Bronze matches column comments
spark.sql(f"""
    ALTER TABLE {CATALOG}.{BRONZE_SCHEMA}.matches
    ALTER COLUMN id COMMENT 'Unique match identifier'
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{BRONZE_SCHEMA}.matches
    ALTER COLUMN season COMMENT 'IPL season year (2008-2020)'
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{BRONZE_SCHEMA}.matches
    ALTER COLUMN winner COMMENT 'Name of the winning team. Null if no result'
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{BRONZE_SCHEMA}.matches
    ALTER COLUMN player_of_match COMMENT 'Player of the match award winner'
""")
print("✅ bronze.matches columns documented")

# Bronze deliveries table description
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{BRONZE_SCHEMA}.deliveries IS
    'Bronze layer: Raw IPL ball-by-ball delivery data from deliveries.csv.
     Contains one row per delivery (260,920 total deliveries).
     Source: Kaggle IPL Complete Dataset.
     No transformations applied - faithful copy of source data.'
""")
print("✅ bronze.deliveries documented")

# Bronze deliveries column comments
spark.sql(f"""
    ALTER TABLE {CATALOG}.{BRONZE_SCHEMA}.deliveries
    ALTER COLUMN match_id COMMENT 'Foreign key to matches.id'
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{BRONZE_SCHEMA}.deliveries
    ALTER COLUMN is_wicket COMMENT 'Binary flag: 1 if wicket fell on this delivery, 0 otherwise'
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{BRONZE_SCHEMA}.deliveries
    ALTER COLUMN batsman_runs COMMENT 'Runs scored by the batter on this delivery (excludes extras)'
""")
print("✅ bronze.deliveries columns documented")

# COMMAND ----------

# Cell 3 — Silver Layer Documentation

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SILVER_SCHEMA}.fact_ball_by_ball IS
    'Silver layer: Cleaned and enriched ball-by-ball fact table.
     Joins deliveries with match context.
     Transformations: type casting, null handling, 
     is_boundary flag, running_total_runs window function.
     One row per delivery. Primary analytical table for Gold aggregations.'
""")
print("✅ silver.fact_ball_by_ball documented")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.fact_ball_by_ball
    ALTER COLUMN running_total_runs COMMENT 
    'Cumulative runs scored in the innings up to this delivery. 
     Calculated using Window function partitioned by match_id and inning.'
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.fact_ball_by_ball
    ALTER COLUMN is_boundary COMMENT 
    'Binary flag: 1 if delivery resulted in a 4 or 6, 0 otherwise'
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.fact_ball_by_ball
    ALTER COLUMN is_wicket_delivery COMMENT 
    'Binary flag: 1 if wicket fell on this delivery, 0 otherwise'
""")
print("✅ silver.fact_ball_by_ball columns documented")

# COMMAND ----------

# Cell 4 — Gold Layer Documentation

# Top batsmen
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{GOLD_SCHEMA}.top_batsmen IS
    'Gold layer: Top 10 batsmen per IPL season ranked by total runs.
     Minimum qualification: 100 balls faced.
     Metrics: total_runs, strike_rate, total_boundaries, rank_in_season.
     Refresh: full overwrite on each pipeline run.'
""")
print("✅ gold.top_batsmen documented")

# Team win percentage
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{GOLD_SCHEMA}.team_win_percentage IS
    'Gold layer: Win percentage for each IPL team per season.
     Calculated as (matches_won / matches_played) * 100.
     Includes all teams that participated in each season.'
""")
print("✅ gold.team_win_percentage documented")

# Bowling economy
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{GOLD_SCHEMA}.bowling_economy IS
    'Gold layer: Bowling economy rates per season.
     Minimum qualification: 120 balls bowled (20 overs).
     Economy rate = (runs_conceded / balls_bowled) * 6.
     Lower economy rate = more economical bowler.'
""")
print("✅ gold.bowling_economy documented")

# COMMAND ----------

# Cell 5 — Access Control Grants
# -------------------------------------------------------
# In production you'd have multiple users/groups
# For this project we grant ourselves full access
# and show the pattern for read-only access
#
# Unity Catalog privilege levels:
# SELECT          = read only
# MODIFY          = read + write
# ALL PRIVILEGES  = full control
# -------------------------------------------------------

# Grant yourself full access to all schemas
spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {CATALOG} TO `pramitsrivastava476@gmail.com`")
print("✅ Full access granted to pipeline owner")

# Show pattern for read-only access (e.g. for analysts)
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{GOLD_SCHEMA} TO `analyst@company.com`")
# This gives analysts read-only access to Gold layer only
# They cannot see Bronze or Silver raw data

print("""
✅ Access control pattern demonstrated:
   Owner     → ALL PRIVILEGES on catalog
   Analysts  → SELECT on gold schema only
   Engineers → MODIFY on silver schema
""")

# COMMAND ----------

# Cell 6 — Verify all documentation is in place

print("=== Bronze Layer ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{BRONZE_SCHEMA}.matches") \
    .filter("col_name = 'Comment'") \
    .show(truncate=False)

print("=== Silver Layer ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{SILVER_SCHEMA}.fact_ball_by_ball") \
    .filter("col_name = 'Comment'") \
    .show(truncate=False)

print("=== Gold Layer ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{GOLD_SCHEMA}.top_batsmen") \
    .filter("col_name = 'Comment'") \
    .show(truncate=False)

print("✅ Governance verification complete")