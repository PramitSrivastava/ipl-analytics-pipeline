# Databricks notebook source
# Cell 1 — Configuration
# -------------------------------------------------------
# Gold layer = business-ready aggregations
# No raw data here — only pre-aggregated analytical tables
# that a dashboard or analyst can query directly
# -------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

STORAGE_ACCOUNT = "iplanalyticsdlake"
CONTAINER       = "ipl-data"
CATALOG         = "ipl_catalog"
SILVER_SCHEMA   = "silver"
GOLD_SCHEMA     = "gold"

GOLD_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"

# Authenticate to ADLS
STORAGE_KEY = "your_storage_key_here"
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# Create gold schema in Unity Catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

print(f"✅ Gold config ready")
print(f"✅ Gold path: {GOLD_PATH}")

# COMMAND ----------

# Cell 2 — Read Silver table
# -------------------------------------------------------
# Gold always reads from Silver — never from Bronze or raw files
# Silver is our single source of truth for cleaned data
# This is the Medallion Architecture contract:
# Bronze → Silver → Gold (never skip layers)
# -------------------------------------------------------

df_silver = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_ball_by_ball")
df_silver.display()
# print(f"✅ Silver rows loaded: {df_silver.count()}")
# print(f"✅ Columns: {df_silver.columns}")

# COMMAND ----------

# Cell 3 — Top Batsmen per Season
# -------------------------------------------------------
# Business question: Who scored the most runs each season?
# We also calculate strike rate — a key batting metric
# Strike rate = (runs scored / balls faced) * 100
# A strike rate above 130 is considered good in T20
# -------------------------------------------------------

# Step 1: Aggregate runs per batter per season
gold_top_batsmen = df_silver.groupBy("season", "batter") \
    .agg(
        F.sum("batsman_runs").alias("total_runs"),
        F.count("ball").alias("total_balls"),
        F.sum("is_boundary").alias("total_boundaries"),
        F.sum("is_wicket_delivery").alias("times_dismissed")
    ) \
    .withColumn(
        "strike_rate",
        F.round((F.col("total_runs") / F.col("total_balls")) * 100, 2)
    ) \
    .filter(F.col("total_balls") >= 100)  # min 100 balls to qualify

# Step 2: Rank batsmen within each season using Window
window_season = Window.partitionBy("season").orderBy(F.desc("total_runs"))

gold_top_batsmen = gold_top_batsmen \
    .withColumn("rank_in_season", F.rank().over(window_season)) \
    .filter(F.col("rank_in_season") <= 10) \
    .orderBy("season", "rank_in_season")

# Step 3: Write to ADLS as Delta
gold_top_batsmen.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}delta/top_batsmen")

# Step 4: Register in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.top_batsmen
    USING DELTA
    LOCATION '{GOLD_PATH}delta/top_batsmen'
""")

print("✅ gold.top_batsmen written and registered")
display(gold_top_batsmen.limit(10))

# COMMAND ----------

# Cell 4 — Team Win Percentages per Season
# -------------------------------------------------------
# We use team1 and team2 instead of batting_team/bowling_team
# because win percentage is match-level, not ball-level
# team1 and team2 represent the two teams in each match
# -------------------------------------------------------

# Get one row per match (distinct match_id)
df_matches = df_silver \
    .select("match_id", "season", "team1", "team2", "winner") \
    .distinct()

# Count total matches per team per season
# Each match involves 2 teams so we union team1 and team2
matches_played = df_matches \
    .select("season", F.col("team1").alias("team"), "match_id") \
    .union(
        df_matches.select("season", F.col("team2").alias("team"), "match_id")
    ) \
    .groupBy("season", "team") \
    .agg(F.countDistinct("match_id").alias("matches_played"))

# Count wins per team per season
matches_won = df_matches \
    .filter(F.col("winner").isNotNull()) \
    .filter(F.col("winner") != "") \
    .groupBy("season", F.col("winner").alias("team")) \
    .agg(F.countDistinct("match_id").alias("matches_won"))

# Join and calculate win percentage
gold_win_pct = matches_played \
    .join(matches_won, on=["season", "team"], how="left") \
    .fillna(0, subset=["matches_won"]) \
    .withColumn(
        "win_percentage",
        F.round((F.col("matches_won") / F.col("matches_played")) * 100, 2)
    ) \
    .orderBy("season", F.desc("win_percentage"))

# Write to ADLS
gold_win_pct.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}delta/team_win_percentage")

# Register in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.team_win_percentage
    USING DELTA
    LOCATION '{GOLD_PATH}delta/team_win_percentage'
""")

print("✅ gold.team_win_percentage written and registered")
display(gold_win_pct.limit(10))

# COMMAND ----------

# Cell 5 — Bowling Economy Rates
# -------------------------------------------------------
# Business question: Which bowlers were most economical?
# Economy rate = runs conceded per over (6 balls)
# Lower economy = better bowler
# Formula: (total_runs_conceded / total_balls_bowled) * 6
# We filter: min 120 balls bowled (= 20 overs) to qualify
# -------------------------------------------------------

gold_bowling = df_silver \
    .groupBy("season", "bowler") \
    .agg(
        F.sum("total_runs").alias("runs_conceded"),
        F.count("ball").alias("balls_bowled"),
        F.sum("is_wicket_delivery").alias("wickets_taken")
    ) \
    .filter(F.col("balls_bowled") >= 120) \
    .withColumn(
        "economy_rate",
        F.round((F.col("runs_conceded") / F.col("balls_bowled")) * 6, 2)
    ) \
    .withColumn(
        "bowling_average",
        F.round(F.col("runs_conceded") / F.col("wickets_taken"), 2)
    ) \
    .orderBy("season", "economy_rate")

# Write to ADLS
gold_bowling.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}delta/bowling_economy")

# Register in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.bowling_economy
    USING DELTA
    LOCATION '{GOLD_PATH}delta/bowling_economy'
""")

print("✅ gold.bowling_economy written and registered")
display(gold_bowling.limit(10))

# COMMAND ----------

# Cell 6 — Final Verification
# -------------------------------------------------------
# Always verify row counts and spot check data quality
# before declaring a layer complete
# -------------------------------------------------------

print("=== Gold Layer Tables ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.{GOLD_SCHEMA}").show()

print("=== Row Counts ===")
for table in ["top_batsmen", "team_win_percentage", "bowling_economy"]:
    count = spark.sql(
        f"SELECT COUNT(*) as cnt FROM {CATALOG}.{GOLD_SCHEMA}.{table}"
    ).collect()[0]['cnt']
    print(f"gold.{table}: {count} rows")

print("\n=== Top 5 Batsmen 2016 Season ===")
spark.sql(f"""
    SELECT season, batter, total_runs, strike_rate, rank_in_season
    FROM {CATALOG}.{GOLD_SCHEMA}.top_batsmen
    WHERE season = 2016
    ORDER BY rank_in_season
    LIMIT 5
""").show()