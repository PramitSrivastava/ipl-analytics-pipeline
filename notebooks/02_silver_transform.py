# Databricks notebook source
# Cell 1 — Configuration
STORAGE_ACCOUNT = "iplanalyticsdlake" 
CONTAINER       = "ipl-data"
CATALOG         = "ipl_catalog"
BRONZE_SCHEMA   = "bronze"
SILVER_SCHEMA   = "silver"

# Path for Silver data storage (Delta files)
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SILVER_SCHEMA}/"

# Create Silver Schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

print(f"✅ Configuration complete. Silver data will be stored at: {SILVER_PATH}")

# COMMAND ----------

# Cell 2 — Load Data from Bronze
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read the registered Bronze tables
df_matches = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.matches")
df_deliveries = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deliveries")

display(df_deliveries.limit(5)) # Quick check to see data loaded

# COMMAND ----------

# Cell 2.5 — Data Quality Checks (Duplicates & Nulls)

# 1. Deduplicate Matches (based on match ID)
matches_count_before = df_matches.count()
df_matches = df_matches.dropDuplicates(["id"])
matches_count_after = df_matches.count()

# 2. Deduplicate Deliveries (based on unique ball identifier)
deliveries_count_before = df_deliveries.count()
df_deliveries = df_deliveries.dropDuplicates(["match_id", "inning", "over", "ball"])
deliveries_count_after = df_deliveries.count()

# 3. Drop rows where the critical Match ID is NULL
# If we don't have a match_id, we can't join, so the data is useless.
df_matches = df_matches.dropna(subset=["id"])
df_deliveries = df_deliveries.dropna(subset=["match_id"])

print(f"Removed {matches_count_before - matches_count_after} duplicate matches.")
print(f"Removed {deliveries_count_before - deliveries_count_after} duplicate deliveries.")

# COMMAND ----------

# Cell 3 — Transform Matches
# 1. Cast 'date' to actual DateType
# 2. Extract Year as 'season'
# 3. Select and rename columns for consistency
df_matches_cleaned = df_matches.withColumn("match_date", F.to_date("date", "yyyy-MM-dd")) \
    .withColumn("season", F.year("match_date")) \
    .select(
        F.col("id").alias("match_id"),
        "season",
        "match_date",
        "venue",
        "city",
        "team1",
        "team2",
        "toss_winner",
        "toss_decision",
        "winner",
        "result",
        "result_margin"
    )

display(df_matches_cleaned.limit(5))

# COMMAND ----------

# Cell 4 — Transform Deliveries
# 1. Logic for boundary: if batsman_runs is 4 or 6, it's a boundary (1), else 0.
# 2. Logic for wicket: handle nulls in the is_wicket column.
df_deliveries_cleaned = df_deliveries.withColumn(
        "is_boundary", 
        F.when(F.col("batsman_runs").isin(4, 6), 1).otherwise(0)
    ).withColumn(
        "is_wicket_delivery", 
        F.when(F.col("is_wicket") == 1, 1).otherwise(0)
    ).select(
        "match_id",
        "inning",
        "over",
        "ball",
        "batter",
        "non_striker",
        "bowler",
        "batsman_runs",
        "extra_runs",
        "total_runs",
        "is_boundary",
        "is_wicket_delivery",
        "dismissal_kind",
        "player_dismissed"
    )

display(df_deliveries_cleaned.limit(5))

# COMMAND ----------

# Cell 5 — Join and Window Functions
# Join matches context onto every ball
df_silver_joined = df_deliveries_cleaned.join(df_matches_cleaned, on="match_id", how="inner")

# Define Window: Partition by match and inning, order by time (over/ball)
window_spec = Window.partitionBy("match_id", "inning").orderBy("over", "ball")

# Calculate Running Total
df_fact_ball_by_ball = df_silver_joined.withColumn(
    "running_total_runs", 
    F.sum("total_runs").over(window_spec)
)

display(df_fact_ball_by_ball.select("match_id", "over", "ball", "batter", "total_runs", "running_total_runs").limit(20))

# COMMAND ----------

# Cell 6 — Permanent Storage
target_table_name = f"{CATALOG}.{SILVER_SCHEMA}.fact_ball_by_ball"

df_fact_ball_by_ball.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{SILVER_PATH}fact_ball_by_ball") \
    .saveAsTable(target_table_name)

print(f"✅ Silver Layer Complete: {target_table_name} is ready for Gold aggregations!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     season, 
# MAGIC     venue, 
# MAGIC     batter as player_of_tournament, 
# MAGIC     SUM(batsman_runs) as total_runs, 
# MAGIC     COUNT(is_wicket_delivery) as total_wickets 
# MAGIC FROM ipl_catalog.silver.fact_ball_by_ball
# MAGIC GROUP BY season, venue, batter
# MAGIC ORDER BY total_runs DESC
# MAGIC LIMIT 10;