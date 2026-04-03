# Databricks notebook source
# Cell 1 — Configuration
# -------------------------------------------------------
# Why define configs at the top?
# Single place to change storage account name, container,
# or catalog name without hunting through the notebook.
# -------------------------------------------------------

STORAGE_ACCOUNT = "iplanalyticsdlake"
CONTAINER       = "ipl-data"
CATALOG         = "ipl_catalog"
BRONZE_SCHEMA   = "bronze"

# ABFSS path — Azure Blob File System Secured
# Format: abfss://<container>@<storage_account>.dfs.core.windows.net/<path>
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/"

print(f"Bronze path: {BRONZE_PATH}")


# COMMAND ----------

# Cell 2 — Create Catalog and Schema in Unity Catalog
# -------------------------------------------------------
# Unity Catalog hierarchy: Catalog → Schema → Table
# We create these once. Think of catalog = database server,
# schema = database, table = table.
# -------------------------------------------------------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# Use the catalog
spark.sql(f"USE CATALOG {CATALOG}")

# Create bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

print(f"✅ Catalog '{CATALOG}' and schema '{BRONZE_SCHEMA}' ready")

# COMMAND ----------

# Cell 2b — Authenticate Databricks to ADLS Gen2
# -------------------------------------------------------
# We're using Storage Account Key authentication here.
# This is the simplest method for a dev/learning setup.
# In production you'd use a Service Principal + Azure Key Vault
# so the key is never stored in notebook code.
# -------------------------------------------------------

STORAGE_KEY = "your_storage_key_here"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

print("✅ Storage account authentication configured")




# COMMAND ----------

# Cell 3 — Ingest matches.csv into Bronze Delta table
# -------------------------------------------------------
# inferSchema=True: Spark reads the file twice — once to 
# infer types, once to load. Fine for Bronze on small files.
# header=True: first row is column names.
# We write as Delta — not CSV — because Delta gives us
# ACID, time travel, and schema enforcement from day one.
# mode="overwrite": safe for Bronze re-runs (idempotent)
# -------------------------------------------------------

matches_df = spark.read.csv(
    f"{BRONZE_PATH}matches.csv",
    header=True,
    inferSchema=True
)

# print(f"matches row count : {matches_df.count()}")
# print(f"matches columns   : {len(matches_df.columns)}")
# matches_df.printSchema()
matches_df.display()


# COMMAND ----------

# Cell 4 — Ingest deliveries.csv into Bronze Delta table

deliveries_df = spark.read.csv(
    f"{BRONZE_PATH}deliveries.csv",
    header=True,
    inferSchema=True
)

print(f"deliveries row count : {deliveries_df.count()}")
print(f"deliveries columns   : {len(deliveries_df.columns)}")
deliveries_df.printSchema()
# deliveries_df.display()

# COMMAND ----------

# Cell 5 — Write Delta files directly to ADLS, then register as External Tables
# -------------------------------------------------------
# Why this approach?
# saveAsTable() = Unity Catalog managed table (UC controls storage location)
# External table = YOU control the storage path in ADLS
# 
# For a data lake architecture, external tables are preferred —
# your data lives in YOUR storage, Unity Catalog just governs it.
# -------------------------------------------------------

# Write matches as Delta files directly to ADLS
matches_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BRONZE_PATH}delta/matches")


print("✅ matches Delta files written to ADLS")

# Write deliveries as Delta files directly to ADLS
deliveries_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BRONZE_PATH}delta/deliveries")

print("✅ deliveries Delta files written to ADLS")

# COMMAND ----------

# Cell 6 — Register Delta files as External Tables in Unity Catalog

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

# Register matches as external table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.matches
    USING DELTA
    LOCATION '{BRONZE_PATH}delta/matches'
""")
print("✅ bronze.matches registered in Unity Catalog")

# Register deliveries as external table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.deliveries
    USING DELTA
    LOCATION '{BRONZE_PATH}delta/deliveries'
""")
print("✅ bronze.deliveries registered in Unity Catalog")