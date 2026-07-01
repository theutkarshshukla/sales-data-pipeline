# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Land the raw CSV data into Delta format with zero transformation logic.
# MAGIC The rule for Bronze is: *store what you received, exactly as you received it*,
# MAGIC plus a little metadata about *when* and *from where* it arrived.
# MAGIC
# MAGIC Why not clean it here? Because if your cleaning logic has a bug, you want to be
# MAGIC able to re-run Silver from the original untouched data — not from data you've
# MAGIC already partially destroyed. Bronze is your safety net / source of truth.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config
# MAGIC Update `raw_path` to wherever you've uploaded `raw_sales.csv` in your Databricks
# MAGIC workspace (Catalog > your volume, or DBFS). In Free Edition, the easiest way is
# MAGIC to upload it via **Catalog > Add Data > Upload files to a volume**.

# COMMAND ----------

raw_path = "/Volumes/workspace/default/sales_raw/raw_sales.csv"  # <-- update this path
bronze_table = "workspace.default.bronze_sales"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read raw CSV
# MAGIC We read everything as-is. Notice we don't even set `inferSchema` too aggressively —
# MAGIC we want to see the raw mess (nulls, mixed types) before deciding how to handle it
# MAGIC in Silver.

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)

df_raw.printSchema()
display(df_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion metadata
# MAGIC This is standard practice: every Bronze table should record *when* the data
# MAGIC landed and *where* it came from, so you can debug and audit later.

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta (append mode)
# MAGIC Bronze tables are typically append-only — every ingestion run adds a new batch,
# MAGIC we never overwrite history here.

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("append")
    .saveAsTable(bronze_table)
)

print(f"Bronze load complete. Row count: {spark.table(bronze_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick sanity check

# COMMAND ----------

display(spark.table(bronze_table).limit(10))
