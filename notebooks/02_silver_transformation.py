# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning & Standardization
# MAGIC
# MAGIC **Purpose:** Turn messy Bronze data into a trustworthy, query-ready table.
# MAGIC This is where the "reduced data errors by 60%" work actually happens:
# MAGIC - Deduplication
# MAGIC - Null handling
# MAGIC - Type normalization
# MAGIC - Standardizing inconsistent text/date formats
# MAGIC
# MAGIC We write Silver using a **MERGE (upsert)** instead of overwrite. This is what
# MAGIC makes the pipeline "incremental" — if Bronze gets new data tomorrow, we only
# MAGIC need to merge the new/changed rows into Silver, not reprocess everything.

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, upper, initcap, to_date, coalesce, lit, trim
)
from delta.tables import DeltaTable

# COMMAND ----------

bronze_table = "workspace.default.bronze_sales"
silver_table = "workspace.default.silver_sales"

# COMMAND ----------

df_bronze = spark.table(bronze_table)
print(f"Bronze row count (includes duplicates/history): {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Deduplicate
# MAGIC `order_id` should be unique. We keep the most recently ingested version of
# MAGIC each order if there are duplicates.

# COMMAND ----------

df_dedup = (
    df_bronze
    .orderBy(col("_ingestion_timestamp").desc())
    .dropDuplicates(["order_id"])
)
print(f"Row count after dedup: {df_dedup.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Standardize text fields
# MAGIC Region came in as "North", "north", "SOUTH" etc. We normalize casing so
# MAGIC aggregations in Gold don't accidentally split "North" and "north" into two
# MAGIC separate groups.

# COMMAND ----------

df_clean = (
    df_dedup
    .withColumn("region", initcap(trim(col("region"))))
    .withColumn("category", initcap(trim(col("category"))))
    .withColumn("customer_name", trim(col("customer_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Standardize dates
# MAGIC Source data had three different date formats. `to_date` with `coalesce`
# MAGIC tries each format in turn until one parses successfully.

# COMMAND ----------

df_clean = df_clean.withColumn(
    "order_date",
    coalesce(
        to_date(col("order_date"), "yyyy-MM-dd"),
        to_date(col("order_date"), "dd/MM/yyyy"),
        to_date(col("order_date"), "dd-MM-yyyy"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — Handle nulls
# MAGIC Business rule for this project:
# MAGIC - Missing `quantity` -> default to 1 (assume single-item order, flag it)
# MAGIC - Missing `unit_price` -> drop the row (we can't reliably estimate revenue)
# MAGIC - Missing `payment_method` -> label as "Unknown"
# MAGIC
# MAGIC These are business decisions, not just code — always be ready to explain
# MAGIC *why* you chose each rule in an interview.

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn(
        "quantity",
        when(col("quantity").isNull(), lit(1)).otherwise(col("quantity"))
    )
    .withColumn(
        "payment_method",
        when(col("payment_method").isNull(), lit("Unknown")).otherwise(col("payment_method"))
    )
    .filter(col("unit_price").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — Derive revenue column
# MAGIC Computing this once in Silver means every downstream consumer (Gold, dashboards)
# MAGIC uses the same consistent definition of revenue.

# COMMAND ----------

df_clean = df_clean.withColumn("revenue", col("quantity") * col("unit_price"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 — Merge into Silver Delta table (upsert)
# MAGIC First run creates the table. Subsequent runs merge on `order_id`, updating
# MAGIC changed rows and inserting new ones — this is the ACID-compliant incremental
# MAGIC loading piece of the project.

# COMMAND ----------

if spark.catalog.tableExists(silver_table):
    delta_silver = DeltaTable.forName(spark, silver_table)
    (
        delta_silver.alias("target")
        .merge(df_clean.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Merged into existing Silver table.")
else:
    df_clean.write.format("delta").saveAsTable(silver_table)
    print("Created Silver table for the first time.")

print(f"Silver row count: {spark.table(silver_table).count()}")

# COMMAND ----------

display(spark.table(silver_table).limit(10))
