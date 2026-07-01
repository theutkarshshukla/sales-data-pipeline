# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregates
# MAGIC
# MAGIC **Purpose:** Produce clean, aggregated tables that dashboards and business
# MAGIC users query directly. Nobody building a dashboard should have to re-derive
# MAGIC revenue logic or re-clean dates — that's already done for them by the time
# MAGIC data reaches Gold.
# MAGIC
# MAGIC We build three Gold tables here, matching the "3 analytical dashboards"
# MAGIC claim on your resume:
# MAGIC 1. `gold_monthly_revenue` — revenue trend over time, by region
# MAGIC 2. `gold_product_performance` — best/worst selling products
# MAGIC 3. `gold_customer_metrics` — customer-level lifetime value & order frequency

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum as _sum, count, avg, countDistinct, date_format, max as _max, min as _min
)

# COMMAND ----------

silver_table = "workspace.default.silver_sales"
df_silver = spark.table(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1 — Monthly Revenue by Region

# COMMAND ----------

df_monthly_revenue = (
    df_silver
    .withColumn("order_month", date_format(col("order_date"), "yyyy-MM"))
    .groupBy("order_month", "region")
    .agg(
        _sum("revenue").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        avg("revenue").alias("avg_order_value"),
    )
    .orderBy("order_month", "region")
)

(
    df_monthly_revenue.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_monthly_revenue")
)

display(df_monthly_revenue.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2 — Product Performance

# COMMAND ----------

df_product_performance = (
    df_silver
    .groupBy("product_id", "product_name", "category")
    .agg(
        _sum("revenue").alias("total_revenue"),
        _sum("quantity").alias("total_units_sold"),
        count("order_id").alias("total_orders"),
    )
    .orderBy(col("total_revenue").desc())
)

(
    df_product_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_product_performance")
)

display(df_product_performance.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3 — Customer Metrics (a simple customer lifetime value view)

# COMMAND ----------

df_customer_metrics = (
    df_silver
    .groupBy("customer_id", "customer_name")
    .agg(
        _sum("revenue").alias("lifetime_value"),
        count("order_id").alias("total_orders"),
        avg("revenue").alias("avg_order_value"),
        _min("order_date").alias("first_order_date"),
        _max("order_date").alias("last_order_date"),
    )
    .orderBy(col("lifetime_value").desc())
)

(
    df_customer_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_customer_metrics")
)

display(df_customer_metrics.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next step: build dashboards
# MAGIC Go to **Dashboards** in the Databricks sidebar, create a new AI/BI Dashboard,
# MAGIC and point each visualization at one of these three Gold tables:
# MAGIC - `gold_monthly_revenue` -> line chart, revenue over time by region
# MAGIC - `gold_product_performance` -> bar chart, top products by revenue
# MAGIC - `gold_customer_metrics` -> table/leaderboard, top customers by lifetime value
