# End-to-End Sales Data Pipeline

A Medallion Architecture (Bronze → Silver → Gold) data pipeline built on
Databricks Free Edition, using PySpark and Delta Lake.

## Architecture

```
raw_sales.csv (synthetic, intentionally messy)
        │
        ▼
  ┌───────────┐   append, minimal transformation, ingestion metadata
  │  BRONZE   │
  └───────────┘
        │
        ▼
  ┌───────────┐   dedup, null handling, type/date normalization, MERGE upsert
  │  SILVER   │
  └───────────┘
        │
        ▼
  ┌───────────┐   business aggregates: revenue, product performance, customer LTV
  │   GOLD    │
  └───────────┘
        │
        ▼
  3 AI/BI Dashboards
```

## Setup

### 1. Generate the raw dataset
```bash
cd data
python3 generate_sales_data.py --rows 5000 --out raw_sales.csv
```

### 2. Upload to Databricks
In your Databricks workspace: **Catalog → Add Data → Upload files to a volume**.
Upload `raw_sales.csv` to a volume, e.g. `/Volumes/workspace/default/sales_raw/`.
Update the `raw_path` variable in `notebooks/01_bronze_ingestion.py` to match.

### 3. Run the notebooks in order
1. `notebooks/01_bronze_ingestion.py`
2. `notebooks/02_silver_transformation.py`
3. `notebooks/03_gold_aggregation.py`

### 4. Build dashboards
Create an AI/BI Dashboard in Databricks with 3 visualizations, one per Gold table:
- `gold_monthly_revenue`
- `gold_product_performance`
- `gold_customer_metrics`

### 5. Schedule it as a Job
In Databricks: **Jobs & Pipelines → Create Job** → add all 3 notebooks as
sequential tasks (01 → 02 → 03) → set a daily trigger. This is what makes the
pipeline "scheduled" and removes the need for manual triggers.

## Why this design

- **Bronze is append-only and untransformed** — it's your safety net. If a bug
  is found in Silver logic, you can always re-run from Bronze without re-fetching
  source data.
- **Silver uses MERGE, not overwrite** — this is what makes loading incremental
  and ACID-compliant. New/changed orders get merged in; nothing is reprocessed
  unnecessarily.
- **Gold is business-facing** — dashboards and stakeholders should never have to
  re-derive revenue or re-clean dates themselves.

## Tech stack
PySpark, Databricks (Free Edition), Delta Lake, Medallion Architecture
