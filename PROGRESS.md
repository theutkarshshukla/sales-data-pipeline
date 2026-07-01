# Project Progress Log

Keep this file updated as you go. At the start of any new Claude Code session,
say: "Read PROGRESS.md and catch me up" — this gives Claude full context
without you re-explaining everything.

## Environment
- Databricks: Free Edition, workspace connected via Git folder to this repo
- Data volume: `/Volumes/workspace/default/sales_raw/raw_sales.csv`
- Compute: Serverless (IMPORTANT — always check Hardware > Accelerator = "None",
  it sometimes defaults to a GPU accelerator (1xA10) which throws a
  "GPU quota exceeded" error even though this pipeline never needs a GPU)

## Status

### ✅ Done
- [x] Databricks Free Edition account created and active
- [x] GitHub repo created: sales-data-pipeline
- [x] Databricks Git folder connected and syncing
- [x] raw_sales.csv generated (5,050 rows w/ intentional dupes/nulls) and
      uploaded to the volume above
- [x] `01_bronze_ingestion.py` ran successfully
  - Fixed: `input_file_name()` is not supported in Unity Catalog —
    replaced with `col("_metadata.file_path")`
  - Result: `workspace.default.bronze_sales` table created, ~5,050 rows

### 🔜 Next up
- [ ] Run `02_silver_transformation.py`
  - Watch for: compute defaulting to GPU accelerator again (set to None)
  - Expect final row count lower than Bronze (dupes removed, null-price rows dropped)
- [ ] Run `03_gold_aggregation.py`
  - Creates 3 Gold tables: gold_monthly_revenue, gold_product_performance,
    gold_customer_metrics
- [ ] Build 3 AI/BI Dashboards, one per Gold table
- [ ] Create a Databricks Job chaining notebooks 01 → 02 → 03, set a schedule
- [ ] Final polish: update README with screenshots, add to resume/portfolio

## Notes / gotchas encountered
- Unity Catalog deprecates `input_file_name()`; use `_metadata.file_path` instead
- Free Edition notebooks can silently default Hardware Accelerator to a GPU
  (1xA10) in the Environment/Configuration panel — always check and set to
  None before running, or you'll hit "Workspace has exceeded its GPU quota"
