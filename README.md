# Kickstarter ETL Pipeline

This project simulates a real-time data ingestion and processing pipeline for Kickstarter campaign data using PySpark. It includes dynamic CSV generation, file-based ingestion with Watchdog, data transformation and aggregation and automatic top-campaign analysis.


---

## Technologies Used

- **Python 3.11**
- **PySpark**
- **Watchdog** (file observer)
- **Parquet** (efficient data storage)
- **CSV** (for top 20 campaign exports)

---

## How It Works

1. `generate_kickstarter_csv.py` generates randomized, realistic Kickstarter datasets.
2. `watcher.py` watches the `data/raw/` folder for new CSV files.
3. When a new file appears:
   - It is processed through the ETL pipeline (`etl.py`)
   - Parsed, cleaned, and transformed into a `fact_campaign` and `dim_campaign` table
   - Summary metrics (success rates) and top 20 campaigns (by goal/day) are updated

---

## Quick Start

### 1. Generate Fake Data
```bash
python scripts/generate_kickstarter_csv.py --rows 1000
```

### 2. Start the Watcher
```bash
python -m scripts.watcher
```

---

## Outputs Path	Description

data/output/fact_campaign/	Cleaned and structured campaign data

data/output/dim_campaign/	Dimension table for categories

data/output/summary_campaign/	Aggregated success metrics

data/output/top_campaigns/	Top 20 campaigns by goal per day