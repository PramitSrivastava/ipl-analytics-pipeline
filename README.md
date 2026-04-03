# IPL Cricket Analytics Pipeline 🏏

A production-grade **Medallion Architecture** data pipeline built on 
Azure Databricks that ingests, transforms, and delivers IPL cricket 
analytics using PySpark, Delta Lake, and Unity Catalog.

## Architecture
Raw CSV Files (Kaggle)
↓
Bronze Layer (ADLS Gen2)
└── Delta tables: matches, deliveries
↓
Silver Layer (ADLS Gen2)
└── Delta table: fact_ball_by_ball
↓
Gold Layer (ADLS Gen2)
├── top_batsmen
├── team_win_percentage
└── bowling_economy

## Tech Stack

| Tool | Purpose |
|------|---------|
| Azure Databricks | Compute + Notebooks |
| ADLS Gen2 | Data Lake Storage |
| PySpark | Transformations |
| Delta Lake | ACID table format |
| Unity Catalog | Governance + Access Control |
| Azure Workflows | Pipeline Orchestration |

## Dataset

IPL ball-by-ball and match data (2008-2020) from Kaggle:
- **matches.csv** — 1,095 matches
- **deliveries.csv** — 260,920 ball-by-ball records

## Pipeline Notebooks

| Notebook | Layer | Description |
|----------|-------|-------------|
| 01_bronze_ingestion | Bronze | Raw CSV → Delta tables |
| 02_silver_transform | Silver | Cleaning, joins, window functions |
| 03_gold_aggregations | Gold | Business aggregations |
| 04_governance | Governance | Unity Catalog documentation |

## Gold Layer Insights

### Top Batsmen per Season
- V Kohli — 973 runs in 2016 (IPL record)
- Strike rate calculations per batter per season
- Minimum 100 balls qualification

### Team Win Percentages
- Win % per team per season
- Calculated as (wins / matches played) * 100

### Bowling Economy Rates
- Economy rate per bowler per season
- Minimum 20 overs (120 balls) qualification

## Key Technical Decisions

**Why Delta Lake over Parquet?**
Delta adds ACID transactions and time travel on top of Parquet.
Corrupted data can be rolled back with RESTORE TABLE.

**Why External Tables over Managed Tables?**
Data lives in our ADLS Gen2 storage, not Databricks internal storage.
Gives full control over data lifecycle and access.

**Why Unity Catalog?**
Single governance layer across all workspaces.
Column-level security, data lineage, and audit logs.

## Pipeline Orchestration

Automated daily pipeline via Databricks Workflows:
bronze_ingestion → silver_transformation → gold_aggregation
Runs daily at 06:00 AM IST. Each task only runs if 
the previous task succeeds.

## Setup Instructions

1. Clone this repository
2. Create Azure resources (see architecture above)
3. Upload CSV files to ADLS Gen2 bronze/ container
4. Configure storage key in notebook Cell 1
5. Run notebooks in order: 01 → 02 → 03 → 04
6. Or trigger the Databricks Workflow

## Author

**Pramit Srivastava**  
Data Engineering Portfolio Project  
