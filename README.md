# E-commerce Data Pipeline with Automated ETL

A complete, production-grade data pipeline that extracts transactional data from an e-commerce platform, transforms it into analytics-ready formats, and loads it into a star-schema data warehouse — all orchestrated by Apache Airflow.

---

## Architecture

```
Source DB (MySQL/PostgreSQL)
        │
        ▼
  ┌──────────┐     ┌─────────────┐     ┌──────────────┐
  │ EXTRACT  │ ──► │  TRANSFORM  │ ──► │    LOAD      │
  │ Python   │     │  Pandas     │     │  PostgreSQL  │
  │ SQLAlchemy│     │  NumPy      │     │  /Snowflake  │
  └──────────┘     └─────────────┘     └──────────────┘
        │                                      │
        └──── Orchestrated by Apache Airflow ──┘
                                               │
                                               ▼
                                    Power BI / Tableau
                                      Dashboards
```

## Project Structure

```
ETL Automation/
├── config/
│   ├── database.yaml             # Database connection settings
│   └── pipeline_config.yaml      # ETL pipeline parameters
├── data/
│   ├── raw/                      # Extracted raw CSV files
│   ├── processed/                # Transformed analytics-ready files
│   └── sample/                   # Sample datasets for development
├── scripts/
│   ├── __init__.py
│   ├── extract.py                # Data extraction module
│   ├── transform.py              # Data transformation module
│   ├── load.py                   # Data warehouse loading module
│   └── etl_pipeline.py           # End-to-end pipeline runner
├── airflow_dags/
│   └── ecommerce_etl_dag.py      # Airflow DAG definition
├── warehouse_schema/
│   ├── source_schema.sql         # Source OLTP database DDL
│   ├── warehouse_schema.sql      # Star schema DDL + materialized views
│   └── analytics_queries.sql     # 12 business analytics queries
├── dashboards/
│   └── dashboard_specs.md        # Dashboard layout specifications
├── tests/
│   ├── test_extract.py           # Extraction unit tests
│   ├── test_transform.py         # Transformation unit tests
│   └── test_quality.py           # Data quality validation suite
├── logs/                         # Pipeline execution logs
├── docs/
│   └── Project_Report.md         # Comprehensive project report
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Quick Start

### Option A: Docker (Recommended — No Database Installation Required)

```bash
# 1. Start PostgreSQL and run the full ETL pipeline
docker compose up --build

# 2. (Optional) Connect to the warehouse to run analytics queries
docker exec -it etl_postgres psql -U etl_user -d ecommerce_dw

# 3. Stop and clean up
docker compose down -v
```

The Docker setup automatically:
- Starts PostgreSQL 15 with source + warehouse databases
- Seeds the source database with sample e-commerce data
- Runs Extract → Transform → Load pipeline end-to-end
- Data persists in `data/` and `logs/` directories locally

### Option B: Manual Setup (Requires MySQL/PostgreSQL Installed)

#### 1. Prerequisites

- Python 3.9+
- MySQL or PostgreSQL database
- Apache Airflow 2.7+ (for orchestration)
- Docker Desktop (for containerized setup)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Databases

Edit `config/database.yaml` with your database credentials, or set environment variables:

```bash
export SOURCE_DB_HOST=localhost
export SOURCE_DB_PORT=3306
export SOURCE_DB_USER=root
export SOURCE_DB_PASSWORD=your_password
export SOURCE_DB_NAME=ecommerce_db

export DW_HOST=localhost
export DW_PORT=5432
export DW_USER=warehouse_user
export DW_PASSWORD=warehouse_pass
export DW_NAME=ecommerce_dw
```

### 4. Create Source Database

```bash
mysql -u root -p ecommerce_db < warehouse_schema/source_schema.sql
```

### 5. Create Data Warehouse

```bash
psql -U warehouse_user -d ecommerce_dw -f warehouse_schema/warehouse_schema.sql
```

### 6. Run the Pipeline

```bash
# Full pipeline (extract → transform → load)
python scripts/etl_pipeline.py

# Individual steps
python scripts/etl_pipeline.py --step extract
python scripts/etl_pipeline.py --step transform
python scripts/etl_pipeline.py --step load
```

### 7. Run Tests

```bash
# All tests
python -m pytest tests/ -v

# Specific test suite
python -m pytest tests/test_transform.py -v
python -m pytest tests/test_quality.py -v
```

### 8. Set Up Airflow (Optional)

```bash
# Copy DAG to Airflow
cp airflow_dags/ecommerce_etl_dag.py ~/airflow/dags/

# Start Airflow
airflow webserver --port 8080 &
airflow scheduler &
```

The DAG runs daily at 02:00 UTC with 3 retries on failure.

## Data Warehouse — Star Schema

| Table | Type | Description |
|-------|------|-------------|
| `sales_fact` | Fact | One row per order with measures (amount, quantity, discount) |
| `customer_dim` | Dimension | Customer profiles with segments (Premium/Regular/New) |
| `product_dim` | Dimension | Product catalog with categories and profit margins |
| `date_dim` | Dimension | Calendar table (2024-2026) with day/month/quarter attributes |
| `payment_dim` | Dimension | Payment methods and transaction statuses |

## Analytics Queries

The `warehouse_schema/analytics_queries.sql` file includes 12 production-ready queries:

1. Total sales per day
2. Best selling products (top 10)
3. Customer lifetime value (top 20)
4. Monthly revenue trend with MoM growth
5. Payment method analysis
6. Sales by product category
7. Customer segmentation analysis
8. Weekend vs weekday comparison
9. Order status distribution
10. Geographic sales analysis
11. Quarterly performance summary
12. Repeat customer rate

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Source Database | MySQL / PostgreSQL |
| ETL Scripts | Python, Pandas, SQLAlchemy |
| Data Warehouse | PostgreSQL / Snowflake / BigQuery |
| Orchestration | Apache Airflow |
| Visualization | Power BI / Tableau / Metabase |
| Testing | pytest, unittest |

## Documentation

See `docs/Project_Report.md` for the complete project report covering:

- System architecture with diagrams
- ETL pipeline design and code walkthrough
- Star schema data warehouse design
- Airflow automation and scheduling
- Security, governance, and GDPR compliance
- Scalability and optimization strategies
- Real-world implementation examples (Amazon, Flipkart, Shopify)

---

**Project:** E-commerce Data Pipeline with Automated ETL
**Course:** Data Analytics
