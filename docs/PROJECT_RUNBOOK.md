# E-commerce ETL Automation Runbook

## 1. Project Features

- Automated Extract -> Transform -> Load pipeline with structured logs.
- PostgreSQL source + PostgreSQL warehouse in one Docker stack.
- Idempotent large synthetic data generation at startup.
- Config-driven extraction with incremental mode and automatic full fallback.
- Data quality checks after warehouse load.
- Materialized view refresh after each successful load.
- Streamlit analytics dashboard with KPI pages.
- Warehouse schema diagram page with live table counts and column metadata.
- Unit tests for extract, transform, and quality checks.

## 2. End-to-End Process

1. Bootstrap databases.
2. Load source schema and baseline seed data.
3. Generate large synthetic source data to configured target counts.
4. Extract source tables to raw files.
5. Transform raw files into warehouse-ready tables.
6. Load warehouse dimensions and fact table.
7. Run post-load quality checks.
8. Refresh materialized views.
9. Serve analytics dashboard.

## 3. Technology Stack

| Area | Technologies |
|---|---|
| Language | Python 3.11 |
| Data Processing | pandas, numpy |
| DB Connectivity | SQLAlchemy, psycopg2-binary |
| Source + Warehouse DB | PostgreSQL 15 |
| Orchestration (optional) | Apache Airflow |
| Dashboard | Streamlit, Plotly |
| Testing | pytest, unittest |
| Containerization | Docker, Docker Compose |
| Config | YAML, environment variables |

## 4. How To Run (Recommended: Docker)

### 4.1 Configure environment

1. Copy the template:

```bash
cp .env.example .env
```

2. Optional: tune synthetic data scale in `.env`:

```bash
ENABLE_LARGE_DATA=true
LARGE_DATA_CUSTOMERS=20000
LARGE_DATA_PRODUCTS=3000
LARGE_DATA_ORDERS=120000
LARGE_DATA_ORDER_LOOKBACK_HOURS=8760
LARGE_DATA_MIN_ORDER_SPAN_DAYS=90
```

### 4.2 Start the stack

```bash
docker compose up --build -d
```

This starts:
- `etl_postgres` (database)
- `etl_pipeline` (setup + ETL run)
- `etl_dashboard` (Streamlit app)

### 4.3 Check status

```bash
docker compose ps
docker compose logs --tail 200 etl
docker compose logs --tail 100 dashboard
```

### 4.4 Access outputs

- Dashboard: http://localhost:8501
- Warehouse schema page: Dashboard sidebar -> `Warehouse Schema`

### 4.5 Re-run ETL manually

```bash
docker compose run --rm etl
```

## 5. Local (Non-Docker) Run

1. Create and activate virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Ensure PostgreSQL is running and credentials are exported as env vars.
4. Run setup + ETL:

```bash
python scripts/setup_databases.py
python scripts/etl_pipeline.py
```

5. Run dashboard:

```bash
streamlit run dashboards/app.py
```

## 6. Data Volume Automation

Large-data generation is controlled by environment variables and is idempotent:
- If current table counts are below targets, missing rows are generated.
- If counts already meet targets, no extra rows are inserted.

### Tunable controls

| Variable | Meaning |
|---|---|
| `ENABLE_LARGE_DATA` | Turn synthetic data generation on/off |
| `LARGE_DATA_CUSTOMERS` | Target customer rows |
| `LARGE_DATA_PRODUCTS` | Target product rows |
| `LARGE_DATA_ORDERS` | Target order rows |
| `LARGE_DATA_ORDER_LOOKBACK_HOURS` | History window used for order timestamp distribution |
| `LARGE_DATA_MIN_ORDER_SPAN_DAYS` | Minimum distinct date-span target; triggers timestamp rebalance when too narrow |

## 7. Dashboard Pages

- Sales Overview
- Customer Analytics
- Product Performance
- Payment Analysis
- Warehouse Schema (diagram + relationships + metadata)

## 8. Testing

Run all tests:

```bash
pytest tests -v
```

Run module-level tests:

```bash
pytest tests/test_extract.py -v
pytest tests/test_transform.py -v
pytest tests/test_quality.py -v
```

## 9. Troubleshooting

### ETL container exits quickly

This is expected after setup + ETL finishes. Inspect output:

```bash
docker compose logs --tail 300 etl
```

### No fresh rows extracted in incremental mode

The extractor automatically falls back to full-table extract when an incremental table returns zero rows.

### Need clean restart

```bash
docker compose down -v
docker compose up --build -d
```

## 10. Useful Commands

```bash
# Start stack
docker compose up --build -d

# Stop stack
docker compose down

# Stop and remove volumes (fresh state)
docker compose down -v

# Follow ETL logs
docker compose logs -f etl

# Follow dashboard logs
docker compose logs -f dashboard
```
