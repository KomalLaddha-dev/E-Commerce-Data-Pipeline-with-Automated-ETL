# E-commerce Data Pipeline with Automated ETL

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791)](https://www.postgresql.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7%2B-017cee)](https://airflow.apache.org)
[![Docker](https://img.shields.io/badge/Docker-%23.svg?logo=docker)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-MIT-green)](#license)

</div>

A complete, **production-grade data pipeline** that extracts transactional data from an e-commerce platform, transforms it into analytics-ready formats, and loads it into a **star-schema data warehouse** — all orchestrated by Apache Airflow.

### ✨ Key Features

- 🔄 **Automated ETL Pipeline** — Extract, Transform, Load workflows with error handling
- 📊 **Star Schema Warehouse** — Optimized for OLAP analytics queries
- ✅ **Data Quality Checks** — Automated validation of loaded data
- 📅 **Airflow Orchestration** — Schedule, retry, and monitor pipeline runs
- 📈 **Real-time Dashboards** — Streamlit integration for analytics
- 🧱 **Warehouse Schema Diagram** — Visual model page with live table metadata
- 🏗️ **Large Synthetic Data Generation** — Idempotent high-volume source data seeding
- 🛡️ **Resilient Extraction Logic** — Incremental mode with automatic full fallback
- 🐳 **Docker Ready** — Complete containerized setup with PostgreSQL
- 🧪 **Comprehensive Tests** — Unit tests for all pipeline stages
- 📚 **Well Documented** — Detailed SQL schemas and queries included

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

### 🚀 Option A: Docker (Recommended)

No database installation required! The complete stack runs in Docker containers.

```bash
# 1. Clone the repository
git clone https://github.com/KomalLaddha-dev/E-Commerce-Data-Pipeline-with-Automated-ETL.git
cd ETL\ Automation

# 2. Create environment file (copy from template)
cp .env.example .env

# 3. Build and start all services
docker compose up --build

# This will:
# ✓ Start PostgreSQL 15 (source DB + data warehouse)
# ✓ Generate large synthetic source data to configured targets
# ✓ Run the full ETL pipeline
# ✓ Start the Streamlit dashboard (http://localhost:8501)
```

**After the pipeline completes:**

```bash
# Optional: Connect to PostgreSQL to run queries
docker exec -it etl_postgres psql -U etl_user -d ecommerce_dw

# Stop all services
docker compose down -v
```

### 📦 Option B: Local Installation

Requires PostgreSQL/MySQL already installed on your system.

#### Prerequisites

```
✓ Python 3.9 or higher
✓ PostgreSQL 12+ or MySQL 8+
✓ pip (Python package manager)
✓ Git
```

#### Installation Steps

```bash
# 1. Clone repository
git clone https://github.com/KomalLaddha-dev/E-Commerce-Data-Pipeline-with-Automated-ETL.git
cd ETL\ Automation

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your database credentials

# 5. Create databases
mysql -u root -p < warehouse_schema/source_schema.sql
psql -U postgres -f warehouse_schema/warehouse_schema.sql

# 6. Run the pipeline
python scripts/etl_pipeline.py
```

### 🔄 Option C: Apache Airflow Setup

For production scheduling and monitoring:

```bash
# 1. Install Airflow (separate from main requirements)
pip install apache-airflow[postgres]==2.7.0

# 2. Copy DAG to Airflow
export AIRFLOW_HOME=~/airflow
mkdir -p $AIRFLOW_HOME/dags
cp airflow_dags/ecommerce_etl_dag.py $AIRFLOW_HOME/dags/

# 3. Initialize Airflow database
airflow db init

# 4. Create admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# 5. Start Airflow scheduler and webserver
airflow webserver --port 8080 &
airflow scheduler &

# 6. Access at http://localhost:8080
```

## 🧪 Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test module
pytest tests/test_extract.py -v
pytest tests/test_transform.py -v
pytest tests/test_quality.py -v

# Run with coverage report
pytest tests/ --cov=scripts --cov-report=html
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file from the template:

```bash
cp .env.example .env
```

Edit `.env` with your actual values:

```bash
# Database credentials
SOURCE_DB_PASSWORD=your_source_password
DW_PASSWORD=your_warehouse_password
DW_HOST=localhost

# Optional: Airflow settings
AIRFLOW__CORE__DAGS_FOLDER=./airflow_dags
AIRFLOW__CORE__MAX_ACTIVE_RUNS=1
```

### Pipeline Configuration

Edit `config/pipeline_config.yaml` to customize:

- **Extraction:** Batch sizes, incremental vs. full reload strategies
- **Transformation:** Customer segments, currency, date formats
- **Loading:** Dimension reload strategies, fact table upsert logic
- **Quality Checks:** Data validation thresholds

Edit `config/database.yaml` for database driver selection.

## 📊 Pipeline Execution

### Run Complete Pipeline

```bash
python scripts/etl_pipeline.py
```

## 📚 Data Warehouse Schema

### Star Schema Tables

| Table | Type | Description |
|-------|------|-------------|
| `sales_fact` | Fact | One row per order with measures (amount, quantity, discount) |
| `customer_dim` | Dimension | Customer profiles with segments (Premium/Regular/New) |
| `product_dim` | Dimension | Product catalog with categories and profit margins |
| `date_dim` | Dimension | Calendar table (2024-2026) with day/month/quarter attributes |
| `payment_dim` | Dimension | Payment methods and transaction statuses |

### Pre-built Analytics Queries

The `warehouse_schema/analytics_queries.sql` file includes 12 production-ready queries:

1. Daily sales summary
2. Top 10 best selling products
3. Customer lifetime value (top 20)
4. Monthly revenue trend with growth rates
5. Payment method distribution
6. Sales by product category
7. Customer segmentation analysis
8. Weekend vs weekday comparison
9. Order status breakdown
10. Geographic sales performance
11. Quarterly performance metrics
12. Repeat customer rate analysis

Run queries directly in PostgreSQL:

```bash
psql -U etl_user -d ecommerce_dw -f warehouse_schema/analytics_queries.sql
```

## 🛠 Technology Stack

| Component | Technology |
|-----------|-----------|
| **Source Database** | MySQL / PostgreSQL |
| **ETL Scripts** | Python 3.9+, Pandas, NumPy, SQLAlchemy |
| **Data Warehouse** | PostgreSQL / Snowflake / BigQuery |
| **Orchestration** | Apache Airflow 2.7+ |
| **Visualization** | Streamlit / Power BI / Tableau |
| **Testing** | pytest, unittest |
| **Containerization** | Docker, Docker Compose |

## 📁 Project Structure

```
ETL Automation/
├── config/
│   ├── database.yaml             # Database connection settings
│   └── pipeline_config.yaml      # ETL pipeline parameters
├── data/
│   ├── raw/                      # Extracted raw CSV files
│   ├── processed/                # Transformed analytics-ready files
│   └── sample/                   # Sample datasets for testing
├── scripts/
│   ├── extract.py                # Data extraction module
│   ├── transform.py              # Data transformation module
│   ├── load.py                   # Data warehouse loading module
│   ├── etl_pipeline.py           # End-to-end pipeline runner
│   └── setup_databases.py        # Database initialization
├── airflow_dags/
│   └── ecommerce_etl_dag.py      # Airflow DAG orchestration
├── warehouse_schema/
│   ├── source_schema.sql         # Source OLTP database DDL
│   ├── warehouse_schema.sql      # Star schema DDL
│   └── analytics_queries.sql     # 12 business queries
├── dashboards/
│   ├── app.py                    # Streamlit dashboard
│   └── dashboard_specs.md        # Dashboard layout
├── tests/
│   ├── test_extract.py           # Extraction tests
│   ├── test_transform.py         # Transformation tests
│   └── test_quality.py           # Data quality tests
├── docs/
│   └── Project_Report.md         # Complete documentation
├── logs/                         # Pipeline execution logs
├── .env.example                  # Environment template
├── .gitignore                    # Git ignore rules
├── requirements.txt              # Python dependencies
├── docker-compose.yml            # Docker services
├── Dockerfile                    # ETL pipeline image
├── Dockerfile.dashboard          # Streamlit dashboard image
└── README.md                     # This file
```

## 📖 Documentation

For comprehensive documentation including architecture, schema design, and implementation examples, see:

- **[Project_Report.md](docs/Project_Report.md)** — Complete project documentation
- **[PROJECT_RUNBOOK.md](docs/PROJECT_RUNBOOK.md)** — Features, automation, run process, and technology guide
- **[pipeline_config.yaml](config/pipeline_config.yaml)** — Pipeline configuration options
- **[analytics_queries.sql](warehouse_schema/analytics_queries.sql)** — SQL query examples

## 🐛 Troubleshooting

### Docker Issues

```bash
# Remove dangling containers
docker container prune -f

# Rebuild from scratch
docker compose up --build --force-recreate

# View logs
docker compose logs -f etl
```

### Database Connection Errors

```bash
# Test PostgreSQL connection
psql -h localhost -U etl_user -d ecommerce_dw

# Test MySQL connection
mysql -h localhost -u root -p ecommerce_db
```

### Pipeline Failures

```bash
# Check logs
tail -f logs/pipeline.log

# Run with verbose output
python scripts/etl_pipeline.py --verbose
```

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure:
- Code passes all tests: `pytest tests/ -v`
- Code follows style guidelines (PEP 8)
- Docstrings are included for all functions

## 📝 License

This project is licensed under the MIT License — see [LICENSE](LICENSE) file for details.

## ✍️ Authors

**Ladd H** — Data Engineer
- GitHub: [@laddh](https://github.com/laddh)

## 🙏 Acknowledgments

- Apache Airflow documentation and community
- PostgreSQL database design best practices
- Real-world ETL patterns from industry leaders

## 📞 Support

For questions or issues:

- Open an [Issue](../../issues)
- Check existing [Discussions](../../discussions)
- Review [Project_Report.md](docs/Project_Report.md)

---

<div align="center">

**[⬆ Back to Top](#e-commerce-data-pipeline-with-automated-etl)**

Built with ❤️ for data analytics and pipeline automation

</div>
