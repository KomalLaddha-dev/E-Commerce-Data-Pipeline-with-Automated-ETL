# Apache Airflow Setup Guide

## ⚠️ Important: Airflow Installation Challenges on Windows

Apache Airflow has complex dependency requirements, especially on Windows. Here are the recommended approaches:

---

## Option 1: Use Docker (RECOMMENDED for Windows)

The easiest way to run Airflow on Windows is using Docker:

```bash
# Start Docker services including Airflow
docker compose up --build

# This automatically:
# ✓ Runs the ETL pipeline
# ✓ Sets up PostgreSQL
# ✓ Runs data validation
```

The Docker setup includes everything you need—no manual Airflow configuration required.

---

## Option 2: Windows Subsystem for Linux (WSL2) — BEST for Production

For a production-like environment on Windows:

```bash
# 1. Enable WSL2
# In PowerShell (as Admin):
wsl --install
wsl --set-default-version 2

# 2. Install Linux distribution (Ubuntu)
# Download from Microsoft Store or command line

# 3. Inside WSL2/Ubuntu terminal:
sudo apt-get update
sudo apt-get install python3.10 python3.10-venv python3-pip postgresql

# 4. Create virtual environment
python3.10 -m venv airflow_env
source airflow_env/bin/activate

# 5. Install Airflow (in WSL2, this works smoothly)
pip install apache-airflow==2.7.0 \
  apache-airflow-providers-postgres==5.10.0 \
  apache-airflow-providers-docker==3.7.0

# 6. Export environment variables
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow:airflow@localhost/airflow

# 7. Initialize Airflow database
airflow db init

# 8. Create admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin123

# 9. Copy DAG
mkdir -p ~/airflow/dags
cp /path/to/ETL\ Automation/airflow_dags/ecommerce_etl_dag.py ~/airflow/dags/

# 10. Start services
airflow webserver --port 8080 &
airflow scheduler &

# 11. Access at http://localhost:8080
```

---

## Option 3: Create Virtual Environment (Advanced)

If you need Airflow on native Windows:

```bash
# 1. Create a dedicated virtual environment
python -m venv airflow_venv
airflow_venv\Scripts\activate

# 2. Upgrade pip/setuptools/wheel
python -m pip install --upgrade pip setuptools wheel

# 3. Set constraint file (CRITICAL for Windows)
set AIRFLOW_VERSION=2.7.0
set PYTHON_VERSION="3.9"  # Must use 3.9-3.11 on Windows, not 3.12+
set CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 4. Install Airflow with constraints
pip install "apache-airflow==${AIRFLOW_VERSION}" `
  --constraint "${CONSTRAINT_URL}"

# 5. Initialize database
$env:AIRFLOW_HOME="$PWD\airflow"
airflow db init

# 6. Create admin user
airflow users create `
  --username admin `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com

# 7. Copy DAG file
mkdir airflow/dags -ErrorAction SilentlyContinue
Copy-Item airflow_dags/ecommerce_etl_dag.py airflow/dags/

# 8. Start Airflow
airflow webserver --port 8080
# In another terminal:
airflow scheduler
```

---

## Option 4: Use Airflow with Anaconda (Easiest for Windows)

```bash
# 1. Create conda environment
conda create -n airflow python=3.10

# 2. Activate environment
conda activate airflow

# 3. Install Airflow from conda
conda install -c conda-forge airflow=2.7.0

# 4. Set Airflow home
set AIRFLOW_HOME=%USERPROFILE%\airflow

# 5. Initialize database
airflow db init

# ... rest of setup (see above)
```

---

## Setting Up the ETL DAG

### 1. Set Environment Variables

Create a `.airflow_env` file in your Airflow home:

```bash
# DATABASE CONNECTIONS
export SOURCE_DB_HOST=localhost
export SOURCE_DB_PORT=3306
export SOURCE_DB_USER=etl_service
export SOURCE_DB_PASSWORD=your_source_password
export SOURCE_DB_NAME=ecommerce_db

export DW_HOST=localhost
export DW_PORT=5432
export DW_USER=warehouse_user
export DW_PASSWORD=your_dw_password
export DW_NAME=ecommerce_dw

# AIRFLOW CONFIG
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/ETL\ Automation/airflow_dags
```

### 2. Copy DAG File

```bash
cp airflow_dags/ecommerce_etl_dag.py $AIRFLOW_HOME/dags/
```

### 3. Verify DAG is Recognized

```bash
airflow dags list
# Should show: ecommerce_etl_pipeline
```

### 4. Test DAG

```bash
# Validate DAG syntax
airflow dags validate

# List all tasks
airflow tasks list ecommerce_etl_pipeline

# Test a specific task (dry run)
airflow tasks test ecommerce_etl_pipeline extract_data 2026-03-18
```

---

## Starting Airflow

### Standalone Mode (Single Process)

```bash
airflow standalone
# Access: http://localhost:8080
# Auto-creates admin user with password shown in logs
```

### Traditional Mode (Separate Services)

```bash
# Terminal 1: Webserver (UI)
airflow webserver --port 8080

# Terminal 2: Scheduler (runs DAGs)
airflow scheduler

# Terminal 3: Flower (monitoring - optional)
airflow celery flower --port 5555
```

---

## DAG Schedule & Monitoring

The `ecommerce_etl_pipeline` DAG:

- **Runs:** Every day at **02:00 UTC**
- **Retries:** 3 attempts with 5-minute delays
- **Tasks:** Extract → Transform → Load → Validate → Refresh Views

### Monitor in Airflow UI

1. Open http://localhost:8080
2. Login with credentials created above
3. Click `ecommerce_etl_pipeline`
4. View:
   - DAG structure (Timetable view)
   - Recent runs (Graph view)
   - Logs (Log view)
   - Task dependencies

### Manual Trigger for Testing

```bash
# Trigger DAG immediately
airflow dags trigger ecommerce_etl_pipeline

# Trigger with specific execution date
airflow dags trigger ecommerce_etl_pipeline \
  --exec-date 2026-03-18
```

---

## Troubleshooting

### Airflow won't start on Windows

**Problem:** "ModuleNotFoundError" or dependency issues

**Solution:**
- Use Docker instead (Option 1)
- Use WSL2 (Option 2) — Best for Windows
- Use Python 3.9-3.11, NOT 3.12+

### DAG not appearing in Airflow

**Check:**
```bash
airflow dags list
airflow dags validate
```

**Fix:**
- Ensure DAG file is in `$AIRFLOW_HOME/dags/`
- No syntax errors in DAG file
- Restart scheduler: `airflow scheduler`

### Database connection errors in Airflow

**Check environment variables:**
```bash
# In Airflow DAG terminal
set | grep SOURCE_DB
set | grep DW_
```

**Verify databases are running:**
```bash
# PostgreSQL
psql -U etl_user -h localhost -d ecommerce_dw

# MySQL
mysql -u etl_service -h localhost -p ecommerce_db
```

### Airflow task fails with import errors

**Solution:**
Make sure the project root is in Python path:

```python
# In ecommerce_etl_dag.py, add at top:
import sys
sys.path.insert(0, '/path/to/ETL\ Automation')
```

---

## Quick Reference: Common Commands

```bash
# List all DAGs
airflow dags list

# Validate DAG syntax
airflow dags validate

# List tasks in a DAG
airflow tasks list ecommerce_etl_pipeline

# Trigger DAG run
airflow dags trigger ecommerce_etl_pipeline

# Check DAG runs
airflow dags list-runs ecommerce_etl_pipeline

# View task logs
airflow tasks logs ecommerce_etl_pipeline extract_data <execution_date>

# Clear task instance
airflow tasks clear ecommerce_etl_pipeline

# Reset database
airflow db reset
```

---

## Recommended: Docker + Airflow

For the smoothest experience, use Docker:

```bash
# 1. Ensure Docker is running
docker --version

# 2. From ETL Automation directory
docker compose up --build

# 3. This automatically:
#    - Runs ETL pipeline
#    - Sets up PostgreSQL
#    - Completes in ~5-10 minutes

# 4. View logs
docker logs -f etl_pipeline
```

---

## Next Steps

1. **Choose Option 1 (Docker)** — Simplest, most reliable
2. **Or choose Option 2 (WSL2)** — Most production-like
3. **Test the DAG** — `airflow dags validate`
4. **Monitor via Web UI** — http://localhost:8080
5. **Review logs** — `$AIRFLOW_HOME/logs/`

Happy Airflow! 🚀
