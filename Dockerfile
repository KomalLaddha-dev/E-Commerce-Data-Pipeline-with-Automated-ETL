FROM python:3.11-slim

WORKDIR /app

# Install only the packages needed to run the ETL pipeline
RUN pip install --no-cache-dir \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    sqlalchemy>=2.0.0 \
    psycopg2-binary>=2.9.0 \
    pyyaml>=6.0.0

# Copy project source files
COPY scripts/ ./scripts/
COPY config/ ./config/
COPY warehouse_schema/ ./warehouse_schema/

# Create data and log directories
RUN mkdir -p data/raw data/processed data/sample logs

# Setup databases, then run full ETL pipeline
CMD ["sh", "-c", "python scripts/setup_databases.py && python scripts/etl_pipeline.py"]
