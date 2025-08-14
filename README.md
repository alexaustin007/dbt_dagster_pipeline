
# Retail Data Pipeline

A complete end-to-end ETL system demonstrating modern data stack practices with local batch and streaming processing.

## Tech Stack
- **Storage**: MySQL 8 (local database)
- **Orchestration**: Dagster (asset-based pipeline management)
- **Transformations**: dbt-mysql (staging → marts)
- **Batch Processing**: Python + pandas
- **Streaming**: Kafka + Spark Structured Streaming (optional demo)
- **Analysis**: Jupyter notebooks + BI tools

## Architecture

```
data/raw/*.csv → Dagster Assets → MySQL raw_* tables → 
dbt staging views → dbt intermediate views → dbt mart tables → Analytics
```

### Data Flow
1. **Raw Data**: CSV files in `data/raw/` (sales.csv, stores.csv, features.csv)
2. **Ingestion**: Dagster assets load CSVs → MySQL (`raw_sales`, `raw_stores`, `raw_features`)
3. **Staging**: dbt creates clean views (`stg_*`) from raw tables
4. **Transformations**: dbt intermediate models (`int_*`) perform joins and business logic
5. **Marts**: dbt final analytics tables (`fct_sales_summary`, `fct_streaming_sales`)

## Quick Start

### Prerequisites
- Python 3.9+
- MySQL 8.0+ running locally
- Java 8+ (for Spark streaming demo only)

### Setup
```bash
# Clone and setup environment
cd retail_data_pipeline_full
source venv/bin/activate  # Virtual environment already created
pip install -r requirements.txt

# Ensure MySQL is running and database exists
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS retail_analytics;"
```

### Run the Pipeline

#### Option 1: Complete Dagster-Orchestrated Pipeline (Recommended)
```bash
# 1. Start Dagster development server
dagster dev -f dagster_project/repository.py

# 2. Open Dagster UI (http://localhost:3000)
# 3. Materialize data ingestion assets:
#    - raw_sales_data
#    - raw_stores_data  
#    - raw_features_data

# 4. Run dbt transformations
cd dbt_project
dbt run

# 5. Query final results
mysql -u root -p retail_analytics -e "SELECT * FROM fct_sales_summary LIMIT 10;"
```

#### Option 2: Manual Step-by-Step
```bash
# 1. Manual ingestion (bypasses Dagster)
python ingestion/from_file.py --db_user root --db_password <password> --db_name retail_analytics

# 2. Run dbt transformations
cd dbt_project
dbt run

# 3. Start Dagster for monitoring (optional)
dagster dev -f dagster_project/repository.py
```

### Streaming Demo (Optional)
```bash
# Terminal 1: Start Kafka
brew services start kafka  # macOS
# or docker-compose up kafka  # Docker

# Terminal 2: Generate streaming events
python streaming/kafka_producer.py

# Terminal 3: Process streaming data
spark-submit streaming/spark_stream_job.py
```

## Project Structure

```
├── data/raw/                   # Source CSV files
├── dagster_project/            # Orchestration
│   ├── assets/data_loading.py  # CSV ingestion assets
│   └── jobs/                   # Pipeline definitions
├── dbt_project/                # Transformations
│   ├── models/staging/         # Clean raw data (views)
│   ├── models/intermediate/    # Business logic (views)
│   └── models/marts/           # Final analytics (tables)
├── ingestion/                  # Data loading utilities
├── streaming/                  # Kafka + Spark demo
└── CLAUDE.md                   # Detailed development guide
```

## Database Schema

### Raw Tables (MySQL)
- `raw_sales` - Weekly sales by store/department
- `raw_stores` - Store metadata (type, size)
- `raw_features` - Economic indicators and promotions

### dbt Models
- **Staging**: `stg_sales`, `stg_stores`, `stg_features` (views)
- **Intermediate**: `int_daily_sales`, `int_store_performance` (views)
- **Marts**: `fct_sales_summary`, `fct_streaming_sales` (tables)

## Key Features

- **Incremental Loading**: Composite primary keys with upsert strategy
- **Data Lineage**: Full dependency tracking via Dagster + dbt
- **Modular Design**: Separate ingestion, transformation, and analysis layers
- **Streaming Support**: Real-time event processing with Kafka + Spark
- **Modern Stack**: Asset-based orchestration with comprehensive monitoring

## Development

See `CLAUDE.md` for detailed development instructions, including:
- Environment setup
- Pipeline execution workflows
- Data lineage mapping
- Troubleshooting guides

## Configuration

- `.env` - Database credentials
- `dbt_project/profiles.yml` - dbt connection settings
- `dagster_project/dagster.yaml` - Orchestration configuration
