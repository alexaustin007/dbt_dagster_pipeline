# 🏬 Retail Data Pipeline — Local Batch + Streaming ETL

A fully‑local data‑engineering sandbox that lets you practise **end‑to‑end pipelines** using open‑source tools only:

| Layer            | Tool(s)                               | Role |
|------------------|---------------------------------------|------|
| Storage          | **MySQL 8** (local)                   | Raw, staging & marts |
| Batch Ingestion  | **Python + pandas**                   | CSV / API → MySQL |
| Batch Processing | **PySpark** (optional)                | Heavy cleans / dedupe |
| Transformations  | **dbt‑mysql**                         | Staging → Marts |
| Orchestration    | **Dagster**                           | Run & monitor jobs |
| Streaming Demo   | **Kafka ▸ Spark Structured Streaming**| Real‑time events |
| Exploration      | **Jupyter Notebook** / BI tool        | Ad‑hoc queries |

---

## 📂 Directory Layout (high‑level)

```text
retail_data_pipeline/
├─ data/                     # ⇢ Drop raw CSV/JSON files here
│  ├─ raw/
│  └─ processed/
├─ ingestion/                # Python loaders (file & API)
├─ spark_jobs/               # Batch Spark scripts
├─ streaming/                # Kafka producer + Spark stream
├─ mysql/                    # One‑off init SQL for known tables
├─ dbt_project/              # dbt models (staging → marts)
├─ dagster_project/          # Dagster jobs & repo
├─ notebooks/                # Analysis / EDA
├─ requirements.txt          # Python deps
├─ .env                      # DB creds & config
└─ PROJECT_OVERVIEW.md       # ← you are here
🔄 End‑to‑End Flow
1 ▪ Batch ETL (default)

data/raw/*.csv
        │
        ▼  (pandas infers schema)
ingestion/from_file.py
        │   stg_<file>.csv  ← auto‑created
        ▼
      MySQL  ──┐
               │ dbt run
               ▼
      MySQL (marts)  → notebooks / BI
2 ▪ Streaming Demo (optional)

Kafka producer  → topic=sales_events
        ▼
Spark Structured Streaming
        ▼
MySQL.stream_sales_events
        ▼  dbt run -m fct_streaming_sales
MySQL.fct_streaming_sales (hourly rolls)
Dagster drives both pipelines:


dagster dev  →  retail_pipeline  (batch)
            ↳  streaming_pipeline (stream + roll‑up)
🚀 Quick Start
Install prerequisites


# Python 3.9+, Java 8+, MySQL 8+
pip install -r requirements.txt
Create & seed MySQL


mysql -u root -p < mysql/init_schema.sql
mysql -u root -p < mysql/init_stream_schema.sql   # when streaming
Batch ingest CSVs


# place sales.csv in data/raw/
python ingestion/from_file.py --db_user root --db_password <pw>
Run dbt models


cd dbt_project
dbt run
dbt test
Start Dagster UI


dagster dev -f dagster_project/repository.py
# kick off `retail_pipeline` from the UI
Streaming demo (optional)

Start Kafka locally (e.g., bin/kafka-server-start.sh …).

Produce events:


python streaming/kafka_producer.py
In another terminal run the Spark stream job:


spark-submit streaming/spark_stream_job.py
Trigger the Dagster streaming_pipeline to roll up hourly metrics.

🛠 Configuration
File	Purpose
.env	MySQL host/user/pw for local runs
dbt_project/profiles.yml	dbt connection profile
dagster_project/dagster.yaml	Dagster instance settings


additionally install everything inside a python virtual environment of venv
/Users/alexaustinchettiar/Downloads/retail_data_pipeline_full

