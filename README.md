
# Retail Data Pipeline

A local batch + streaming ETL pipeline using:
- MySQL
- dbt
- Dagster
- Spark (optional)
- Kafka (for streaming demo)

## Getting Started

1. Install Python 3.9+ and Java (for Spark).
2. `pip install -r requirements.txt`
3. Start MySQL locally and run scripts in `mysql/`
4. Place CSVs in `data/raw/` and run: `python ingestion/from_file.py`
5. Run Dagster dev server: `dagster dev -f dagster_project/repository.py`
6. For streaming demo:
   - Start Kafka locally (`brew services start kafka` or similar)
   - Run `python streaming/kafka_producer.py`
   - Run `spark-submit streaming/spark_stream_job.py`
7. `dbt run` will build models. Explore results with your BI tool.
