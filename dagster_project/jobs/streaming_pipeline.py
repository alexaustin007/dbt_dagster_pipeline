
from dagster import job, op
import subprocess, sys, pathlib

@op
def start_stream():
    spark_job = pathlib.Path(__file__).parents[2] / "streaming" / "spark_stream_job.py"
    # For demo purposes we'll just print the command
    print(f"Run Spark: spark-submit {spark_job}")

@op
def dbt_rollup():
    subprocess.check_call(["dbt", "run", "-m", "fct_streaming_sales", "--project-dir", "dbt_project"])

@job
def streaming_pipeline():
    start_stream()
    dbt_rollup()
