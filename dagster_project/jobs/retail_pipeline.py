
from dagster import job, op
import subprocess, sys, pathlib

@op
def ingest_files():
    script = pathlib.Path(__file__).parents[2] / "ingestion" / "from_file.py"
    subprocess.check_call([sys.executable, str(script)])

@op
def dbt_run():
    subprocess.check_call(["dbt", "run", "--project-dir", "dbt_project"])

@job
def retail_pipeline():
    ingest_files()
    dbt_run()
