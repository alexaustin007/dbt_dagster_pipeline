
from dagster import job, op, In, Out, Nothing
import subprocess, sys, pathlib

@op(out=Out(Nothing))
def ingest_files():
    script = pathlib.Path(__file__).parents[2] / "ingestion" / "from_file.py"
    subprocess.check_call([sys.executable, str(script)])

@op(ins={"start": In(Nothing)})
def dbt_run():
    project_dir = pathlib.Path(__file__).parents[2] / "dbt_project"
    subprocess.check_call(["dbt", "run", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir), "--exclude", "fct_streaming_sales"])

@job
def retail_pipeline():
    dbt_run(start=ingest_files())
