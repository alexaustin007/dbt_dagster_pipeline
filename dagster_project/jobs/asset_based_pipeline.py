from dagster import job, op, Config
from dagster_project.assets.data_loading import all_raw_data_loaded
import subprocess

class DbtConfig(Config):
    dbt_project_dir: str = "dbt_project"

@op
def run_dbt_transformations(context):
    """Run dbt transformations after raw data is loaded."""
    config = context.op_config
    subprocess.check_call(["dbt", "run", "--project-dir", config.dbt_project_dir])
    return "dbt transformations completed successfully"

@job(
    description="Asset-based pipeline that loads raw data and runs dbt transformations"
)
def asset_based_pipeline():
    """Pipeline that runs dbt transformations after raw data assets are materialized."""
    run_dbt_transformations() 