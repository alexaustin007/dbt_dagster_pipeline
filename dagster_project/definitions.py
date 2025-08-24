from dagster import Definitions
from pathlib import Path
from dagster_dbt import dbt_assets, DbtCliResource, DbtProject
from dagster_project.jobs.retail_pipeline import retail_pipeline
from dagster_project.jobs.streaming_pipeline import streaming_pipeline
from dagster_project.jobs.asset_based_pipeline import asset_based_pipeline
from dagster_project.assets.data_loading import (
    raw_sales_data, 
    raw_stores_data, 
    raw_features_data, 
    all_raw_data_loaded
)
from dagster_project.assets.streaming_pipeline import (
    kafka_producer_status,
    spark_streaming_status,
    streaming_data_validation,
    streaming_analytics_summary,
    run_streaming_dbt,
    streaming_pipeline_job
)
from dagster_project.resources.database import mysql_resource

# Resolve absolute dbt paths so Dagster can find the project regardless of cwd
ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt_project"
MANIFEST_PATH = DBT_DIR / "target" / "manifest.json"

@dbt_assets(
    manifest=MANIFEST_PATH,
    project=DbtProject(project_dir=str(DBT_DIR), profiles_dir=str(DBT_DIR)),
)
def dbt_models(context, dbt: DbtCliResource):
    # Build all batch models (exclude streaming models to avoid conflicts)
    yield from dbt.cli(
        [
            "build",
            "--project-dir", str(DBT_DIR),
            "--profiles-dir", str(DBT_DIR),
            "--exclude", "fct_streaming_sales", "streaming",
        ],
        context=context,
    ).stream()

defs = Definitions(
    assets=[
        raw_sales_data,
        raw_stores_data,
        raw_features_data,
        all_raw_data_loaded,
        dbt_models,
        # Streaming assets
        kafka_producer_status,
        spark_streaming_status, 
        streaming_data_validation,
        streaming_analytics_summary,
        run_streaming_dbt,
    ],
    jobs=[
        retail_pipeline,
        streaming_pipeline,
        asset_based_pipeline,
        streaming_pipeline_job,
    ],
    resources={
        "mysql_resource": mysql_resource,
        "dbt": DbtCliResource(project_dir=str(DBT_DIR), profiles_dir=str(DBT_DIR)),
    },
)