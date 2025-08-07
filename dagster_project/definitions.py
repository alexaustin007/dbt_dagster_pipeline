from dagster import Definitions
from dagster_project.jobs.retail_pipeline import retail_pipeline
from dagster_project.jobs.streaming_pipeline import streaming_pipeline
from dagster_project.jobs.asset_based_pipeline import asset_based_pipeline
from dagster_project.assets.data_loading import (
    raw_sales_data, 
    raw_stores_data, 
    raw_features_data, 
    all_raw_data_loaded
)
from dagster_project.resources.database import mysql_resource

defs = Definitions(
    assets=[
        raw_sales_data,
        raw_stores_data, 
        raw_features_data,
        all_raw_data_loaded
    ],
    jobs=[
        retail_pipeline, 
        streaming_pipeline,
        asset_based_pipeline
    ],
    resources={
        "mysql_resource": mysql_resource
    }
) 