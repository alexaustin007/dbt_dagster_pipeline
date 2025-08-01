
from dagster import repository
from jobs.retail_pipeline import retail_pipeline
from jobs.streaming_pipeline import streaming_pipeline

@repository
def retail_repo():
    return [retail_pipeline, streaming_pipeline]
