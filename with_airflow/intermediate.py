import requests
import pandas as pd

from dagster import AssetKey, AssetIn, asset, repository, with_resources

from dagster_airflow import load_assets_from_airflow_dag
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

from with_airflow.airflow_migration_dag_int import migration_dag

migration_assets = load_assets_from_airflow_dag(
    migration_dag,
    task_ids_by_asset_key={
        AssetKey("int_airflow_top_story_ids"): {"load_top_story_ids"}
    }
)

@asset(
    ins={"top_story_ids": AssetIn("int_airflow_top_story_ids")},
    io_manager_key="migration_io_manager",
    group_name="partially_migrated"
)
def int_top_stories(top_story_ids):
    results = []
    for item_id in top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)
    df = pd.DataFrame(results)

    return df

@repository
def intermediate():
    return with_resources(
        [*migration_assets, int_top_stories],
        resource_defs={
            "migration_io_manager": s3_pickle_io_manager.configured(
                {"s3_bucket": "dagster-sample-data", "s3_prefix": "hackernews"}
            ),
            "s3": s3_resource,
        }
    )