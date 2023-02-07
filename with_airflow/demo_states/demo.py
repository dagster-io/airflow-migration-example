from dagster import AssetKey, asset

from dagster_airflow import load_assets_from_airflow_dag

from with_airflow.airflow_migration_dag import migration_dag

import requests

@asset(
    io_manager_key="migration_io_manager",
    group_name="partially_migrated"
)
def top_story_ids():
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_10_newstories = requests.get(newstories_url).json()[:10]
    return top_10_newstories

migration_assets = load_assets_from_airflow_dag(
    migration_dag,
    task_ids_by_asset_key={
        AssetKey("airflow_top_stories"): {"load_top_stories"}
    },
    upstream_dependencies_by_asset_key={
        AssetKey("airflow_top_stories"): {AssetKey("top_story_ids")}
    }
)