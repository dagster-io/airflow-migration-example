# State 1
from dagster import AssetKey

from dagster_airflow import load_assets_from_airflow_dag

from with_airflow.airflow_migration_dag import migration_dag

migration_assets = load_assets_from_airflow_dag(
    migration_dag,
    task_ids_by_asset_key={
        AssetKey("airflow_top_story_ids"): {"load_top_story_ids"},
        AssetKey("airflow_top_stories"): {"load_top_stories"}
    },
    upstream_dependencies_by_asset_key={
        AssetKey("airflow_top_stories"): {AssetKey("airflow_top_story_ids")}
    }
)



# State 2
from dagster import AssetKey

from dagster_airflow import load_assets_from_airflow_dag

from with_airflow.airflow_migration_dag import migration_dag

def _load_top_story_ids(ds, **kwargs):
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_10_newstories = requests.get(newstories_url).json()[:10]
    top_10_newstories_strings = ','.join(str(x) for x in top_10_newstories)
    s3.put_object(Body=top_10_newstories_strings, Bucket='dagster-sample-data', Key='hacker_news_500_top_story_ids.txt')

migration_assets = load_assets_from_airflow_dag(
    migration_dag,
    task_ids_by_asset_key={
        AssetKey("airflow_top_story_ids"): {"load_top_story_ids"},
        AssetKey("airflow_top_stories"): {"load_top_stories"}
    },
    upstream_dependencies_by_asset_key={
        AssetKey("airflow_top_stories"): {AssetKey("airflow_top_story_ids")}
    }
)



# State 3
from dagster import AssetKey, asset

from dagster_airflow import load_assets_from_airflow_dag

from with_airflow.airflow_migration_dag import migration_dag

@asset(
    io_manager_key="migration_io_manager",
    group_name="partially_migrated"
)
def _load_top_story_ids(ds, **kwargs):
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_10_newstories = requests.get(newstories_url).json()[:10]
    top_10_newstories_strings = ','.join(str(x) for x in top_10_newstories)
    s3.put_object(Body=top_10_newstories_strings, Bucket='dagster-sample-data', Key='hacker_news_500_top_story_ids.txt')

migration_assets = load_assets_from_airflow_dag(
    migration_dag,
    task_ids_by_asset_key={
        AssetKey("airflow_top_story_ids"): {"load_top_story_ids"},
        AssetKey("airflow_top_stories"): {"load_top_stories"}
    },
    upstream_dependencies_by_asset_key={
        AssetKey("airflow_top_stories"): {AssetKey("airflow_top_story_ids")}
    }
)

# State 4
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
        AssetKey("airflow_top_story_ids"): {"load_top_story_ids"},
        AssetKey("airflow_top_stories"): {"load_top_stories"}
    },
    upstream_dependencies_by_asset_key={
        AssetKey("airflow_top_stories"): {AssetKey("airflow_top_story_ids")}
    }
)



# State 5
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



