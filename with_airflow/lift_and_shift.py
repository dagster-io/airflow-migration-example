from dagster import AssetKey, repository

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

@repository
def lift_and_shift_dags():
    return [migration_assets]
