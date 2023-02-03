from . import complete, intermediate, migrated

from dagster import Definitions, load_assets_from_package_module
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

complete_assets = load_assets_from_package_module(complete, "fully_migrated")
intermediate_assets = load_assets_from_package_module(intermediate)
airflow_assets = load_assets_from_package_module(migrated)

defs = Definitions(
    assets=[*complete_assets, *intermediate_assets, *airflow_assets],
    resources={
        "migration_io_manager": s3_pickle_io_manager.configured(
            {"s3_bucket": "dagster-sample-data", "s3_prefix": "hackernews"}
        ),
        "s3": s3_resource,
    }
)