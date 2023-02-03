import requests
import pandas as pd

from dagster import asset, repository, with_resources
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

@asset(group_name="fully_migrated")
def top_story_ids():
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_10_newstories = requests.get(newstories_url).json()[:10]
    return top_10_newstories

@asset(group_name="fully_migrated")
def top_stories(top_story_ids):
    results = []
    for item_id in top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)
    df = pd.DataFrame(results)

    return df

@repository
def migrated_assets():
    return with_resources(
        [top_story_ids, top_stories],
        resource_defs={
            "io_manager": s3_pickle_io_manager.configured(
                {"s3_bucket": "dagster-sample-data", "s3_prefix": "hackernews"}
            ),
            "s3": s3_resource,
        }
    )