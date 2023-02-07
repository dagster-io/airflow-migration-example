import requests
import pandas as pd

from dagster import asset
@asset(
    # io_manager_key="migration_io_manager",
)
def top_story_ids():
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_10_newstories = requests.get(newstories_url).json()[:10]
    return top_10_newstories

@asset(
    # io_manager_key="migration_io_manager"
)
def top_stories(top_story_ids):
    results = []
    for item_id in top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)
    df = pd.DataFrame(results)

    return df