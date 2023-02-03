from __future__ import annotations

import pendulum
import requests
import boto3
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

s3 = boto3.client('s3')

with DAG(
    dag_id="from_airflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="0 0 * * *",
    catchup=False,
) as migration_dag:

    def _load_top_story_ids(ds, **kwargs):
      newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
      top_10_newstories = requests.get(newstories_url).json()[:10]
      top_10_newstories_strings = ','.join(str(x) for x in top_10_newstories)
      s3.put_object(Body=top_10_newstories_strings, Bucket='dagster-sample-data', Key='hacker_news_500_top_story_ids.txt')
    
    load_top_story_ids = PythonOperator(
        task_id="load_top_story_ids",
        python_callable=_load_top_story_ids,
    )
    
    
    def _load_top_stories(ds, **kwargs):
      obj = s3.get_object(Bucket='dagster-sample-data', Key='hacker_news_500_top_story_ids.txt')
      hackernews_topstory_ids = obj['Body'].read()
      print(hackernews_topstory_ids)
      results = []
      for item_id in hackernews_topstory_ids:
          item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
          results.append(item)
      df = pd.DataFrame(results)
      output = df.to_csv()

      print(output)

      s3.put_object(Body=df.to_csv(), Bucket='dagster-sample-data', Key='hacker_news_500_top_stories.csv')
      
    
    load_top_stories = PythonOperator(
        task_id="load_top_stories",
        python_callable=_load_top_stories,
    )

    load_top_story_ids >> load_top_stories