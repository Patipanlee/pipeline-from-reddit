import praw # type: ignore
from datetime import datetime
from airflow import DAG
from airflow.models import Variable # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.utils import timezone # type: ignore

import great_expectations as gx # type: ignore
import numpy as np
import pandas as pd
import requests
from great_expectations.dataset import PandasDataset # type: ignore
 
def _get_date_reddit(**context):
    client_id = Variable.get("client_id")
    client_secret = Variable.get("client_secret")
    user_agent="api-pipeline-523 1.0"
    
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent)

    reddit.read_only = True
    
    sub = "dataengineering"
    subreddit = reddit.subreddit(sub)
    get_data = []

    for readit in subreddit.new(limit=100):
        raw = {
        "timestamp":int(readit.created_utc),
        "author":readit.author.name,
        "id":readit.id,
        "title":readit.title,
        "score":readit.score
        }

        get_data.append(raw)
    raw_data = pd.DataFrame(get_data)

    return raw_data

def _vaildate_notnull(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="get_date_reddit", key="return_value")

    df = pd.DataFrame.from_records(raw_data)
    dataset = PandasDataset(df)
    
    results1 = dataset.expect_column_values_to_not_be_null(column='timestamp')
    results2 = dataset.expect_column_values_to_not_be_null(column='author')
    results3 = dataset.expect_column_values_to_not_be_null(column='id')
    results4 = dataset.expect_column_values_to_not_be_null(column='title')
    results5 = dataset.expect_column_values_to_not_be_null(column='score')
    re = results1["success"]+results2["success"]+results3["success"]+results4["success"]+results5["success"]
    
    assert re == 5

def _create_table(**context):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS reddit (
            date DATE NOT NULL,
            time TIME NOT NULL,
            author VARCHAR(99) NOT NULL,
            id VARCHAR(99) NOT NULL,
            title VARCHAR(500) NOT NULL,
            score int NOT NULL
        )
    """
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres(**context):

    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="get_date_reddit", key="return_value")
    d = pd.DataFrame(raw_data)
    d['title'] = d['title'].str.replace("'", "")
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for i in range(len(d)):
        stamp = datetime.utcfromtimestamp(d["timestamp"][i])
        a1 = stamp.strftime("%d/%m/%Y")
        a2 = stamp.strftime("%H:%M:%S")        
        a3 = d["author"][i]
        a4 = d["id"][i]
        a5 = d["title"][i]
        a6 = d["score"][i]

        sql = f"""
        INSERT INTO reddit (date,time,author,id,title,score) VALUES ('{a1}','{a2}','{a3}','{a4}','{a5}','{a6}')
        """
        cursor.execute(sql)
    connection.commit()

with DAG(
    "reddit_api_pipeline",
    start_date=timezone.datetime(2024, 5, 10),
    schedule="0 0 * * *",
):
    
    start = EmptyOperator(task_id="start")

    get_date_reddit = PythonOperator(
        task_id="get_date_reddit",
        python_callable=_get_date_reddit,
    )

    vaildate_notnull = PythonOperator(
        task_id="vaildate_notnull",
        python_callable=_vaildate_notnull,
    )

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=_create_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> get_date_reddit >> vaildate_notnull >> create_table >> load_data_to_postgres >> end