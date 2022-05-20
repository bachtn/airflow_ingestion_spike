from datetime import datetime
from datetime import timedelta

import pandas as pd
from airflow.decorators import dag, task
from pendulum import today


@dag(
    dag_id="ingest_data",
    description="Ingest data from a file to another DAG",
    tags=["example"],
    default_args={'owner': 'airflow'},
    schedule_interval=timedelta(minutes=1),
    start_date=today().add(hours=-1)
)
def ingest_data():
    @task
    def get_data_to_ingest_from_local_file_task():
        return get_data_to_ingest_from_local_file()

    @task
    def save_data_task(data_to_ingest_df):
        save_data(data_to_ingest_df)

    # Task relationships
    data_to_ingest = get_data_to_ingest_from_local_file_task()
    save_data_task(data_to_ingest)


ingest_data_dag = ingest_data()


#####
def get_data_to_ingest_from_local_file():
    input_data_df = pd.read_csv("input_data/power_plants.csv")
    data_to_ingest_df = input_data_df.sample(n=5)
    return data_to_ingest_df


def save_data(data_to_ingest_df):
    filepath = f"output_data/{datetime.now()}.csv"
    data_to_ingest_df.to_csv(filepath, index=False)
