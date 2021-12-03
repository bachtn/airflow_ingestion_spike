from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago



@dag(
    dag_id="example_2",
    description="Example 2 DAG",
    tags=["ingestion"],
    default_args={'owner': 'airflow'},
    schedule_interval=timedelta(minutes=2),
    start_date=days_ago(n=0, hour=1)
)
def my_dag_example():
    @task
    def task_1():
        return ingest_file()

    @task
    def task_2(x):
        return x + 1

    # Task relationships
    x = task_1()
    y = task_2(x=x)
    print(y)


example_dag = my_dag_example()



#####
def ingest_file():
    return 3