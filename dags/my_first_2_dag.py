from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello, World!")

with DAG(
    dag_id="my_first_2_dag",
    schedule_interval="0 0 * * *",
    start_date=datetime(2025, 1, 21),
    catchup=False,
    tags=["my"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    hello = PythonOperator(
        task_id="hello",
        python_callable=hello,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> hello >> end
