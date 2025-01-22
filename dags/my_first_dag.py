from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="my_first_dag",
    schedule_interval="0 0 * * *",
    start_date=datetime(2025, 1, 21),
    catchup=False,
    tags=["my"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> end
