from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json
import logging

CITY = "Saint-Petersburg"
FILE_PATH = "/tmp/weather_data.json"

def fetch_weather_data():
    api_key = Variable.get("weatherbit_api_key")
    url = f"https://api.weatherbit.io/v2.0/current?city={CITY}&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open(FILE_PATH, "w") as f:
            json.dump(data, f)
    else:
        error_message = f"Failed to fetch weather data: {response.status_code}"
        logging.error(error_message)
        raise ValueError(error_message)

def validate_weather_data():
    with open(FILE_PATH, "r") as f:
        data = json.load(f)
    if data["count"] == 0:
        raise ValueError("No weather data found")
    else:
        logging.info("Weather data found")
    temperature = data["data"][0]["temp"]
    if temperature < -50 or temperature > 50:
        raise ValueError("Temperature is out of range")
    else:
        logging.info("Temperature is in range")
    return True

def send_notification(**kwargs):
    ti = kwargs["ti"]
    is_valid = ti.xcom_pull(task_ids="validate_weather_data")
    if is_valid:
        with open(FILE_PATH, "r") as f:
            data = json.load(f)
        temp = data["data"][0]["temp"]
        logging.info(f"Задача успешно выполнена. Данные о погоде в {CITY} сохранены в файл {FILE_PATH}. Текущая температура: {temp}°C.")
    else:
        logging.error("Weather data is not valid. Notification not sent.")

with DAG(
    dag_id="my_daily_weather_report_dag",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["my"],
) as dag:
    start = EmptyOperator(task_id="start")

    fetch_weather_data = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    validate_weather_data = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_weather_data >> validate_weather_data >> send_notification >> end