from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator

import logging
import json


CITIES = {
    "Lviv": (49.8397, 24.0297),
    "Kyiv": (50.4501, 30.5234),
    "Odesa": (46.4825, 30.7233),
    "Kharkiv": (49.9935, 36.2304),
    "Zhmerynka": (49.0345, 28.1062)
}

def _process_weather(ti, city):
    info = ti.xcom_pull(f"extract_data_{city}")
    current_data = info["data"][0]

    timestamp = current_data["dt"] 
    temp = current_data["temp"]
    humidity = current_data["humidity"]
    clouds = current_data["clouds"]
    wind_speed = current_data["wind_speed"]

    logging.info(f"{city} - {timestamp}: {temp, humidity, clouds, wind_speed}")
    return timestamp, city, temp, humidity, clouds, wind_speed


with DAG(
    dag_id="weather_processor",
    schedule="@daily",
    start_date=datetime(2026, 3, 20),
    catchup=True,
    ) as dag:

    b_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
        sql="""CREATE TABLE IF NOT EXISTS measures (
            timestamp TIMESTAMP,
            city VARCHAR(50),
            temp FLOAT,
            humidity FLOAT,
            cloudiness FLOAT,
            wind_speed FLOAT
        );"""
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn_http",
        endpoint="data/3.0/onecall/timemachine",
        request_params={"appid": Variable.get("WEATHER_API_KEY"), "lat": 49.8, "lon": 24.0, "dt": 1711485600},
        )

    for city, coords in CITIES.items():

        extract_data = HttpOperator(
            task_id=f"extract_data_{city}",
            http_conn_id="weather_conn_http",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": coords[0],
                "lon": coords[1],
                "dt": "{{ logical_date.int_timestamp }}",
                "units": "metric"
            },
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
        )


        process_data = PythonOperator(
            task_id=f"process_data_{city}",
            python_callable=_process_weather,
            op_kwargs={'city': city}
            )
        

        inject_data = SQLExecuteQueryOperator(
            task_id=f"inject_data_{city}",
            conn_id="weather_conn",
            sql=f"""
            INSERT INTO measures (timestamp, city, temp, humidity, cloudiness, wind_speed) VALUES
            ({{{{ti.xcom_pull(task_ids='process_data_{city}')[0]}}}},
             '{{{{ti.xcom_pull(task_ids='process_data_{city}')[1]}}}}',
             {{{{ti.xcom_pull(task_ids='process_data_{city}')[2]}}}},
             {{{{ti.xcom_pull(task_ids='process_data_{city}')[3]}}}},
             {{{{ti.xcom_pull(task_ids='process_data_{city}')[4]}}}},
             {{{{ti.xcom_pull(task_ids='process_data_{city}')[5]}}}});
            """,
        )

        b_create >> check_api >> extract_data >> process_data >> inject_data
