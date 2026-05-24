from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

import logging
import json


CITIES = {
    "Lviv": (49.8397, 24.0297),
    "Kyiv": (50.4501, 30.5234),
    "Odesa": (46.4825, 30.7233),
    "Kharkiv": (49.9935, 36.2304),
    "Zhmerynka": (49.0345, 28.1062),
}

WIND_SPEED_THRESHOLD = 10.0


def _process_weather(ti, city):
    info = ti.xcom_pull(task_ids=f"extract_load_{city}.extract_data_{city}")
    current_data = info["data"][0]

    timestamp = current_data["dt"]
    temp = current_data["temp"]
    humidity = current_data["humidity"]
    clouds = current_data["clouds"]
    wind_speed = current_data["wind_speed"]

    logging.info(f"[{city}] ts={timestamp} temp={temp} hum={humidity} "
                 f"clouds={clouds} wind={wind_speed}")

    record = {
        "timestamp": timestamp,
        "city": city,
        "temp": temp,
        "humidity": humidity,
        "cloudiness": clouds,
        "wind_speed": wind_speed,
    }
    ti.xcom_push(key="weather_record", value=record)


def _check_wind(ti, city):
    record = ti.xcom_pull(
        task_ids=f"extract_load_{city}.process_data_{city}",
        key="weather_record",
    )
    wind_speed = record["wind_speed"]
    logging.info(f"[{city}] wind_speed={wind_speed}, threshold={WIND_SPEED_THRESHOLD}")

    if wind_speed > WIND_SPEED_THRESHOLD:
        return f"extract_load_{city}.alert_load_{city}"
    return f"extract_load_{city}.normal_load_{city}"


def _load_weather(ti, city, alert: bool = False):
    record = ti.xcom_pull(
        task_ids=f"extract_load_{city}.process_data_{city}",
        key="weather_record",
    )

    if alert:
        logging.warning(
            f"[ALERT] High wind in {city}! wind_speed={record['wind_speed']} m/s"
        )

    ti.xcom_push(key="load_record", value=record)



default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_processor_v2",
    schedule="@daily",
    start_date=datetime(2026, 5, 20),
    catchup=True,
    default_args=default_args,
    tags=["weather", "ucу-hw2"],
) as dag:

    b_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS measures (
                timestamp TIMESTAMP,
                city VARCHAR(50),
                temp FLOAT,
                humidity FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT
            );
        """,
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn_http",
        endpoint="data/3.0/onecall/timemachine",
        request_params={
            "appid": Variable.get("WEATHER_API_KEY"),
            "lat": 49.8,
            "lon": 24.0,
            "dt": 1711485600,
        },
        timeout=20,
        poke_interval=10,
    )

    for city, coords in CITIES.items():

        with TaskGroup(group_id=f"extract_load_{city}") as city_group:

            extract_data = HttpOperator(
                task_id=f"extract_data_{city}",
                http_conn_id="weather_conn_http",
                endpoint="data/3.0/onecall/timemachine",
                data={
                    "appid": Variable.get("WEATHER_API_KEY"),
                    "lat": coords[0],
                    "lon": coords[1],
                    "dt": "{{ logical_date.int_timestamp }}",
                    "units": "metric",
                },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True,
            )

            process_data = PythonOperator(
                task_id=f"process_data_{city}",
                python_callable=_process_weather,
                op_kwargs={"city": city},
            )

            branch = BranchPythonOperator(
                task_id=f"check_wind_{city}",
                python_callable=_check_wind,
                op_kwargs={"city": city},
            )

            # normal path
            normal_load = PythonOperator(
                task_id=f"normal_load_{city}",
                python_callable=_load_weather,
                op_kwargs={"city": city, "alert": False},
            )

            # alert
            alert_load = PythonOperator(
                task_id=f"alert_load_{city}",
                python_callable=_load_weather,
                op_kwargs={"city": city, "alert": True},
            )

            inject_data = SQLExecuteQueryOperator(
                task_id=f"inject_data_{city}",
                conn_id="weather_conn",
                sql=f"""
                    INSERT INTO measures (timestamp, city, temp, humidity, cloudiness, wind_speed)
                    VALUES (
                        {{{{ti.xcom_pull(
                            task_ids='extract_load_{city}.process_data_{city}',
                            key='weather_record'
                        )['timestamp']}}}},
                        '{{{{ti.xcom_pull(
                            task_ids='extract_load_{city}.process_data_{city}',
                            key='weather_record'
                        )['city']}}}}',
                        {{{{ti.xcom_pull(
                            task_ids='extract_load_{city}.process_data_{city}',
                            key='weather_record'
                        )['temp']}}}},
                        {{{{ti.xcom_pull(
                            task_ids='extract_load_{city}.process_data_{city}',
                            key='weather_record'
                        )['humidity']}}}},
                        {{{{ti.xcom_pull(
                            task_ids='extract_load_{city}.process_data_{city}',
                            key='weather_record'
                        )['cloudiness']}}}},
                        {{{{ti.xcom_pull(
                            task_ids='extract_load_{city}.process_data_{city}',
                            key='weather_record'
                        )['wind_speed']}}}});
                """,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            extract_data >> process_data >> branch
            branch >> [normal_load, alert_load] >> inject_data

        b_create >> check_api >> city_group