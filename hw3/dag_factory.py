from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

CITIES: dict[str, tuple[float, float]] = {
    "Lviv": (49.8397, 24.0297),
    "Kyiv": (50.4501, 30.5234),
    "Odesa": (46.4825, 30.7233),
    "Kharkiv": (49.9935, 36.2304),
    "Zhmerynka": (49.0345, 28.1062),
}

GCS_BUCKET = "weather-hw3"
GCS_CONN = "google_cloud_default"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "email_on_failure": False,
}

DAG_PARAMS = {
    "wind_threshold": Param(10.0, type="number", description="Wind speed alert threshold in m/s"),
}


def _gcs_hook():
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    return GCSHook(gcp_conn_id=GCS_CONN)


def _write_latest(city: str, ds: str) -> None:
    _gcs_hook().upload(
        bucket_name=GCS_BUCKET,
        object_name=f"raw/{city}/.latest",
        data=ds,
        mime_type="text/plain",
    )


def _read_latest(city: str) -> str:
    data = _gcs_hook().download(
        bucket_name=GCS_BUCKET,
        object_name=f"raw/{city}/.latest",
    )
    return data.decode().strip()


def _extract_weather(city: str, lat: float, lon: float, ds: str, **context) -> None:
    import requests

    hook = _gcs_hook()
    raw_object = f"raw/{city}/{ds}.json"

    if hook.exists(bucket_name=GCS_BUCKET, object_name=raw_object):
        logging.info(f"[{city}][{ds}] Raw file already in GCS - skipping extract.")
        _write_latest(city, ds)
        return

    api_key = Variable.get("WEATHER_API_KEY")
    logical_ts = int(context["logical_date"].timestamp())

    response = requests.get(
        "https://api.openweathermap.org/data/3.0/onecall/timemachine",
        params={"appid": api_key, "lat": lat, "lon": lon, "dt": logical_ts, "units": "metric"},
        timeout=30,
    )
    response.raise_for_status()

    hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=raw_object,
        data=response.text,
        mime_type="application/json",
    )
    _write_latest(city, ds)
    logging.info(f"[{city}][{ds}] Raw data saved to gs://{GCS_BUCKET}/{raw_object}")


def _transform_weather(city: str, wind_threshold: str, **context) -> None:
    hook = _gcs_hook()
    ds = _read_latest(city)
    raw_object = f"raw/{city}/{ds}.json"
    processed_object = f"processed/{city}/{ds}.json"

    if hook.exists(bucket_name=GCS_BUCKET, object_name=processed_object):
        logging.info(f"[{city}][{ds}] Processed file already in GCS - skipping transform.")
        return

    raw_bytes = hook.download(bucket_name=GCS_BUCKET, object_name=raw_object)
    raw = json.loads(raw_bytes)
    current = raw["data"][0]
    threshold = float(wind_threshold)

    record = {
        "timestamp": current["dt"],
        "city": city,
        "temp": current["temp"],
        "humidity": current["humidity"],
        "cloudiness": current["clouds"],
        "wind_speed": current["wind_speed"],
        "is_alert": current["wind_speed"] > threshold,
    }

    hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=processed_object,
        data=json.dumps(record),
        mime_type="application/json",
    )
    logging.info(f"[{city}][{ds}] Processed data saved to gs://{GCS_BUCKET}/{processed_object}")


def _quality_check(city: str, **context) -> None:
    hook = _gcs_hook()
    ds = _read_latest(city)
    processed_object = f"processed/{city}/{ds}.json"

    raw_bytes = hook.download(bucket_name=GCS_BUCKET, object_name=processed_object)
    record = json.loads(raw_bytes)
    errors: list[str] = []

    # all fields are present
    required = {"timestamp", "city", "temp", "humidity", "cloudiness", "wind_speed"}
    if missing := required - record.keys():
        errors.append(f"Missing fields: {missing}")

    # temperature in realistic range
    if not (-90 <= record.get("temp", 999) <= 60):
        errors.append(f"Temperature out of range: {record['temp']} C")

    # humidity between 0 and 100%
    if not (0 <= record.get("humidity", -1) <= 100):
        errors.append(f"Humidity out of range: {record['humidity']} %")

    # wind speed is not negative
    if record.get("wind_speed", -1) < 0:
        errors.append(f"Negative wind speed: {record['wind_speed']} m/s")

    # city name matches expected value
    if record.get("city") != city:
        errors.append(f"City mismatch: expected '{city}', got '{record.get('city')}'")

    if errors:
        raise ValueError(
            f"[{city}][{ds}] Quality checks FAILED:\n"
            + "\n".join(f"{e}" for e in errors)
        )

    logging.info(f"[{city}][{ds}] All 5 quality checks passed")


def _load_to_db(city: str, **context) -> None:
    from airflow.providers.sqlite.hooks.sqlite import SqliteHook

    hook = _gcs_hook()
    ds = _read_latest(city)
    processed_object = f"processed/{city}/{ds}.json"

    raw_bytes = hook.download(bucket_name=GCS_BUCKET, object_name=processed_object)
    record = json.loads(raw_bytes)

    if record["is_alert"]:
        logging.warning(f"[ALERT][{city}][{ds}] High wind! wind_speed={record['wind_speed']} m/s")

    sqlite = SqliteHook(sqlite_conn_id="weather_conn")
    sqlite.run(
        sql="""
            INSERT INTO measures (timestamp, city, temp, humidity, cloudiness, wind_speed)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
        parameters=(
            record["timestamp"],
            record["city"],
            record["temp"],
            record["humidity"],
            record["cloudiness"],
            record["wind_speed"],
        ),
    )
    logging.info(f"[{city}][{ds}] Record inserted into DB")


def create_ingestion_dag(city: str, lat: float, lon: float) -> DAG:
    with DAG(
        dag_id=f"weather_ingestion_{city}",
        is_paused_upon_creation=False,
        schedule="@daily",
        start_date=datetime(2026, 5, 20),
        catchup=True,
        default_args=DEFAULT_ARGS,
        params=DAG_PARAMS,
        tags=["hw3"],
    ) as dag:

        PythonOperator(
            task_id="extract_weather",
            python_callable=_extract_weather,
            op_kwargs={"city": city, "lat": lat, "lon": lon, "ds": "{{ ds }}"},
            outlets=[Asset(f"gs://{GCS_BUCKET}/raw/{city}")],
        )

    return dag


def create_processing_dag(city: str) -> DAG:
    with DAG(
        dag_id=f"weather_processing_{city}",
        is_paused_upon_creation=False,
        schedule=[Asset(f"gs://{GCS_BUCKET}/raw/{city}")],
        start_date=datetime(2026, 5, 20),
        catchup=False,
        default_args=DEFAULT_ARGS,
        params=DAG_PARAMS,
        tags=["hw3"],
    ) as dag:

        create_table = SQLExecuteQueryOperator(
            task_id="create_table",
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

        transform = PythonOperator(
            task_id="transform_weather",
            python_callable=_transform_weather,
            op_kwargs={"city": city, "wind_threshold": "{{ params.wind_threshold }}"},
        )

        quality = PythonOperator(
            task_id="quality_check",
            python_callable=_quality_check,
            op_kwargs={"city": city},
        )

        load = PythonOperator(
            task_id="load_to_db",
            python_callable=_load_to_db,
            op_kwargs={"city": city},
        )

        create_table >> transform >> quality >> load

    return dag


for _city, (_lat, _lon) in CITIES.items():
    globals()[f"weather_ingestion_{_city}"] = create_ingestion_dag(_city, _lat, _lon)
    globals()[f"weather_processing_{_city}"] = create_processing_dag(_city)