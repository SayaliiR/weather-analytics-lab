"""
Weather ETL DAG
Extracts weather info (recent history + forecast) for two cities from the
Open-Meteo forecast API and loads it into Snowflake.

"""
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

 
SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_TABLE = "BIGSTUDY_DB.RAW.WEATHER_DAILY"
API_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "weather_code",
]
 
 
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn().cursor()
 
 

with DAG(
    dag_id="weather_etl",
    description="Data pipeline to extract weather data into Snowflake",
    start_date=datetime(2026, 9, 27),
    schedule="0 7 * * *",         
    catchup=True,
    tags=["ETL", "weather"],
) as dag:
 
    @task
    def get_cities():
        """Read the city list from an Airflow Variable."""
        return json.loads(Variable.get("weather_cities"))
 
    @task
    def extract(city: dict):
        """Call Open-Meteo for one city; return the raw JSON plus the city name."""
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "daily": ",".join(DAILY_FIELDS),
            "past_days": int(Variable.get("weather_past_days", default_var=60)),
            "forecast_days": int(Variable.get("weather_forecast_days", default_var=7)),
            "timezone": "auto",
        }
        response = requests.get(API_URL, params=params)
        return {"city": city["name"], "data": response.json()}
 
    @task
    def transform(payload: dict):
        """Turn one city's API response into rows matching RAW.WEATHER_DAILY."""
        data, city = payload["data"], payload["city"]
        today = datetime.now(ZoneInfo(data["timezone"])).date().isoformat()

        df = pd.DataFrame({
            "city": city,
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "date": data["daily"]["time"],
            "temp_max": data["daily"]["temperature_2m_max"],
            "temp_min": data["daily"]["temperature_2m_min"],
            "temp_mean": data["daily"]["temperature_2m_mean"],
            "precipitation_sum": data["daily"]["precipitation_sum"],
            "wind_speed_max": data["daily"]["wind_speed_10m_max"],
            "weather_code": data["daily"]["weather_code"],
        })
        df["is_forecast"] = df["date"] > today

        # INSERT needs plain Python values: NaN -> None, numpy types -> Python types
        df = df.astype(object).where(pd.notnull(df), None)
        return [list(row) for row in df.values.tolist()]
 
    @task
    def load(rows_per_city: list):
        """Full refresh of the raw table inside one transaction (idempotent)."""
        records = [row for city_rows in rows_per_city for row in city_rows]
        cur = get_snowflake_cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    city VARCHAR(50) NOT NULL,
                    latitude FLOAT NOT NULL,
                    longitude FLOAT NOT NULL,
                    date DATE NOT NULL,
                    temp_max FLOAT,
                    temp_min FLOAT,
                    temp_mean FLOAT,
                    precipitation_sum FLOAT,
                    wind_speed_max FLOAT,
                    weather_code INT,
                    is_forecast BOOLEAN NOT NULL,
                    PRIMARY KEY (city, date)
                )
            """)
            cur.execute(f"DELETE FROM {TARGET_TABLE}")
            cur.executemany(
                f"""INSERT INTO {TARGET_TABLE}
                    (city, latitude, longitude, date, temp_max, temp_min, temp_mean,
                     precipitation_sum, wind_speed_max, weather_code, is_forecast)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                records,
            )
            cur.execute("COMMIT")
            print(f"Loaded {len(records)} rows into {TARGET_TABLE}")
        except Exception as e:
            cur.execute("ROLLBACK")
            print(f"Load failed, transaction rolled back: {e}")
            raise
        finally:
            cur.close()
 
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="weather_dbt",
        wait_for_completion=False,
    )
 
    cities = get_cities()
    raw = extract.expand(city=cities)
    rows = transform.expand(payload=raw)
    load(rows) >> trigger_dbt
 