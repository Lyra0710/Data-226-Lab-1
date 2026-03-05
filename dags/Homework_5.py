from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'email': ['ananya.yallapragada@sjsu.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Chicago"
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"API request failed: {response.status_code}")
    return response.json()

@task
def transform(raw_data, latitude, longitude, city):
    if "daily" not in raw_data:
        raise ValueError("Missing 'daily' key in API response")

    data = raw_data["daily"]
    records = []

    for i in range(len(data["time"])):
        record = {
            "latitude": latitude,
            "longitude": longitude,
            "date": data["time"][i],
            "temp_max": data["temperature_2m_max"][i],
            "temp_min": data["temperature_2m_min"][i],
            "precipitation": data["precipitation_sum"][i],
            "weather_code": data["weather_code"][i],
            "city": city
        }
        records.append(record)  # BUG FIX: actually append

    return records

@task
def load(target_table, records):  # BUG FIX: don't accept/pass cursor
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    con = hook.get_conn().cursor()

    try:
        con.execute("BEGIN;")

        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude        FLOAT,
                longitude       FLOAT,
                date            DATE,
                temp_max        FLOAT,
                temp_min        FLOAT,
                precipitation   FLOAT,
                weather_code    VARCHAR(3),
                PRIMARY KEY (latitude, longitude, date)
            );
        """)

        con.execute(f"DELETE FROM {target_table};")

        for r in records:  # BUG FIX: records is a list, not a DataFrame
            latitude = r['latitude']
            longitude = r['longitude']
            date = r['date']
            temp_max = r['temp_max']
            temp_min = r['temp_min']
            precipitation = r['precipitation']
            weather_code = r['weather_code']

            sql = f"""
                INSERT INTO {target_table}
                (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES
                ('{latitude}', '{longitude}', '{date}', '{temp_max}', '{temp_min}', '{precipitation}', '{weather_code}');
            """
            con.execute(sql)

        con.execute("COMMIT;")
        print(f"Loaded {len(records)} records into {target_table}")

    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise

with DAG(
    dag_id='WeatherData_ETL',
    start_date=datetime(2026, 2, 25),
    catchup=False,
    tags=['ETL'],
    default_args=default_args,
    schedule='30 2 * * *'
) as dag:
    LATITUDE = Variable.get("Latitude")
    LONGITUDE = Variable.get("Longitude")
    CITY = Variable.get("City")
    target_table = "raw.WEATHER_DATA_hw_5"

    raw_data = extract(LATITUDE, LONGITUDE)
    data = transform(raw_data, LATITUDE, LONGITUDE, CITY)
    load(target_table, data)  