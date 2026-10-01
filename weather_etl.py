"""
Lab 2 - Weather Prediction Analytics | DAG 1 of 2: ETL
"""
from datetime import date, datetime, timedelta, timezone
import logging

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

log = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_TABLE = "DEMO_DB.RAW.WEATHER_DAILY"
DEFAULT_API_URL = "https://api.open-meteo.com/v1/forecast"

# Open-Meteo daily variable -> Snowflake column (units: C, mm, h, km/h, WMO code)
DAILY_VARIABLES = {
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "temperature_2m_mean": "temp_mean_c",
    "precipitation_sum": "precipitation_mm",
    "rain_sum": "rain_mm",
    "precipitation_hours": "precipitation_hours",
    "wind_speed_10m_max": "wind_speed_max_kmh",
    "weather_code": "weather_code",
}

COLUMNS = (
    ["city", "latitude", "longitude", "timezone", "date"]
    + list(DAILY_VARIABLES.values())
    + ["is_forecast", "load_run_id"]
)

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    city                 VARCHAR(50)   NOT NULL,
    latitude             NUMBER(9,6)   NOT NULL,
    longitude            NUMBER(9,6)   NOT NULL,
    timezone             VARCHAR(50),
    date                 DATE          NOT NULL,
    temp_max_c           FLOAT,
    temp_min_c           FLOAT,
    temp_mean_c          FLOAT,
    precipitation_mm     FLOAT,
    rain_mm              FLOAT,
    precipitation_hours  FLOAT,
    wind_speed_max_kmh   FLOAT,
    weather_code         INTEGER,
    is_forecast          BOOLEAN       NOT NULL,
    load_run_id          VARCHAR(250),
    loaded_at            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_weather_daily PRIMARY KEY (city, date)
)
"""

INSERT_SQL = (
    f"INSERT INTO {TARGET_TABLE} ({', '.join(COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(COLUMNS))})"
)


@task
def get_cities() -> list:
    """Read and validate the Airflow Variable `weather_cities` at run time."""
    cities = Variable.get("weather_cities", deserialize_json=True)

    if not isinstance(cities, list) or len(cities) < 2:
        raise ValueError("Variable `weather_cities` must be a JSON list with at least 2 cities")
    names = [c.get("city") for c in cities]
    if len(set(names)) != len(names):
        raise ValueError(f"Duplicate city names in `weather_cities`: {names}")
    for c in cities:
        if not c.get("city"):
            raise ValueError(f"City entry without a name: {c}")
        if not (-90 <= float(c["latitude"]) <= 90 and -180 <= float(c["longitude"]) <= 180):
            raise ValueError(f"Invalid coordinates for {c['city']}: {c}")

    log.info("Cities to load: %s", names)
    return cities


@task(retries=3, retry_delay=timedelta(minutes=1))
def extract(city: dict) -> dict:
    """Call the Open-Meteo Forecast API for ONE city (past days + forecast days)."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "daily": ",".join(DAILY_VARIABLES),
        "past_days": int(Variable.get("weather_past_days", default_var=90)),         # API max 92
        "forecast_days": int(Variable.get("weather_forecast_days", default_var=7)),  # API max 16
        "timezone": city.get("timezone", "auto"),  # "auto" = local time zone of the coordinates
    }
    api_url = Variable.get("open_meteo_api_url", default_var=DEFAULT_API_URL)

    response = requests.get(api_url, params=params, timeout=30)
    if response.status_code != 200:
        # Open-Meteo returns {"error": true, "reason": "..."} on HTTP 400
        raise ValueError(
            f"Open-Meteo error for {city['city']}: {response.status_code} {response.text[:300]}"
        )

    payload = response.json()
    log.info("%s: received %d days (tz=%s)",
             city["city"], len(payload["daily"]["time"]), payload.get("timezone"))
    return {
        "city": city["city"],
        "latitude": float(city["latitude"]),
        "longitude": float(city["longitude"]),
        "timezone": payload.get("timezone"),
        "utc_offset_seconds": payload.get("utc_offset_seconds", 0),
        "daily": payload["daily"],
    }


@task
def transform(api_response: dict) -> list:
    """Turn the column-oriented API arrays into one record per (city, date)."""
    daily = api_response["daily"]
    days = daily["time"]

    for api_name in DAILY_VARIABLES:
        values = daily.get(api_name)
        if values is None or len(values) != len(days):
            raise ValueError(f"{api_response['city']}: '{api_name}' missing or wrong length")

    # Days from 'today' (in the city's own time zone) onward are forecasts, not observations
    utc_offset = timedelta(seconds=api_response["utc_offset_seconds"])
    local_today = (datetime.now(timezone.utc) + utc_offset).date()

    records = []
    for i, day in enumerate(days):
        record = {
            "city": api_response["city"],
            "latitude": api_response["latitude"],
            "longitude": api_response["longitude"],
            "timezone": api_response["timezone"],
            "date": day,
        }
        for api_name, column in DAILY_VARIABLES.items():
            record[column] = daily[api_name][i]  # None stays None -> SQL NULL
        record["is_forecast"] = date.fromisoformat(day) >= local_today
        records.append(record)

    log.info("%s: %d records (%d forecast)", api_response["city"], len(records),
             sum(r["is_forecast"] for r in records))
    return records


@task
def load(records_per_city) -> int:
    """Idempotent load of ALL cities in ONE Snowflake transaction."""
    records = [r for city_records in records_per_city for r in city_records]
    if not records:
        raise ValueError("Nothing to load")

    run_id = get_current_context()["run_id"]
    rows = [tuple(r.get(c) for c in COLUMNS[:-1]) + (run_id,) for r in records]

    # The exact date window re-fetched for each city - this is what we replace
    windows = {}
    for r in records:
        start, end = windows.get(r["city"], (r["date"], r["date"]))
        windows[r["city"]] = (min(start, r["date"]), max(end, r["date"]))

    conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
    cur = conn.cursor()
    try:
        # DDL runs BEFORE the transaction on purpose: in Snowflake a DDL statement
        # implicitly COMMITs any open transaction, which would break atomicity.
        cur.execute(CREATE_TABLE_SQL)

        cur.execute("BEGIN")
        for city, (start, end) in windows.items():
            cur.execute(
                f"DELETE FROM {TARGET_TABLE} WHERE city = %s AND date BETWEEN %s AND %s",
                (city, start, end),
            )
            log.info("%s: deleted %s existing rows for %s..%s", city, cur.rowcount, start, end)

        cur.executemany(INSERT_SQL, rows)
        log.info("Inserted %d rows", len(rows))

        # Data-quality gates INSIDE the transaction - failing any of them rolls everything back
        cur.execute(
            f"SELECT city, date, COUNT(*) FROM {TARGET_TABLE} "
            f"GROUP BY city, date HAVING COUNT(*) > 1 LIMIT 5"
        )
        duplicates = cur.fetchall()
        if duplicates:
            raise ValueError(f"Duplicate (city, date) rows after load: {duplicates}")

        for city, (start, end) in windows.items():
            cur.execute(
                f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE city = %s AND date BETWEEN %s AND %s",
                (city, start, end),
            )
            loaded = cur.fetchone()[0]
            expected = sum(1 for r in records if r["city"] == city)
            if loaded != expected:
                raise ValueError(f"{city}: expected {expected} rows in window, found {loaded}")

        cur.execute("COMMIT")
        log.info("COMMIT ok - %d rows for %s", len(rows), list(windows))
        return len(rows)
    except Exception as e:
        cur.execute("ROLLBACK")
        log.error("Load failed, transaction rolled back: %s", e)
        raise e
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="weather_etl",
    description="Open-Meteo (2 cities) -> Snowflake RAW, idempotent transactional load",
    start_date=datetime(2026, 9, 20),
    schedule="0 2 * * *",  # daily 02:00 UTC
    catchup=False,
    max_active_runs=1,     # never two loads racing on the same table
    default_args={"owner": "kaushik", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["lab2", "weather", "ETL"],
) as dag:
    cities = get_cities()
    api_responses = extract.expand(city=cities)          # one mapped task per city
    city_records = transform.expand(api_response=api_responses)
    loaded = load(city_records)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_elt",
        trigger_dag_id="weather_dbt_elt",
        wait_for_completion=False,
    )

    loaded >> trigger_dbt
