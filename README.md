# DATA226_Lab1

## Apache Superset BI dashboard

The local Superset service adds a BI layer without changing the Airflow or dbt
services. It uses a separate local metadata database and connects to Snowflake
with the same key-pair authentication approach as the pipeline.

### Start Superset

The repository `.env` is local-only and ignored by Git. It contains the
Superset secret key and local admin password. The provided `.env.example` shows
the required variable names for a fresh checkout; use unique local values.

From the repository root, run:

```powershell
docker compose up -d --build
```

Open [http://localhost:8088](http://localhost:8088). The local admin username
is `admin`; its generated password is the `SUPERSET_ADMIN_PASSWORD` value in
`.env`. The initialization service upgrades Superset's metadata database,
creates this account once, and initializes Superset permissions.

### Connect Snowflake and build the dashboard

In Superset, add the Snowflake database using the existing `snowflake_conn`
values from Airflow and the read-only key at
`/opt/superset/keys/snowflake_key.p8`. Register the analytics datasets and
create the charts and dashboard described in [the BI dashboard guide](docs/bi_dashboard.md).

The dbt datasets are `DEMO_DB.ANALYTICS.WEATHER_DAILY_METRICS` and
`DEMO_DB.ANALYTICS.WEATHER_CITY_SUMMARY`. The main dashboard uses the daily
metrics table.
