# Weather Analytics Dashboard

## Dashboard purpose

The Weather Analytics Dashboard compares daily and rolling weather patterns
between the two configured cities. dbt transforms the Open-Meteo data in
Snowflake into chart-ready analytics, and Superset presents those analytics
with city and date filters.

## Dashboard usage

Use the **City** filter to compare both cities or focus on one. Use the **Time
range** filter to narrow the dates shown across the charts. The charts show
daily mean temperature, its seven-day moving average, seven-day rolling
precipitation, temperature anomaly, and dry-spell length. The moving average
helps smooth daily variation; the anomaly shows temperature relative to the
trailing baseline calculated by dbt.

The daily table includes forecast dates as well as observations. The
`is_forecast` column identifies forecast rows. Some rolling measures are null
until their required date window is available.

## Dataset

dbt's configured target is database `DEMO_DB`, schema `ANALYTICS`. The
analytics models are configured as tables, so their expected Snowflake names
are:

| Superset dataset | Fully qualified Snowflake table | Purpose |
|---|---|---|
| `weather_daily_metrics` | `DEMO_DB.ANALYTICS.WEATHER_DAILY_METRICS` | Main time-series dataset, one row per city/date, including daily and rolling metrics. |
| `weather_city_summary` | `DEMO_DB.ANALYTICS.WEATHER_CITY_SUMMARY` | One row per city with recent-history and forecast summary measures. |

The daily metrics table is the main dashboard dataset because it contains the
date and city dimensions as well as all five requested chart measures.

### Exact fields in `WEATHER_DAILY_METRICS`

The final `SELECT` in the dbt model produces these fields:

`weather_key`, `city`, `date`, `latitude`, `longitude`, `timezone`,
`is_forecast`, `is_missing_day`, `temp_max_c`, `temp_min_c`, `temp_mean_c`,
`diurnal_range_c`, `precipitation_mm`, `rain_mm`, `precipitation_hours`,
`wind_speed_max_kmh`, `weather_code`, `weather_description`,
`weather_category`, `is_wet_day`, `temp_mean_ma_7d_c`,
`temp_mean_ma_30d_c`, `temp_baseline_30d_c`, `temp_anomaly_c`,
`temp_anomaly_zscore`, `precip_rolling_7d_mm`, `precip_rolling_30d_mm`,
`wet_days_30d`, `dry_spell_length_days`.

### Exact fields in `WEATHER_CITY_SUMMARY`

`city`, `first_observed_date`, `last_observed_date`, `observed_days_total`,
`observed_days_30d`, `avg_temp_30d_c`, `max_temp_30d_c`, `min_temp_30d_c`,
`total_precip_30d_mm`, `wet_days_30d`, `latest_temp_ma_7d_c`,
`latest_temp_anomaly_c`, `current_dry_spell_days`,
`longest_dry_spell_days`, `forecast_days`, `forecast_avg_temp_c`,
`forecast_max_temp_c`, `forecast_total_precip_mm`,
`forecast_max_dry_spell_days`, `updated_at`.

## Superset and Snowflake setup

The Compose setup exposes Superset at `http://localhost:8088`. It installs
`snowflake-sqlalchemy`, initializes the Superset metadata database, creates a
local administrator if one does not already exist, and runs `superset init`.
The metadata database is a separate local SQLite file in a named Docker volume;
the existing Airflow/Postgres metadata service is not changed.

The existing `.env` is local-only and ignored by Git. It holds a generated
Superset secret key and generated local admin password. Do not commit `.env` or
the `keys/` directory. The Snowflake private key is mounted read-only into the
Superset container at `/opt/superset/keys/snowflake_key.p8`.

Because the account and login details are stored in the Airflow connection,
copy the non-secret connection values from Airflow when configuring Superset;
do not put Snowflake passwords or private-key contents in this repository.
In Superset, go to **Settings → Data → Database Connections → + Database** and
use the Snowflake SQLAlchemy URI format below, substituting the account,
username, role, and warehouse values already used by `snowflake_conn`:

```text
snowflake://<USER>@<ACCOUNT>/DEMO_DB?role=<ROLE>&warehouse=<WAREHOUSE>
```

In **Advanced → Security → Secure Extra**, use the mounted key path. If that
key is encrypted, enter its passphrase in the secure field as well:

```json
{
  "auth_method": "keypair",
  "auth_params": {
    "privatekey_path": "/opt/superset/keys/snowflake_key.p8"
  }
}
```

For an encrypted key, add `"privatekey_pass": "<key passphrase>"` inside
`auth_params` through the Superset UI. Never put the passphrase in a committed
file. Test the connection; the Snowflake user/role must be able to use the
warehouse and read both analytics tables.

### Register the datasets

After the Snowflake connection succeeds:

1. Go to **Data → Datasets → + Dataset**.
2. Select the Snowflake database connection, `ANALYTICS` schema, and
   `WEATHER_DAILY_METRICS` table; save it.
3. Repeat for `WEATHER_CITY_SUMMARY`.
4. Open the daily metrics dataset and confirm that Superset recognizes `date`
   as a temporal column and `city` as a categorical column. Mark `date` as the
   dataset's main temporal column if Superset did not detect it automatically.

## Visualizations

Create the five charts from the `weather_daily_metrics` dataset. For each,
choose **Time-series Line Chart**, use `date` as the time column, and group by
`city`. The source model has one row per city/date; the aggregation below
therefore returns that daily value (and remains valid if the dataset is later
extended).

| Chart name | Chart type | Dimensions | Metric | Purpose |
|---|---|---|---|---|
| Daily Mean Temperature by City | Time-series Line Chart | `date`; series `city` | `AVG(temp_mean_c)` | Compare daily average temperatures. |
| 7-Day Moving Average Temperature | Time-series Line Chart | `date`; series `city` | `AVG(temp_mean_ma_7d_c)` | Compare smoothed temperature trends. |
| 7-Day Rolling Rainfall by City | Time-series Line Chart | `date`; series `city` | `MAX(precip_rolling_7d_mm)` | Compare each day's trailing seven-day precipitation total. |
| Temperature Anomaly by City | Time-series Line Chart | `date`; series `city` | `AVG(temp_anomaly_c)` | Show warmer or cooler days relative to the trailing baseline. |
| Dry Spell Length by City | Time-series Line Chart | `date`; series `city` | `MAX(dry_spell_length_days)` | Compare the consecutive dry-day count over time. |

For each chart, set a descriptive title as above and save it. Add all five to
the dashboard. Arrange temperature and moving average side by side, with
rainfall/anomaly and dry-spell charts below. Use a dashboard width that keeps
the city legend and date axis readable.

To create the dashboard itself, open **Dashboards → + Dashboard**, enter
`Weather Analytics Dashboard` as its title, save it, and choose **Edit
dashboard** to add the saved charts.

## Dashboard filters

Add native dashboard filters in **Edit dashboard → Filter bar → Add/Edit
filters**:

| Filter | Type / column | Scope |
|---|---|---|
| City | Select filter on `weather_daily_metrics.city` | All five charts. |
| Time range | Time range filter on `weather_daily_metrics.date` | All five charts. |

Save the filter configuration and dashboard. Leave the city filter at its
default of both configured cities and the time range at the full available
range. These defaults support the first assignment screenshot.

## Screenshot plan

Take screenshots in the browser after the charts and filters are saved and
loaded. Include the dashboard title and filter bar in both images.

### Screenshot 1 — both cities and full range

1. Open **Dashboards → Weather Analytics Dashboard**.
2. Set **City** to both city values (clear a single-city selection if needed).
3. Set **Time range** to **No filter** or the full available range.
4. Click **Apply** if the filter bar shows an Apply button; wait until the
   charts finish refreshing.
5. Arrange the page so at least the temperature, moving-average, rainfall,
   and anomaly charts are visible, then capture the screenshot. If all five
   fit clearly, include the dry-spell chart too.

### Screenshot 2 — filtered dashboard updates

1. Stay on the same dashboard.
2. Change **Time range** to a smaller period, such as the last 30 days, or
   choose one city in **City**.
3. Click **Apply** and wait until the charts finish refreshing.
4. Capture the same dashboard area with the changed filter value visible and
   the refreshed charts. This shows the dashboard responding to a filter.

## Manual setup still required

The container setup and local administrator initialization are automated. The
Snowflake account values live in the Airflow connection metadata database,
which this repository does not expose to the Superset container. Therefore,
adding the Superset Snowflake connection and registering datasets require the
UI steps above. Creating charts, arranging them, and adding dashboard-native
filters are also UI steps; this keeps the lab dashboard editable and avoids
depending on version-specific internal dashboard JSON.
