{% snapshot snapshot_weather_daily %}

{#
  SCD Type 2 history of every (city, date) row.
  Forecast values change from run to run (and become observations later),
  so this snapshot keeps every version -> lets us measure forecast accuracy.
#}
{{
  config(
    target_schema='snapshot',
    unique_key='weather_key',
    strategy='check',
    check_cols=['temp_max_c', 'temp_min_c', 'temp_mean_c', 'precipitation_mm', 'is_forecast'],
  )
}}

select
    weather_key,
    city,
    date,
    temp_max_c,
    temp_min_c,
    temp_mean_c,
    precipitation_mm,
    is_forecast,
    loaded_at
from {{ ref('stg_weather_daily') }}

{% endsnapshot %}
