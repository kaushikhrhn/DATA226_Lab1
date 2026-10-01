-- Staging: clean, typed, exactly one row per (city, date)
with source as (

    select * from {{ source('raw', 'weather_daily') }}

),

deduplicated as (

    -- Defensive: Snowflake does not enforce PRIMARY KEY constraints
    select *
    from source
    qualify row_number() over (partition by city, date order by loaded_at desc) = 1

)

select
    city || '|' || to_char(date, 'YYYY-MM-DD')                   as weather_key,
    city,
    latitude,
    longitude,
    timezone,
    date,
    temp_max_c,
    temp_min_c,
    coalesce(temp_mean_c, (temp_max_c + temp_min_c) / 2)        as temp_mean_c,
    temp_max_c - temp_min_c                                     as diurnal_range_c,
    precipitation_mm,
    rain_mm,
    precipitation_hours,
    wind_speed_max_kmh,
    weather_code,
    precipitation_mm >= {{ var('wet_day_threshold_mm') }}      as is_wet_day,
    is_forecast,
    loaded_at
from deduplicated
