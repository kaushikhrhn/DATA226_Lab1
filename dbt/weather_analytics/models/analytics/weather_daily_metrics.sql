-- One row per city per calendar day with the metrics shown in Tableau:
--   * 7-day / 30-day moving averages of daily mean temperature
--   * temperature anomaly (and z-score) vs. the trailing 30-day baseline
--   * rolling 7-day / 30-day rainfall and wet-day count
--   * dry-spell length (consecutive days with precipitation < wet-day threshold)
-- Window metrics stay NULL until their window is complete (no misleading partial averages).

{% set min_days = var('min_baseline_days') %}

with daily as (

    select * from {{ ref('stg_weather_daily') }}

),

city_bounds as (

    select
        city,
        min(date)      as first_date,
        max(date)      as last_date,
        max(latitude)  as latitude,
        max(longitude) as longitude,
        max(timezone)  as timezone
    from daily
    group by city

),

-- Calendar spine (every city x every day) so "ROWS 6 PRECEDING" always means 7 calendar days,
-- even if the API ever skips a day.
day_numbers as (

    select row_number() over (order by seq4()) - 1 as day_offset
    from table(generator(rowcount => 3660))   -- up to ~10 years of history

),

calendar as (

    select
        b.city,
        dateadd(day, n.day_offset, b.first_date) as date
    from city_bounds b
    join day_numbers n
      on n.day_offset <= datediff(day, b.first_date, b.last_date)

),

filled as (

    select
        c.city || '|' || to_char(c.date, 'YYYY-MM-DD') as weather_key,
        c.city,
        c.date,
        b.latitude,
        b.longitude,
        b.timezone,
        d.temp_max_c,
        d.temp_min_c,
        d.temp_mean_c,
        d.diurnal_range_c,
        d.precipitation_mm,
        d.rain_mm,
        d.precipitation_hours,
        d.wind_speed_max_kmh,
        d.weather_code,
        d.is_wet_day,
        coalesce(d.is_forecast, false) as is_forecast,
        d.weather_key is null          as is_missing_day
    from calendar c
    join city_bounds b
      on b.city = c.city
    left join daily d
      on d.city = c.city
     and d.date = c.date

),

windowed as (

    select
        filled.*,
        {{ rolling('count', '*', 7) }}                     as days_in_7d,
        {{ rolling('count', '*', 30) }}                    as days_in_30d,
        {{ rolling('avg', 'temp_mean_c', 7) }}             as ma_7d,
        {{ rolling('avg', 'temp_mean_c', 30) }}            as ma_30d,
        {{ rolling('stddev_samp', 'temp_mean_c', 30) }}    as std_30d,
        {{ rolling('count', 'temp_mean_c', 30) }}          as obs_30d,
        {{ rolling('sum', 'precipitation_mm', 7) }}        as rain_7d,
        {{ rolling('sum', 'precipitation_mm', 30) }}       as rain_30d,
        {{ rolling('sum', 'iff(is_wet_day, 1, 0)', 30) }}  as wet_30d,

        -- dry spells (gaps-and-islands): every wet day starts a new group
        sum(iff(is_wet_day, 1, 0)) over (
            partition by city order by date
            rows between unbounded preceding and current row) as wet_day_group
    from filled

),

enriched as (

    select
        windowed.*,

        -- anomaly baseline = the 30 days BEFORE the current day
        -- (yesterday's 30-day window, so today's value never biases its own baseline)
        lag(days_in_30d) over (partition by city order by date) as days_in_baseline,
        lag(ma_30d)      over (partition by city order by date) as baseline_avg,
        lag(std_30d)     over (partition by city order by date) as baseline_std,
        lag(obs_30d)     over (partition by city order by date) as baseline_obs,

        case
            when is_wet_day is null then null
            when is_wet_day then 0
            else sum(iff(is_wet_day = false, 1, 0)) over (
                     partition by city, wet_day_group order by date
                     rows between unbounded preceding and current row)
        end as dry_spell_days
    from windowed

),

flagged as (

    select
        enriched.*,
        days_in_7d = 7                                        as has_7d,
        days_in_30d = 30 and obs_30d >= {{ min_days }}        as has_30d_temp,
        days_in_30d = 30                                      as has_30d,
        days_in_baseline = 30 and baseline_obs >= {{ min_days }} as has_baseline
    from enriched

)

select
    s.weather_key,
    s.city,
    s.date,
    s.latitude,
    s.longitude,
    s.timezone,
    s.is_forecast,
    s.is_missing_day,

    -- daily measures
    s.temp_max_c,
    s.temp_min_c,
    round(s.temp_mean_c, 2)                                   as temp_mean_c,
    round(s.diurnal_range_c, 2)                               as diurnal_range_c,
    s.precipitation_mm,
    s.rain_mm,
    s.precipitation_hours,
    s.wind_speed_max_kmh,
    s.weather_code,
    w.description                                             as weather_description,
    w.category                                                as weather_category,
    s.is_wet_day,

    -- moving averages
    round(iff(s.has_7d, s.ma_7d, null), 2)                    as temp_mean_ma_7d_c,
    round(iff(s.has_30d_temp, s.ma_30d, null), 2)             as temp_mean_ma_30d_c,

    -- temperature anomaly vs. trailing 30-day baseline
    round(iff(s.has_baseline, s.baseline_avg, null), 2)       as temp_baseline_30d_c,
    round(iff(s.has_baseline,
              s.temp_mean_c - s.baseline_avg, null), 2)       as temp_anomaly_c,
    round(iff(s.has_baseline and s.baseline_std > 0,
              (s.temp_mean_c - s.baseline_avg) / s.baseline_std,
              null), 2)                                       as temp_anomaly_zscore,

    -- rolling rainfall
    round(iff(s.has_7d, s.rain_7d, null), 2)                  as precip_rolling_7d_mm,
    round(iff(s.has_30d, s.rain_30d, null), 2)                as precip_rolling_30d_mm,
    iff(s.has_30d, s.wet_30d, null)                           as wet_days_30d,

    -- dry spell
    s.dry_spell_days                                          as dry_spell_length_days
from flagged s
left join {{ ref('wmo_weather_codes') }} w
  on w.weather_code = s.weather_code
