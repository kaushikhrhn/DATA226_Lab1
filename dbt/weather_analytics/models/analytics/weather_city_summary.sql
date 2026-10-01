-- One row per city: KPI values for the Tableau dashboard header
with metrics as (

    select * from {{ ref('weather_daily_metrics') }}

),

observed as (

    select * from metrics
    where not is_forecast
      and not is_missing_day

),

latest as (

    select city, max(date) as last_observed_date
    from observed
    group by city

),

history as (

    select
        city,
        min(date)                  as first_observed_date,
        count(*)                   as observed_days_total,
        max(dry_spell_length_days) as longest_dry_spell_days
    from observed
    group by city

),

last_30_days as (

    select
        o.city,
        count(*)                          as observed_days_30d,
        round(avg(o.temp_mean_c), 2)      as avg_temp_30d_c,
        max(o.temp_max_c)                 as max_temp_30d_c,
        min(o.temp_min_c)                 as min_temp_30d_c,
        round(sum(o.precipitation_mm), 2) as total_precip_30d_mm,
        sum(iff(o.is_wet_day, 1, 0))      as wet_days_30d
    from observed o
    join latest l
      on l.city = o.city
    where o.date > dateadd(day, -30, l.last_observed_date)
    group by o.city

),

latest_day as (

    select
        o.city,
        o.temp_mean_ma_7d_c     as latest_temp_ma_7d_c,
        o.temp_anomaly_c        as latest_temp_anomaly_c,
        o.dry_spell_length_days as current_dry_spell_days
    from observed o
    join latest l
      on l.city = o.city
     and l.last_observed_date = o.date

),

forecast as (

    select
        city,
        count(*)                          as forecast_days,
        round(avg(temp_mean_c), 2)        as forecast_avg_temp_c,
        max(temp_max_c)                   as forecast_max_temp_c,
        round(sum(precipitation_mm), 2)   as forecast_total_precip_mm,
        max(dry_spell_length_days)        as forecast_max_dry_spell_days
    from metrics
    where is_forecast
    group by city

)

select
    l.city,
    h.first_observed_date,
    l.last_observed_date,
    h.observed_days_total,
    t.observed_days_30d,
    t.avg_temp_30d_c,
    t.max_temp_30d_c,
    t.min_temp_30d_c,
    t.total_precip_30d_mm,
    t.wet_days_30d,
    d.latest_temp_ma_7d_c,
    d.latest_temp_anomaly_c,
    d.current_dry_spell_days,
    h.longest_dry_spell_days,
    f.forecast_days,
    f.forecast_avg_temp_c,
    f.forecast_max_temp_c,
    f.forecast_total_precip_mm,
    f.forecast_max_dry_spell_days,
    current_timestamp() as updated_at
from latest l
join history h      on h.city = l.city
join last_30_days t on t.city = l.city
join latest_day d   on d.city = l.city
left join forecast f on f.city = l.city
