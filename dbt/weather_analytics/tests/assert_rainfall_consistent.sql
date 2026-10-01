-- Fails on negative precipitation or rolling totals
select weather_key, precipitation_mm, precip_rolling_7d_mm, precip_rolling_30d_mm
from {{ ref('weather_daily_metrics') }}
where precipitation_mm < 0
   or precip_rolling_7d_mm < 0
   or precip_rolling_30d_mm < precip_rolling_7d_mm
