-- Fails if any day has a minimum temperature above its maximum
select weather_key, temp_min_c, temp_max_c
from {{ ref('stg_weather_daily') }}
where temp_min_c > temp_max_c
