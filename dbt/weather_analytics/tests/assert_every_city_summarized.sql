-- Every city in the metrics table must be present in the summary table
select m.city
from (select distinct city from {{ ref('weather_daily_metrics') }}) m
left join {{ ref('weather_city_summary') }} s
  on s.city = m.city
where s.city is null
