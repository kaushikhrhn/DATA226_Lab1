-- Wet days must have dry_spell_length_days = 0, dry days must have >= 1
select weather_key, is_wet_day, dry_spell_length_days
from {{ ref('weather_daily_metrics') }}
where (is_wet_day and dry_spell_length_days <> 0)
   or (not is_wet_day and dry_spell_length_days < 1)
