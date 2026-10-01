{#
  Window aggregate over the last `days` calendar days (current day included), per city.
  Example: {{ rolling('avg', 'temp_mean_c', 7) }}  -> 7-day moving average
#}
{% macro rolling(agg, expr, days) -%}
    {{ agg }}({{ expr }}) over (
        partition by city order by date rows between {{ days - 1 }} preceding and current row)
{%- endmacro %}
