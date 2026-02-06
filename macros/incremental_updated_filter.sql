{% macro incremental_updated_filter(alias, updated_col='updated_at', target_updated_col='updated_at') -%}
{% if is_incremental() -%}
AND {{ alias }}.{{ updated_col }} > TIMESTAMP_SUB(
  (
    SELECT COALESCE(MAX({{ target_updated_col }}), TIMESTAMP('1970-01-01'))
    FROM {{ this }}
  ),
  INTERVAL {{ var('safety_window_hours', 24) }} HOUR
)
{% endif -%}
{%- endmacro %}
