{% macro dedupe_customer_shopify_map(relation) -%}
{% if execute %}
  {% set rel = adapter.get_relation(
      database=relation.database,
      schema=relation.schema,
      identifier=relation.identifier
  ) %}
  {% if rel is not none %}
    {% set sql %}
      create or replace table {{ relation }} as
      select
        created_at,
        updated_at,
        customer_id,
        shopify_id
      from (
        select
          created_at,
          updated_at,
          customer_id,
          shopify_id,
          row_number() over (
            partition by customer_id, shopify_id
            order by updated_at desc, created_at desc
          ) as rn
        from {{ relation }}
      )
      where rn = 1
    {% endset %}
    {{ return(sql) }}
  {% endif %}
{% endif %}
{{ return('select 1') }}
{%- endmacro %}
