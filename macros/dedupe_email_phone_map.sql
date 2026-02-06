{% macro dedupe_email_phone_map(relation) -%}
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
        customer_id,
        email,
        phone,
        created_at,
        updated_at
      from (
        select
          customer_id,
          coalesce(email, '') as email,
          coalesce(phone, '') as phone,
          created_at,
          updated_at,
          row_number() over (
            partition by coalesce(email, ''), coalesce(phone, '')
            order by updated_at desc, created_at desc, customer_id desc
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
