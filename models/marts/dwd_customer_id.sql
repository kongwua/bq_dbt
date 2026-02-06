{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    alias='dwd_customer_id',
    merge_exclude_columns=['created_at'],
    pre_hook="{{ dedupe_customer_id(this) }}"
  )
}}

with source_ids as (
  select distinct customer_id
  from {{ ref('dwd_customer_email_phone_map') }}
),
existing as (
  {% if is_incremental() %}
  select customer_id, merge_record, created_at
  from {{ this }}
  {% else %}
  select
    cast(null as string) as customer_id,
    cast(null as json) as merge_record,
    cast(null as timestamp) as created_at
  where false
  {% endif %}
)

select
  s.customer_id,
  coalesce(e.merge_record, json '{}') as merge_record,
  coalesce(e.created_at, current_timestamp()) as created_at,
  current_timestamp() as updated_at
from source_ids as s
left join existing as e
  on e.customer_id = s.customer_id
