{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['customer_id', 'shopify_id'],
    alias='dwd_customer_shopify_map',
    merge_exclude_columns=['created_at'],
    pre_hook="{{ dedupe_customer_shopify_map(this) }}"
  )
}}

with source_links as (
  select
    customer_id,
    shopify_id,
    max(updated_at) as updated_at
  from {{ ref('int_customer_shopify_links') }} as s
  where 1=1
  {{ incremental_updated_filter('s') }}
  group by 1, 2
),
existing as (
  {% if is_incremental() %}
  select customer_id, shopify_id, created_at
  from {{ this }}
  {% else %}
  select
    cast(null as string) as customer_id,
    cast(null as int64) as shopify_id,
    cast(null as timestamp) as created_at
  where false
  {% endif %}
)

select
  coalesce(e.created_at, current_timestamp()) as created_at,
  current_timestamp() as updated_at,
  s.customer_id,
  s.shopify_id
from source_links as s
left join existing as e
  on e.customer_id = s.customer_id
 and e.shopify_id = s.shopify_id
