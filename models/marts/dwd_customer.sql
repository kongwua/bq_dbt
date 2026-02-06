{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    alias='dwd_customer',
    merge_exclude_columns=['created_at']
  )
}}

with changed_customer_ids as (
  select distinct customer_id
  from {{ ref('dwd_customer_email_phone_map') }} as map
  where 1=1
  {{ incremental_updated_filter('map') }}

  union distinct

  select distinct customer_id
  from {{ ref('dwd_customer_shopify_map') }} as sm
  where 1=1
  {{ incremental_updated_filter('sm') }}

  union distinct

  select distinct sm.customer_id
  from {{ ref('stg_ods_shopify_customers') }} as ods
  inner join {{ ref('dwd_customer_shopify_map') }} as sm
    on ods.id = sm.shopify_id
  where 1=1
  {{ incremental_updated_filter('ods') }}
),
source as (
  select
    map.customer_id,
    map.email,
    map.phone,
    array_agg(ods.currency ignore nulls order by ods.updated_at desc limit 1)[safe_offset(0)] as currency,
    array_agg(ods.addresses ignore nulls order by ods.updated_at desc limit 1)[safe_offset(0)] as addresses,
    array_agg(ods.first_name ignore nulls order by ods.updated_at desc limit 1)[safe_offset(0)] as firstname,
    array_agg(ods.last_name ignore nulls order by ods.updated_at desc limit 1)[safe_offset(0)] as lastname,
    min(ods.created_at) as shopify_created_at,
    to_json(array_agg(distinct ods.tags ignore nulls)) as shopify_tags,
    map.created_at as map_created_at
  from {{ ref('dwd_customer_email_phone_map') }} as map
  left join {{ ref('dwd_customer_shopify_map') }} as sm
    on map.customer_id = sm.customer_id
  left join {{ ref('stg_ods_shopify_customers') }} as ods
    on sm.shopify_id = ods.id
  where map.customer_id in (select customer_id from changed_customer_ids)
  group by map.customer_id, map.email, map.phone, map.created_at
),
existing as (
  {% if is_incremental() %}
  select customer_id, created_at
  from {{ this }}
  {% else %}
  select
    cast(null as string) as customer_id,
    cast(null as timestamp) as created_at
  where false
  {% endif %}
)

select
  coalesce(e.created_at, s.map_created_at, current_timestamp()) as created_at,
  current_timestamp() as updated_at,
  s.customer_id,
  s.email,
  s.phone,
  s.currency,
  s.addresses,
  s.firstname,
  s.lastname,
  s.shopify_created_at,
  s.shopify_tags
from source as s
left join existing as e
  on e.customer_id = s.customer_id
