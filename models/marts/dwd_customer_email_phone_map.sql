{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['email', 'phone'],
    alias='dwd_customer_email_phone_map',
    merge_exclude_columns=['created_at'],
    pre_hook="{{ dedupe_email_phone_map(this) }}"
  )
}}

with source_pairs as (
  select
    coalesce(email, '') as email,
    coalesce(phone, '') as phone,
    max(updated_at) as updated_at
  from {{ ref('int_customer_email_phone_pairs') }} as p
  where 1=1
  {{ incremental_updated_filter('p') }}
  group by 1, 2
),
existing as (
  {% if is_incremental() %}
  select
    customer_id,
    coalesce(email, '') as email,
    coalesce(phone, '') as phone,
    created_at
  from {{ this }}
  {% else %}
  select
    cast(null as string) as customer_id,
    cast(null as string) as email,
    cast(null as string) as phone,
    cast(null as timestamp) as created_at
  where false
  {% endif %}
)

select
  coalesce(e.customer_id, generate_uuid()) as customer_id,
  sp.email,
  sp.phone,
  coalesce(e.created_at, current_timestamp()) as created_at,
  current_timestamp() as updated_at
from source_pairs as sp
left join existing as e
  on e.email = sp.email
 and e.phone = sp.phone
