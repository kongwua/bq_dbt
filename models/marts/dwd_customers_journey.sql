{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='shopify_order_id',
    alias='dwd_customers_journey',
    merge_exclude_columns=['created_at']
  )
}}

with source_journey as (
  select
    current_timestamp() as sync_ts,
    d.customer_id,
    d.shopify_id,
    ol.shopify_order_id,
    coalesce(ol.shop_url, '') as shop_url,
    coalesce(
      safe_cast(json_value(ol.customer_journey_summary, '$.order_index') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.orderIndex') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.customer_order_index') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.customerOrderIndex') as int64),
      0
    ) as order_index,
    coalesce(
      safe_cast(json_value(ol.customer_journey_summary, '$.day_to_conversion') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.dayToConversion') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.days_to_conversion') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.daysToConversion') as int64),
      0
    ) as day_to_conversion,
    coalesce(safe_cast(json_value(ol.customer_journey_summary, '$.ready') as bool), false) as ready,
    coalesce(
      nullif(json_value(ol.customer_journey_summary, '$.first_landing_page'), ''),
      nullif(json_value(ol.customer_journey_summary, '$.firstLandingPage'), ''),
      nullif(json_value(ol.customer_journey_summary, '$.first_visit.landing_page'), ''),
      nullif(json_value(ol.customer_journey_summary, '$.firstVisit.landingPage'), ''),
      ''
    ) as first_landing_page,
    safe_cast(coalesce(
      json_value(ol.customer_journey_summary, '$.first_occurred_at'),
      json_value(ol.customer_journey_summary, '$.firstOccurredAt'),
      json_value(ol.customer_journey_summary, '$.first_visit.occurred_at'),
      json_value(ol.customer_journey_summary, '$.firstVisit.occurredAt')
    ) as timestamp) as first_occurred_at,
    coalesce(
      json_value(ol.customer_journey_summary, '$.first_referral_code'),
      json_value(ol.customer_journey_summary, '$.firstReferralCode'),
      json_value(ol.customer_journey_summary, '$.first_visit.referral_code'),
      json_value(ol.customer_journey_summary, '$.firstVisit.referralCode')
    ) as first_referral_code,
    coalesce(
      json_value(ol.customer_journey_summary, '$.first_referrer_url'),
      json_value(ol.customer_journey_summary, '$.firstReferrerUrl'),
      json_value(ol.customer_journey_summary, '$.first_visit.referrer_url'),
      json_value(ol.customer_journey_summary, '$.firstVisit.referrerUrl')
    ) as first_referrer_url,
    coalesce(
      json_value(ol.customer_journey_summary, '$.first_source'),
      json_value(ol.customer_journey_summary, '$.firstSource'),
      json_value(ol.customer_journey_summary, '$.first_visit.source'),
      json_value(ol.customer_journey_summary, '$.firstVisit.source')
    ) as first_source,
    coalesce(
      json_value(ol.customer_journey_summary, '$.first_source_type'),
      json_value(ol.customer_journey_summary, '$.firstSourceType'),
      json_value(ol.customer_journey_summary, '$.first_visit.source_type'),
      json_value(ol.customer_journey_summary, '$.firstVisit.sourceType')
    ) as first_source_type,
    coalesce(
      to_json_string(json_query(ol.customer_journey_summary, '$.first_utm_parameters')),
      to_json_string(json_query(ol.customer_journey_summary, '$.firstUtmParameters')),
      to_json_string(json_query(ol.customer_journey_summary, '$.first_visit.utm_parameters')),
      to_json_string(json_query(ol.customer_journey_summary, '$.firstVisit.utmParameters'))
    ) as first_utm_parameters,
    coalesce(
      nullif(json_value(ol.customer_journey_summary, '$.last_landing_page'), ''),
      nullif(json_value(ol.customer_journey_summary, '$.lastLandingPage'), ''),
      nullif(json_value(ol.customer_journey_summary, '$.last_visit.landing_page'), ''),
      nullif(json_value(ol.customer_journey_summary, '$.lastVisit.landingPage'), ''),
      ''
    ) as last_landing_page,
    safe_cast(coalesce(
      json_value(ol.customer_journey_summary, '$.last_occurred_at'),
      json_value(ol.customer_journey_summary, '$.lastOccurredAt'),
      json_value(ol.customer_journey_summary, '$.last_visit.occurred_at'),
      json_value(ol.customer_journey_summary, '$.lastVisit.occurredAt')
    ) as timestamp) as last_occurred_at,
    coalesce(
      json_value(ol.customer_journey_summary, '$.last_referral_code'),
      json_value(ol.customer_journey_summary, '$.lastReferralCode'),
      json_value(ol.customer_journey_summary, '$.last_visit.referral_code'),
      json_value(ol.customer_journey_summary, '$.lastVisit.referralCode')
    ) as last_referral_code,
    coalesce(
      json_value(ol.customer_journey_summary, '$.last_referrer_url'),
      json_value(ol.customer_journey_summary, '$.lastReferrerUrl'),
      json_value(ol.customer_journey_summary, '$.last_visit.referrer_url'),
      json_value(ol.customer_journey_summary, '$.lastVisit.referrerUrl')
    ) as last_referrer_url,
    coalesce(
      json_value(ol.customer_journey_summary, '$.last_source'),
      json_value(ol.customer_journey_summary, '$.lastSource'),
      json_value(ol.customer_journey_summary, '$.last_visit.source'),
      json_value(ol.customer_journey_summary, '$.lastVisit.source')
    ) as last_source,
    coalesce(
      json_value(ol.customer_journey_summary, '$.last_source_type'),
      json_value(ol.customer_journey_summary, '$.lastSourceType'),
      json_value(ol.customer_journey_summary, '$.last_visit.source_type'),
      json_value(ol.customer_journey_summary, '$.lastVisit.sourceType')
    ) as last_source_type,
    coalesce(
      to_json_string(json_query(ol.customer_journey_summary, '$.last_utm_parameters')),
      to_json_string(json_query(ol.customer_journey_summary, '$.lastUtmParameters')),
      to_json_string(json_query(ol.customer_journey_summary, '$.last_visit.utm_parameters')),
      to_json_string(json_query(ol.customer_journey_summary, '$.lastVisit.utmParameters'))
    ) as last_utm_parameters,
    json_query(ol.customer_journey_summary, '$.moments') as moments,
    coalesce(
      safe_cast(json_value(ol.customer_journey_summary, '$.moments_count.count') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.momentsCount.count') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.moments_count') as int64),
      safe_cast(json_value(ol.customer_journey_summary, '$.momentsCount') as int64),
      array_length(ifnull(json_query_array(ol.customer_journey_summary, '$.moments'), []))
    ) as moments_count
  from {{ ref('int_customer_journey_latest') }} as ol
  inner join {{ ref('dwd_orders') }} as d
    on d.order_id = ol.shopify_order_id
  where d.customer_id is not null
    and d.customer_id != 'unknown'
    and d.shopify_id is not null
  {{ incremental_updated_filter('ol') }}
),
existing as (
  {% if is_incremental() %}
  select shopify_order_id, created_at
  from {{ this }}
  {% else %}
  select
    cast(null as int64) as shopify_order_id,
    cast(null as timestamp) as created_at
  where false
  {% endif %}
)

select
  coalesce(e.created_at, s.sync_ts) as created_at,
  s.sync_ts as updated_at,
  s.customer_id,
  s.shopify_id,
  s.shopify_order_id,
  s.shop_url,
  s.order_index,
  s.day_to_conversion,
  s.ready,
  s.first_landing_page,
  s.first_occurred_at,
  s.first_referral_code,
  s.first_referrer_url,
  s.first_source,
  s.first_source_type,
  s.first_utm_parameters,
  s.last_landing_page,
  s.last_occurred_at,
  s.last_referral_code,
  s.last_referrer_url,
  s.last_source,
  s.last_source_type,
  s.last_utm_parameters,
  s.moments,
  s.moments_count
from source_journey as s
left join existing as e
  on e.shopify_order_id = s.shopify_order_id
