{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    alias='dwd_orders',
    merge_exclude_columns=['created_at']
  )
}}

with map_latest as (
  select
    shopify_id,
    array_agg(customer_id order by updated_at desc limit 1)[safe_offset(0)] as customer_id
  from {{ ref('dwd_customer_shopify_map') }}
  group by shopify_id
),
source_orders as (
  select
    current_timestamp() as sync_ts,
    ol.order_id,
    coalesce(map_latest.customer_id, 'unknown') as customer_id,
    ol.shopify_id,
    coalesce(ol.order_name, '') as order_name,
    ol.note,
    ol.tags,
    coalesce(ol.email, '') as email,
    ol.phone,
    ol.app_id,
    ol.refunds,
    ol.currency,
    ol.shop_url,
    ol.closed_at,
    ol.confirmed,
    ol.browser_ip,
    coalesce(ol.order_created_at, current_timestamp()) as order_created_at,
    ol.order_deleted_at,
    to_json(array(
      select json_value(li, '$.sku')
      from unnest(ifnull(json_query_array(ol.line_items, '$'), [])) as li
      where json_value(li, '$.sku') is not null
    )) as sku,
    to_json(array(
      select safe_cast(json_value(li, '$.product_id') as int64)
      from unnest(ifnull(json_query_array(ol.line_items, '$'), [])) as li
      where json_value(li, '$.product_id') is not null
    )) as shopify_product_id,
    ol.order_updated_at,
    ol.order_cancelled_at,
    ol.source_name,
    coalesce(ol.total_price, 0) as total_price,
    ol.fulfillments,
    ol.landing_site,
    ol.order_processed_at,
    ol.total_weight,
    ol.contact_email,
    ol.client_details,
    ol.discount_codes,
    ol.referring_site,
    (
      select sum(safe_cast(json_value(sl, '$.discounted_price') as numeric))
      from unnest(ifnull(json_query_array(ol.shipping_lines, '$'), [])) as sl
    ) as shipping_price,
    ol.subtotal_price,
    ol.billing_address,
    ol.customer_locale,
    ol.deleted_message,
    ol.duties_included,
    ol.estimated_taxes,
    ol.total_discounts,
    ol.total_price_usd,
    ol.shipping_address,
    ol.current_total_tax,
    ol.total_outstanding,
    ol.fulfillment_status,
    ol.current_total_price,
    ol.total_discounts_set,
    ol.presentment_currency,
    ol.current_total_tax_set,
    ol.discount_applications,
    ol.payment_gateway_names,
    ol.current_subtotal_price,
    ol.total_line_items_price,
    ol.buyer_accepts_marketing,
    ol.current_total_discounts
  from {{ ref('int_orders_latest') }} as ol
  left join map_latest
    on ol.shopify_id = map_latest.shopify_id
  where ol.shopify_id is not null
  {{ incremental_updated_filter('ol') }}
),
existing as (
  {% if is_incremental() %}
  select order_id, created_at
  from {{ this }}
  {% else %}
  select
    cast(null as int64) as order_id,
    cast(null as timestamp) as created_at
  where false
  {% endif %}
)

select
  coalesce(e.created_at, s.sync_ts) as created_at,
  s.sync_ts as updated_at,
  s.order_id,
  s.customer_id,
  s.shopify_id,
  s.order_name,
  s.note,
  s.tags,
  s.email,
  s.phone,
  s.app_id,
  s.refunds,
  s.currency,
  s.shop_url,
  s.closed_at,
  s.confirmed,
  s.browser_ip,
  s.order_created_at,
  s.order_deleted_at,
  s.sku,
  s.shopify_product_id,
  s.order_updated_at,
  s.order_cancelled_at,
  s.source_name,
  s.total_price,
  s.fulfillments,
  s.landing_site,
  s.order_processed_at,
  s.total_weight,
  s.contact_email,
  s.client_details,
  s.discount_codes,
  s.referring_site,
  s.shipping_price,
  s.subtotal_price,
  s.billing_address,
  s.customer_locale,
  s.deleted_message,
  s.duties_included,
  s.estimated_taxes,
  s.total_discounts,
  s.total_price_usd,
  s.shipping_address,
  s.current_total_tax,
  s.total_outstanding,
  s.fulfillment_status,
  s.current_total_price,
  s.total_discounts_set,
  s.presentment_currency,
  s.current_total_tax_set,
  s.discount_applications,
  s.payment_gateway_names,
  s.current_subtotal_price,
  s.total_line_items_price,
  s.buyer_accepts_marketing,
  s.current_total_discounts
from source_orders as s
left join existing as e
  on e.order_id = s.order_id
