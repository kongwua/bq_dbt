select
  ods.order_id as shopify_order_id,
  ods.shop_url,
  ods.created_at,
  ods.customer_journey_summary,
  ods.updated_at,
  ods._airbyte_extracted_at
from {{ ref('stg_ods_shopify_customer_journey_summary') }} as ods
where ods.order_id is not null
  and ods.customer_journey_summary is not null
qualify row_number() over (
  partition by ods.order_id
  order by ods.updated_at desc, ods._airbyte_extracted_at desc
) = 1
