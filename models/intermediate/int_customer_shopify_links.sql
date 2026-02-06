select distinct
  map.customer_id,
  ods.id as shopify_id,
  ods.updated_at
from {{ ref('stg_ods_shopify_customers') }} as ods
inner join {{ ref('dwd_customer_email_phone_map') }} as map
  on (
    (ods.email is not null and ods.email = coalesce(map.email, ''))
    or (ods.phone is not null and ods.phone = coalesce(map.phone, ''))
  )
where ods.id is not null
