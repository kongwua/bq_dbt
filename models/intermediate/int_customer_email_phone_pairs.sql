select distinct
  coalesce(email, '') as email,
  coalesce(phone, '') as phone,
  updated_at
from {{ ref('stg_ods_shopify_customers') }}
where (email is not null and email != '')
   or (phone is not null and phone != '')
