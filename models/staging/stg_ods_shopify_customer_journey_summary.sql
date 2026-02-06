select *
from {{ source('userprofile', 'ods_shopify_customer_journey_summary') }}
