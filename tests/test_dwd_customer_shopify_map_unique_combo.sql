select customer_id, shopify_id, count(*) as cnt
from {{ ref('dwd_customer_shopify_map') }}
group by 1,2
having count(*) > 1
