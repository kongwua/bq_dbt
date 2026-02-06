select coalesce(email, '') as email, coalesce(phone, '') as phone, count(*) as cnt
from {{ ref('dwd_customer_email_phone_map') }}
group by 1,2
having count(*) > 1
