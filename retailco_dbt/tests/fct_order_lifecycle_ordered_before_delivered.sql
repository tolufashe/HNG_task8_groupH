select *
from {{ ref('fct_order_lifecycle') }}
where delivered_at is not null
  and ordered_at > delivered_at