{{ config(materialized='table') }}

{{ config(materialized='table') }}
select
    md5(cast(oi.order_item_id as text)) as sales_sk,
    oi.order_id,
    oi.order_item_id,
    dc.customer_sk,
    dp.product_sk,
    ds.store_sk,
    de.employee_sk,
    dd.date_key,
    oi.quantity,
    oi.unit_price,
    oi.discount_pct,
    oi.line_total
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
left join {{ ref('dim_customer') }} dc
    on o.customer_id = dc.customer_id
    and dc.is_current = true
left join {{ ref('dim_product') }} dp
    on oi.product_id = dp.product_id
    and dp.is_current = true
left join {{ ref('dim_store') }} ds
    on o.store_id = ds.store_id
left join {{ ref('dim_employee') }} de
    on o.employee_id = de.employee_id
left join {{ ref('dim_date') }} dd
    on o.ordered_at::date = dd.date_key
where o.status != 'cancelled'