{{ config(materialized='table') }}

{{ config(materialized='table') }}
select
    md5(cast(o.order_id as text)) as lifecycle_sk,
    o.order_id,
    dc.customer_sk,
    ds.store_sk,
    de.employee_sk,
    d_ordered.date_key as ordered_date_key,
    d_paid.date_key as paid_date_key,
    d_shipped.date_key as shipped_date_key,
    d_delivered.date_key as delivered_date_key,
    o.status,
    o.total_amount,
    o.discount_amount,
    o.ordered_at,
    o.paid_at,
    o.shipped_at,
    o.delivered_at,
    o.cancelled_at
from {{ ref('stg_orders') }} o
left join {{ ref('dim_customer') }} dc
    on o.customer_id = dc.customer_id
    and dc.is_current = true
left join {{ ref('dim_store') }} ds
    on o.store_id = ds.store_id
left join {{ ref('dim_employee') }} de
    on o.employee_id = de.employee_id
left join {{ ref('dim_date') }} d_ordered
    on o.ordered_at::date = d_ordered.date_key
left join {{ ref('dim_date') }} d_paid
    on o.paid_at::date = d_paid.date_key
left join {{ ref('dim_date') }} d_shipped
    on o.shipped_at::date = d_shipped.date_key
left join {{ ref('dim_date') }} d_delivered
    on o.delivered_at::date = d_delivered.date_key