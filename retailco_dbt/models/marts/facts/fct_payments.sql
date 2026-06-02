{{ config(materialized='table') }}

{{ config(materialized='table') }}
select
    md5(cast(p.payment_id as text)) as payment_sk,
    p.payment_id,
    p.order_id,
    dc.customer_sk,
    ds.store_sk,
    dpm.payment_method_sk,
    dd.date_key,
    p.amount_paid,
    p.currency,
    p.status,
    p.payment_type
from {{ ref('stg_payments') }} p
left join {{ ref('stg_orders') }} o
    on p.order_id = o.order_id
left join {{ ref('dim_customer') }} dc
    on p.customer_id = dc.customer_id
    and dc.is_current = true
left join {{ ref('dim_store') }} ds
    on o.store_id = ds.store_id
left join {{ ref('dim_payment_method') }} dpm
    on p.payment_method_id = dpm.payment_method_id
left join {{ ref('dim_date') }} dd
    on p.paid_at::date = dd.date_key
where not (
    p.amount_paid = 0
    or (p.amount_paid < 0 and p.payment_type != 'refund')
)