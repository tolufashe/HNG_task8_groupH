{{ config(materialized='table') }}

select
    md5(cast(payment_id as text)) as flagged_payment_sk,
    payment_id,
    order_id,
    customer_id,
    amount_paid,
    currency,
    status,
    payment_type,
    flag_reason,
    created_at
from {{ ref('stg_payments') }}
where flag_reason is not null