{{ config(materialized='table') }}
select
    md5(cast(payment_id as text))   as flagged_payment_sk,
    payment_id,
    order_id,
    customer_id,
    amount_paid,
    currency,
    status,
    payment_type,
    case
        when amount_paid = 0 then 'zero_amount'
        when amount_paid < 0 and payment_type != 'refund' then 'unexplained_negative'
        else 'other'
    end                             as flag_reason,
    created_at
from {{ ref('stg_payments') }}
where amount_paid = 0
   or (amount_paid < 0 and payment_type != 'refund')