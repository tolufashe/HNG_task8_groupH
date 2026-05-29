{{ config(materialized='view') }}

select
    id::uuid as payment_id,
    "orderId"::uuid as order_id,
    "customerId"::uuid as customer_id,
    "paymentMethodId"::uuid as payment_method_id,
    "amountPaid"::numeric as amount_paid,
    currency::text as currency,
    status::text as status,
    "paymentType"::text as payment_type,
    reference::text as reference,
    "paidAt"::timestamp as paid_at,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    case
        when "amountPaid"::numeric = 0 then 'zero amount'
        when "amountPaid"::numeric < 0
         and "paymentType"::text != 'refund' then 'unexplained negative'
        else null
    end                                 as flag_reason
from {{ source('raw', 'payments') }}