{{ config(materialized='view') }}

select
    id::uuid                        as payment_id,
    order_id::uuid                  as order_id,
    customer_id::uuid               as customer_id,
    payment_method_id::uuid         as payment_method_id,
    amount_paid::numeric            as amount_paid,
    currency::text                  as currency,
    status::text                    as status,
    payment_type::text              as payment_type,
    reference::text                 as reference,
    paid_at::timestamp              as paid_at,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'payments') }}
