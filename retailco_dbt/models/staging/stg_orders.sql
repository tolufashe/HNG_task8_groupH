{{ config(materialized='view') }}

select
    id::uuid                        as order_id,
    customer_id::uuid               as customer_id,
    store_id::uuid                  as store_id,
    employee_id::uuid               as employee_id,
    status::text                    as status,
    discount_code::text             as discount_code,
    discount_amount::numeric        as discount_amount,
    total_amount::numeric           as total_amount,
    ordered_at::timestamp           as ordered_at,
    paid_at::timestamp              as paid_at,
    shipped_at::timestamp           as shipped_at,
    delivered_at::timestamp         as delivered_at,
    cancelled_at::timestamp         as cancelled_at,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'orders') }}
