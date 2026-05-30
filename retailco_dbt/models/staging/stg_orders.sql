{{ config(materialized='view') }}

select
    id::uuid as order_id,
    "customerId"::uuid as customer_id,
    "storeId"::uuid as store_id,
    "employeeId"::uuid as employee_id,
    status::text as status,
    "discountCode"::text as discount_code,
    "discountAmount"::numeric as discount_amount,
    "totalAmount"::numeric as total_amount,
    "orderedAt"::timestamp as ordered_at,
    "paidAt"::timestamp as paid_at,
    "shippedAt"::timestamp as shipped_at,
    "deliveredAt"::timestamp as delivered_at,
    "cancelledAt"::timestamp as cancelled_at,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'orders') }}