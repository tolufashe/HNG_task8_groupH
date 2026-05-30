{{ config(materialized='view') }}

select
    id::uuid as order_item_id,
    "orderId"::uuid as order_id,
    "productId"::uuid as product_id,
    quantity::int as quantity,
    "unitPrice"::numeric as unit_price,
    "discountPct"::numeric as discount_pct,
    "lineTotal"::numeric as line_total,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'order_items') }}