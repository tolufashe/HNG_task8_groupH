{{ config(materialized='view') }}

select
    id::uuid                        as order_item_id,
    order_id::uuid                  as order_id,
    product_id::uuid                as product_id,
    quantity::integer               as quantity,
    unit_price::numeric             as unit_price,
    discount_pct::numeric           as discount_pct,
    line_total::numeric             as line_total,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'order_items') }}
