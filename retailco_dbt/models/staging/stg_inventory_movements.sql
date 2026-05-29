{{ config(materialized='view') }}

select
    id::uuid as movement_id,
    "productId"::uuid as product_id,
    "storeId"::uuid as store_id,
    "movementType"::text as movement_type,
    quantity::int as quantity,
    "referenceId"::uuid as reference_id,
    "referenceType"::text as reference_type,
    notes::text as notes,
    "movedAt"::timestamp as moved_at,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'inventory_movements') }}