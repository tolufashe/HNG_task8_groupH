{{ config(materialized='view') }}

select
    id::uuid                        as movement_id,
    product_id::uuid                as product_id,
    store_id::uuid                  as store_id,
    movement_type::text             as movement_type,
    quantity::integer               as quantity,
    reference_id::text              as reference_id,
    reference_type::text            as reference_type,
    notes::text                     as notes,
    moved_at::timestamp             as moved_at,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'inventory_movements') }}
