{{ config(materialized='view') }}

select
    id::uuid                        as product_id,
    sku::text                       as sku,
    name::text                      as product_name,
    category::text                  as category,
    sub_category::text              as sub_category,
    brand::text                     as brand,
    supplier::text                  as supplier,
    cost_price::numeric             as cost_price,
    selling_price::numeric          as selling_price,
    effective_from::timestamp       as effective_from,
    is_deleted::boolean             as is_deleted,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'products') }}
