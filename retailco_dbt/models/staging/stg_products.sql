{{ config(materialized='view') }}

select
    id::uuid as product_id,
    sku::text as sku,
    name::text as name,
    category::text as category,
    "subCategory"::text as sub_category,
    brand::text as brand,
    supplier::text as supplier,
    "costPrice"::numeric as cost_price,
    "sellingPrice"::numeric as selling_price,
    "effectiveFrom"::timestamp as effective_from,
    "isDeleted"::boolean as is_deleted,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'products') }}