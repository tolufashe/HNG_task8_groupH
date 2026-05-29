{{ config(materialized='view') }}

select
    id::uuid as payment_method_id,
    name::text as name,
    provider::text as provider,
    "isDigital"::boolean as is_digital,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'payment_methods') }}