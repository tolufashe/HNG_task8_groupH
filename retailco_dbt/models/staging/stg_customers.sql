{{ config(materialized='view') }}

select
    id::uuid as customer_id,
    "firstName"::text as first_name,
    "lastName"::text as last_name,
    email::text as email,
    phone::text as phone,
    segment::text as segment,
    tier::text as tier,
    address::text as address,
    city::text as city,
    state::text as state,
    "effectiveFrom"::timestamp as effective_from,
    "registeredAt"::timestamp as registered_at,
    "isDeleted"::boolean as is_deleted,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'customers') }}