{{ config(materialized='view') }}

select
    id::uuid                        as customer_id,
    first_name::text                as first_name,
    last_name::text                 as last_name,
    email::text                     as email,
    phone::text                     as phone,
    segment::text                   as segment,
    tier::text                      as tier,
    address::text                   as address,
    city::text                      as city,
    state::text                     as state,
    effective_from::timestamp       as effective_from,
    registered_at::timestamp        as registered_at,
    is_deleted::boolean             as is_deleted,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'customers') }}