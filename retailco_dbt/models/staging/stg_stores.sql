{{ config(materialized='view') }}

select
    id::uuid                        as store_id,
    name::text                      as store_name,
    city::text                      as city,
    state::text                     as state,
    address::text                   as address,
    phone::text                     as phone,
    manager_name::text              as manager_name,
    opened_date::date               as opened_date,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'stores') }}