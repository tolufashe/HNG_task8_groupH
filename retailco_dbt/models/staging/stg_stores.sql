{{ config(materialized='view') }}

select
    id::uuid as store_id,
    name::text as name,
    city::text as city,
    state::text as state,
    address::text as address,
    phone::text as phone,
    "managerName"::text as manager_name,
    "openedDate"::date as opened_date,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'stores') }}