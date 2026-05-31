{{ config(materialized='view') }}

select
    id::uuid                        as employee_id,
    store_id::uuid                  as store_id,
    first_name::text                as first_name,
    last_name::text                 as last_name,
    email::text                     as email,
    role::text                      as role,
    hired_date::date                as hired_date,
    is_deleted::boolean             as is_deleted,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'employees') }}
