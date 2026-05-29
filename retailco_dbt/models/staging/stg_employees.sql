{{ config(materialized='view') }}

select
    id::uuid as employee_id,
    "storeId"::uuid as store_id,
    "firstName"::text as first_name,
    "lastName"::text as last_name,
    email::text as email,
    role::text as role,
    "hiredDate"::date as hired_date,
    "isDeleted"::boolean as is_deleted,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at
from {{ source('raw', 'employees') }}