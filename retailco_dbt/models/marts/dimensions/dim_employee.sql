{{ config(materialized='table') }}

select
    md5(cast(employee_id as text)) as employee_sk,
    employee_id,
    store_id,
    first_name,
    last_name,
    email,
    role,
    hired_date,
    is_deleted
from {{ ref('stg_employees') }}