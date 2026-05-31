{{ config(materialized='view') }}

select
    id::uuid                        as payment_method_id,
    name::text                      as payment_method_name,
    provider::text                  as provider,
    is_digital::boolean             as is_digital,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'payment_methods') }}
