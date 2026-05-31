{{ config(materialized='table') }}
select
    md5(cast(dbt_scd_id as text)) as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    segment,
    tier,
    address,
    city,
    state,
    registered_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null
         then true else false end as is_current,
    is_deleted
from {{ source('snapshots', 'snap_customer') }}