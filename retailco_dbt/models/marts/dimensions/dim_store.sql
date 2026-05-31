{{ config(materialized='table') }}
select
    md5(cast(store_id as text)) as store_sk,
    store_id,
    store_name,
    city,
    state,
    address,
    phone,
    manager_name,
    opened_date
from {{ ref('stg_stores') }}