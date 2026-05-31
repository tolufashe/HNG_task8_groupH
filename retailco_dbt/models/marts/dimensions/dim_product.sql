{{ config(materialized='table') }}
select
    md5(cast(dbt_scd_id as text)) as product_sk,
    product_id,
    sku,
    product_name,
    category,
    sub_category,
    brand,
    supplier,
    cost_price,
    selling_price,
    effective_from,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null
         then true else false end as is_current,
    is_deleted
from {{ source('snapshots', 'snap_product') }}