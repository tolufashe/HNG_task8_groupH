{{ config(materialized='table') }}
with daily_movements as (
    select
        moved_at::date as movement_date,
        product_id,
        store_id,
        sum(case when movement_type = 'in'
                 then quantity else 0 end) as quantity_in,
        sum(case when movement_type = 'out'
                 then quantity else 0 end) as quantity_out
    from {{ ref('stg_inventory_movements') }}
    group by 1, 2, 3
)
select
    md5(cast(dm.movement_date as text)
        || cast(dm.product_id as text)
        || cast(dm.store_id as text)) as inventory_sk,
    dd.date_key,
    dp.product_sk,
    ds.store_sk,
    dm.quantity_in,
    dm.quantity_out,
    dm.quantity_in - dm.quantity_out as net_movement
from daily_movements dm
left join {{ ref('dim_date') }} dd
    on dm.movement_date = dd.date_key
left join {{ ref('dim_product') }} dp
    on dm.product_id = dp.product_id
    and dp.is_current = true
left join {{ ref('dim_store') }} ds
    on dm.store_id = ds.store_id