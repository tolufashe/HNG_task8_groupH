{% snapshot dim_product %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=False
    )
}}

select
    product_id,
    sku,
    name,
    category,
    sub_category,
    brand,
    supplier,
    cost_price,
    selling_price,
    effective_from,
    is_deleted
from {{ ref('stg_products') }}

{% endsnapshot %}