{% snapshot snap_customer %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=False
    )
}}
select
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
    effective_from,
    registered_at,
    is_deleted,
    updated_at
from {{ ref('stg_customers') }}
{% endsnapshot %}