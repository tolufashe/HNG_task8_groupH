{{ config(materialized='table') }}
select
    md5(cast(payment_method_id as text)) as payment_method_sk,
    payment_method_id,
    payment_method_name,
    provider,
    is_digital
from {{ ref('stg_payment_methods') }}