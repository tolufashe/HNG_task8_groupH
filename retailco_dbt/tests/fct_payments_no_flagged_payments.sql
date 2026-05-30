select p.*
from {{ ref('fct_payments') }} p
inner join {{ ref('flagged_payments') }} fp
    on p.payment_id = fp.payment_id