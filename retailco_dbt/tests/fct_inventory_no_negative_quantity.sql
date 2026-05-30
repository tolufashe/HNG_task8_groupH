select *
from {{ ref('fct_inventory_daily') }}
where quantity_in < 0
   or quantity_out < 0