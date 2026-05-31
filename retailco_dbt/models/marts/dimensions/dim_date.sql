{{ config(materialized='table') }}

with date_spine as (
    select generate_series(
        '2023-01-01'::date,
        '2028-12-31'::date,
        '1 day'::interval
    )::date as date_day
),

nigerian_holidays as (
    select unnest(array[
        '2023-01-01', '2023-04-07', '2023-04-10',
        '2023-05-01', '2023-06-12', '2023-10-01',
        '2023-12-25', '2023-12-26',
        '2024-01-01', '2024-03-29', '2024-04-01',
        '2024-05-01', '2024-06-12', '2024-10-01',
        '2024-12-25', '2024-12-26',
        '2025-01-01', '2025-04-18', '2025-04-21',
        '2025-05-01', '2025-06-12', '2025-10-01',
        '2025-12-25', '2025-12-26'
    ]::date[]) as holiday_date
)

select
    d.date_day as date_key,
    extract(year from d.date_day)::int as year,
    extract(quarter from d.date_day)::int as quarter,
    extract(month from d.date_day)::int as month,
    to_char(d.date_day, 'Month') as month_name,
    extract(week from d.date_day)::int as week,
    extract(dow from d.date_day)::int as day_of_week,
    to_char(d.date_day, 'Day') as day_name,
    case when extract(dow from d.date_day) in (0,6)
         then true else false end as is_weekend,
    case when h.holiday_date is not null
         then true else false end as is_public_holiday
from date_spine d
left join nigerian_holidays h
    on d.date_day = h.holiday_date