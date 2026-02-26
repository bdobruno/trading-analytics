{{config (materialised='view')}}

with daily_performance as (
    select * from {{ref('daily_performance')}}
)

select
    extract(year from day) as year,
    avg(total_return_pct),
    avg(nr_of_trades)
from daily_performance
group by extract(year from day)