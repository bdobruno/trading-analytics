{{config (materialised='view')}}

with weekly_performance as (
    select * from {{ref('weekly_performance')}}
)

select
    substring(week, 1, 4) as year,
    avg(total_return_pct),
    avg(nr_of_trades)
from weekly_performance
group by substring(week, 1, 4)
