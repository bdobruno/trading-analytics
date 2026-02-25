{{config (materialised='view')}}

with monthly_performance as (
    select * from {{ref('monthly_performance')}}
)

select
    substring(month, -4, 4) as year,
    avg(total_return_pct),
    avg(nr_of_trades)
from monthly_performance
group by substring(month, -4, 4)
