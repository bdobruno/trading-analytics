{{config (materialised='view')}}

with trades_view as (
    select * from {{ref('trades_view')}}
)


select
    sum(
        case
            when tv.risk_reward > 0 then 1 else 0 end
    ) as winning_trades,
    count(*) as nr_of_trades,
    tv.date_opened
from trades_view tv
group by tv.date_opened
