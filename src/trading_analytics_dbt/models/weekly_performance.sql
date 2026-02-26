{{config (materialised='view')}}

with trades_view as (
    select * from {{ref('trades_view')}}
),

trade_count as (
    select * from {{ref('trade_count')}}
)

select
    strftime(tc.date_opened, '%G%V') AS week,
    sum(tc.nr_of_trades) as nr_of_trades,
    sum(tc.winning_trades)::float / sum(tc.nr_of_trades) as accuracy,
    avg(tv.risk_per_trade) as avg_risk_per_trade,
    COALESCE(avg(case when tv.risk_reward > 0 then tv.risk_reward end),0) as avg_win,
    COALESCE(avg(case when tv.risk_reward < 0 then tv.risk_reward end),0) as avg_loss,
    avg(return_pct) as avg_return_pct,
    sum(return_pct) as total_return_pct
from trade_count tc
join trades_view tv on tc.date_opened = tv.date_opened
group by strftime(tc.date_opened, '%G%V')
