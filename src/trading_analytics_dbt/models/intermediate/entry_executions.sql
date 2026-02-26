{{ config (materialized='view') }}

with executions as (
    select * from {{source('stocksdb', 'executions')}}
)

select
    trade_id,
    min(filled_at) as filled_at,
    sum(filled_avg_price * filled_qty) / sum(filled_qty) as filled_avg_price,
    sum(filled_qty) as filled_qty,
    symbol,
    case
        when side = 'buy' then 'bullish'
        else 'bearish'
    end as side,
    account_id
from executions
where position_intent like '%_to_open'
group by trade_id, symbol, side, account_id
