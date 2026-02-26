{{ config (materialized='view') }}

with executions as (
    select * from {{source('stocksdb', 'executions')}}
)

select
    trade_id,
    max(filled_at) as filled_at,
    sum(filled_avg_price * filled_qty) / sum(filled_qty) as filled_avg_price,
    sum(filled_qty) as filled_qty,
    symbol,
    account_number
from executions
where position_intent like '%_to_close'
group by trade_id, symbol, side, account_number
