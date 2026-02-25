{{ config (materialized='view') }}

with entry_executions as (
    select * from {{ref('entry_executions')}}
    ),

exit_executions as (
    select * from {{ref('exit_executions')}}
),

stop_orders as (
    select * from {{source('stocksdb', 'stop_orders')}}
)

select
    entre.trade_id,
    entre.symbol,
    entre.side,
    entre.filled_at as date_opened,
    entre.filled_qty as entry_qty,
    entre.filled_avg_price as entry_price,
    so.stop_price,
    exite.filled_qty as exit_qty,
    exite.filled_avg_price as exit_price,
    exite.filled_at as date_closed,
    entre.account_id
from entry_executions entre
left join exit_executions exite on exite.trade_id = entre.trade_id
left join stop_orders so on so.trade_id = entre.trade_id
