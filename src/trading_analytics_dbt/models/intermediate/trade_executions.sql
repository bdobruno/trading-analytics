{{ config (materialized='view') }}

with entry_executions as (
    select * from {{ref('entry_executions')}}
    ),

exit_executions as (
    select * from {{ref('exit_executions')}}
),

stop_orders as (
    select * from {{source('stocksdb', 'stop_orders')}}
),

unique_stop_orders as (
    select trade_id, min(stop_price) as stop_price
    from stop_orders
    group by trade_id
)

select
    entre.trade_id,
    entre.symbol,
    entre.side,
    entre.filled_at as date_opened,
    entre.filled_qty as entry_qty,
    entre.filled_avg_price as entry_price,
    uso.stop_price,
    exite.filled_qty as exit_qty,
    exite.filled_avg_price as exit_price,
    exite.filled_at as date_closed,
    entre.account_number
from entry_executions entre
left join exit_executions exite on exite.trade_id = entre.trade_id
left join unique_stop_orders uso on uso.trade_id = entre.trade_id
