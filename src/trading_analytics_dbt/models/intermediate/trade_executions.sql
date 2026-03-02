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
),

-- single leg entries
single_entries as (
    select
        trade_id,
        symbol,
        side,
        filled_at,
        filled_avg_price,
        filled_qty,
        account_number
    from entry_executions
    where parent_order_id is null
),

-- spread entries collapsed into one row per trade
spread_entries as (
    select
        trade_id,
        regexp_extract(min(symbol), '^([A-Z]+)', 1) || '_SPREAD' as symbol,
        min(side) as side,
        min(filled_at) as filled_at,
        sum(filled_avg_price * filled_qty) / sum(filled_qty) as filled_avg_price,
        min(filled_qty) as filled_qty,
        min(account_number) as account_number
    from entry_executions
    where parent_order_id is not null
    group by trade_id
),

-- single leg exits
single_exits as (
    select
        trade_id,
        filled_avg_price,
        filled_qty,
        filled_at
    from exit_executions
    where parent_order_id is null
),

-- spread exits collapsed into one row per trade
spread_exits as (
    select
        trade_id,
        sum(filled_avg_price * filled_qty) / sum(filled_qty) as filled_avg_price,
        min(filled_qty) as filled_qty,
        max(filled_at) as filled_at
    from exit_executions
    where parent_order_id is not null
    group by trade_id
),

all_entries as (
    select * from single_entries
    union all
    select * from spread_entries
),

all_exits as (
    select * from single_exits
    union all
    select * from spread_exits
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
from all_entries entre
left join all_exits exite on exite.trade_id = entre.trade_id
left join unique_stop_orders uso on uso.trade_id = entre.trade_id
