{{config(materialised='view')}}

with trade_executions as (
    select * from {{ref('trade_executions')}}
),

account_snapshots as (
    select * from {{source('stocksdb', 'account_snapshots')}}
)

select
    te.trade_id,
    te.symbol,
    te.side,
    te.date_opened,
    te.entry_qty as qty,
    te.entry_price,
    te.stop_price,
    te.exit_price,
    te.date_closed,
    te.account_id,
    asn.equity
from trade_executions te
left join account_snapshots asn on asn.account_id = te.account_id and asn.date = date(te.date_opened)
where exit_qty = entry_qty
