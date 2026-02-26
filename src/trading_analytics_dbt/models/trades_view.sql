{{ config (materialized='view') }}

with closed_trades as (
    select * from {{ref('closed_trades')}}
    ),

base_metrics as (
    select
        trade_id,
        symbol,
        side,
        date_opened,
        qty,
        entry_price,
        stop_price,
        exit_price,
        date_closed,
        equity,
        qty * entry_price as capital_required,
        datediff('day', date_opened, date_closed) as duration_days,
        round(abs(entry_price - stop_price) * qty,2) as risk_size,
        round(
            case
                when side = 'bullish' then ((exit_price - entry_price) / (entry_price - stop_price))
                when side = 'bearish' then ((entry_price - exit_price) / (stop_price - entry_price))
            end
        ,2) as risk_reward
from closed_trades),

calculated_metrics as (
    select
        *,
        risk_size / equity as risk_per_trade,
        (risk_size / equity) * risk_reward as return_pct
    from base_metrics
)

select
    *,
    return_pct / case when duration_days = 0 then 1 else duration_days end as return_per_day_pct
from calculated_metrics
