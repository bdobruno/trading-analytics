{{ config (materialized='view') }}

with tickers as (
    select * from {{source('stocksdb', 'tickers')}}
),

watchlist_events as (
    select * from {{source('stocksdb', 'watchlist_events')}}
)

select
    t.ticker,
    we.wl_type,
    we.event_time,
    we.event_type
from tickers t
join watchlist_events we on t.id = we.ticker_id
