{{config (materialised='view') }}

with ticker_data as (
    select * from {{ref('ticker_data')}}
)

select
    ticker,
    strftime(event_time, '%d %B %Y') as event_time,
    wl_type
from ticker_data
where wl_type = 'gappers'
order by event_time desc, ticker
