{{config (materialised='view') }}

with ticker_data as (
    select * from {{ref('ticker_data')}}
)

select
    ticker,
    strftime(event_time, '%d %B %Y') as event_time,
    wl_type
from (
  select
    ticker,
    event_time,
    event_type,
    wl_type,
    row_number() over (partition by ticker order by event_time desc) as rn
  from ticker_data
  where wl_type = 'pets'
)
where rn = 1 and event_type = 'add'
order by ticker
