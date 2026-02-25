{{config (materialised='view')}}

with pets as (
    select * from {{ref('pets')}}
),

gappers as (
    select * from {{ref('gappers')}}
)


select
    ticker,
    event_time,
    wl_type
from pets

UNION ALL

select
    ticker,
    event_time,
    wl_type
from gappers
where strptime(event_time, '%d %B %Y') = CURRENT_DATE
