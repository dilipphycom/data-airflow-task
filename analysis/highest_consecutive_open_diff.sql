with ordered as (
  select
    date,
    open,
    lag(open) over(order by date) as prev_open
  from `data-task-phycom.final_dataset.stocke_market_daily_data`
),
diffs as (
  select
    date,
    open,
    prev_open,
    abs(open - prev_open) as diff
  from ordered
  where prev_open is not null
)
select
  date,
  prev_open,
  open,
  diff
from diffs
order by diff desc
limit 1;
