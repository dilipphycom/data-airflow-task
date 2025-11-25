select
  extract(year from date) as year,
  max(open) as highest_open,
  max(close) as highest_close
from
  `data-task-phycom.final_dataset.stocke_market_daily_data`
group by
  year
order by
  year asc;
