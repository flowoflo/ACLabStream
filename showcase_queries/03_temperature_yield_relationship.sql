-- does catalyst performance change depending on temperature?
--dashbaord ready
--x = temperature range
--y = average yield
-- series = catalyst
select
    catalyst,
    temperature_range,
    batch_count,
    sample_count,
    round(avg_temperature_c, 2) as avg_temperature_c,
    round(avg_yield_pct, 2) as avg_yield_pct,
    round(avg_purity_pct, 2) as avg_purity_pct
from analytics.temperature_performance
where batch_count >= 2
order by
    catalyst,
    avg_temperature_c;