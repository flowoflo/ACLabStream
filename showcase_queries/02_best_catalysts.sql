--which catalysts perform the best overall?

select
    catalyst,
    batch_count,
    sample_count,
    round(avg_temperature_c, 2) as avg_temperature_c,
    round(avg_loading_molpct, 2) as avg_loading_molpct,
    round(avg_yield_pct, 2) as avg_yield_pct,
    round(avg_purity_pct, 2) as avg_purity_pct,
    round(max_yield_pct, 2) as best_observed_yield_pct
from analytics.catalyst_performance
order by avg_yield_pct desc;