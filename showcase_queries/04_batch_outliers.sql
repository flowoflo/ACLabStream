-- which experiments perfomred badly?
-- eg if overall avg = 72% and one batch produced 48% you would seee
-- yield-vs-overall = -24
-- pipeline supports investigation, not just aggregation
with overall as (

    select
        avg(avg_yield_pct) as overall_avg_yield
    from analytics.batch_summary
    where avg_yield_pct is not null

)

select
    b.instrument,
    b.batch_id,
    b.started_at,
    b.catalyst,
    round(b.temperature_c, 2) as temperature_c,
    b.sample_count,
    round(b.avg_yield_pct, 2) as avg_yield_pct,
    round(b.avg_purity_pct, 2) as avg_purity_pct,
    round(
        b.avg_yield_pct - o.overall_avg_yield,
        2
    ) as yield_vs_overall
from analytics.batch_summary b
cross join overall o
where b.avg_yield_pct < o.overall_avg_yield
order by yield_vs_overall asc
limit 20;