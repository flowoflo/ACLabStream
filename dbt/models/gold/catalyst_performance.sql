with batches as (

    select *
    from {{ ref('batch_summary') }}

)

select
    catalyst,

    count(*) as batch_count,
    sum(sample_count) as sample_count,

    avg(temperature_c) as avg_temperature_c,
    avg(loading_molpct) as avg_loading_molpct,

    avg(avg_yield_pct) as avg_yield_pct,
    min(min_yield_pct) as min_yield_pct,
    max(max_yield_pct) as max_yield_pct,

    avg(avg_purity_pct) as avg_purity_pct,
    min(min_purity_pct) as min_purity_pct,
    max(max_purity_pct) as max_purity_pct

from batches

group by catalyst