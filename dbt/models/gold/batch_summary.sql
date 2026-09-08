with samples as (

    select *
    from {{ ref('deduped_lab_samples') }}

)

select
    instrument,
    batch_id,

    min(started_at) as started_at,

    min(catalyst) as catalyst,
    avg(temperature_c) as temperature_c,
    avg(loading_molpct) as loading_molpct,
    min(solvent) as solvent,

    count(*) as sample_count,

    avg(yield_pct) as avg_yield_pct,
    min(yield_pct) as min_yield_pct,
    max(yield_pct) as max_yield_pct,

    avg(purity_pct) as avg_purity_pct,
    min(purity_pct) as min_purity_pct,
    max(purity_pct) as max_purity_pct,

    bool_or(invalid_temperature) as has_invalid_temperature,
    bool_or(invalid_yield) as has_invalid_yield,
    bool_or(invalid_purity) as has_invalid_purity

from samples

group by
    instrument,
    batch_id