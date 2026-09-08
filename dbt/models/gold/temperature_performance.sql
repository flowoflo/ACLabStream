with batches as (

    select *
    from {{ ref('batch_summary') }}
    where temperature_c is not null

),

bucketed as (

    select
        *,

        case
            when temperature_c < 60  then '<60°C'
            when temperature_c < 80  then '60-79°C'
            when temperature_c < 100 then '80-99°C'
            when temperature_c < 120 then '100-119°C'
            else '120°C+'
        end as temperature_range

    from batches

)

select
    catalyst,
    temperature_range,

    count(*) as batch_count,
    sum(sample_count) as sample_count,

    avg(temperature_c) as avg_temperature_c,

    avg(avg_yield_pct) as avg_yield_pct,
    min(min_yield_pct) as min_yield_pct,
    max(max_yield_pct) as max_yield_pct,

    avg(avg_purity_pct) as avg_purity_pct,
    min(min_purity_pct) as min_purity_pct,
    max(max_purity_pct) as max_purity_pct

from bucketed

group by
    catalyst,
    temperature_range