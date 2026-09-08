-- showcase (silver quality) that the pipeline detected bad records,
--preserved lineage, cleaned analytical val, and retained explicit quality flags.


select
    instrument,

    count(*) as total_samples,

    count(*) filter (
        where invalid_temperature
    ) as invalid_temperature_samples,

    count(*) filter (
        where invalid_yield
    ) as invalid_yield_samples,

    count(*) filter (
        where invalid_purity
    ) as invalid_purity_samples,

    round(
        100.0 * count(*) filter (where invalid_temperature)
        / nullif(count(*), 0),
        2
    ) as invalid_temperature_pct,

    round(
        100.0 * count(*) filter (where invalid_yield)
        / nullif(count(*), 0),
        2
    ) as invalid_yield_pct,

    round(
        100.0 * count(*) filter (where invalid_purity)
        / nullif(count(*), 0),
        2
    ) as invalid_purity_pct

from analytics.deduped_lab_samples

group by instrument

order by instrument;