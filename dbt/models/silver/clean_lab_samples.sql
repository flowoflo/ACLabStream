-- cleaning data
-- Input: unified_lab_samples
-- One row = one sample
--
-- We've been normalizing data until now. Now we begin to apply data-quality rules

with source as (
    select *
    from {{ ref('unified_lab_samples') }}
)

select
    instrument, raw_id, source_file, batch_id, sample_number, ingested_at,
    
    -- RX already has a proper timestamp.
-- LabTrak dates are still text, so detect their format and parse them.
coalesce(
    started_at,
    case
        -- YYYY-MM-DD HH:MM:SS
        when started_at_raw ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'            then to_timestamp(started_at_raw, 'YYYY-MM-DD HH24:MI:SS')
        -- YYYY-MM-DD
        when started_at_raw ~ '^\d{4}-\d{2}-\d{2}$'                              then to_timestamp(started_at_raw, 'YYYY-MM-DD')
        -- DD/MM/YYYY
        when started_at_raw ~ '^\d{2}/\d{2}/\d{4}$'                              then to_timestamp(started_at_raw, 'DD/MM/YYYY')
        -- MM-DD-YY
        when started_at_raw ~ '^\d{2}-\d{2}-\d{2}$'                              then to_timestamp(started_at_raw, 'MM-DD-YY')
        -- DD-Mon-YYYY
        when started_at_raw ~ '^\d{2}-[A-Za-z]{3}-\d{4}$'                        then to_timestamp(started_at_raw, 'DD-Mon-YYYY')
        else null
    end
) as started_at,

-- Keep the original LabTrak text for traceability.
started_at_raw,

    -- Preserve the standardized-but-unclean value for reference.
    temperature_c as temperature_c_source,
    -- instrument sentinel/error values (negatives, unexpected, or overblown values) become NULLs.
    case when temperature_c in (-999, -999.9, 9999, 0) then null else temperature_c end as temperature_c,

    -- standardize catalyst names.
    case
        when lower(trim(catalyst)) in ('pd/c', 'pd-c', '10% pd/c', 'palladium on carbon')        then 'Pd/C'
        when lower(trim(catalyst)) in ('pto2', 'pto₂', 'adams catalyst')                         then 'PtO2'
        when lower(trim(catalyst)) in ('ni-raney', 'raney ni', 'raney nickel', 'ni(r)', 'ra-ni') then 'Ni-Raney'
        when lower(trim(catalyst)) in ('ru/al2o3', 'ru/alumina', 'ru-al2o3', 'ru / al2o3')       then 'Ru/Al2O3'
        when lower(trim(catalyst)) in ('none', 'blank', 'no cat', 'uncatalysed')                 then 'none'
        else trim(catalyst)
    end as catalyst,

    loading_molpct, nullif(trim(solvent), '') as solvent,

    -- Preserve original standardized measurements.
    yield_pct as yield_pct_source, purity_pct as purity_pct_source,

    -- Physically impossible yields (below zero over 100pc) become NULL.
    case when yield_pct < 0 or yield_pct > 100 then null else yield_pct end as yield_pct,
    case when purity_pct < 0 or purity_pct > 100 then null else purity_pct end as purity_pct,

    operator, notes, sample_flags,

    -- Explicit labels/quality indicators let us know WHY something became NULL.
    temperature_c in (-999, -999.9, 9999, 0) as invalid_temperature,
    (yield_pct < 0 or yield_pct > 100)       as invalid_yield,
    (purity_pct < 0 or purity_pct > 100)     as invalid_purity

from source