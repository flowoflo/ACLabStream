/*

This model combines cleaned sample data from the RX-2100 and LabTrak 3000 into one shared dataset.

Both instruments collect similar lab results, 
but their original files have different column names and some fields that only exist for one instrument.

This query lines up the RX sample columns with the LabTrak sample columns.

It adds one shared instrument field, renames LabTrak’s run_id to batch_id,
fills in missing fields with NULL, and UNION ALLs them into one common dataset.

now we can stop worrying whether the source was RX json or Labtrak csv and work with one normalized file.
*/

-- One row = one laboratory sample, regardless of instrument.

with rx as (

    select
        'RX-2100' as instrument,
        raw_id,
        source_file,
        batch_id,
        sample_number,

        started_at,
        null::text as started_at_raw,

        temperature_c,
        catalyst,
        loading_molpct,
        solvent,
        yield_pct,
        purity_pct,

        null::text as operator, --postgres equivalnet of CAST(NULL AS varchar(max)) AS operator,
        null::text as notes,

        sample_flags,
        ingested_at

    from {{ ref('stg_rx_samples') }}

),

labtrak as (

    select
        'LabTrak 3000' as instrument,
        raw_id,
        source_file,
        run_id as batch_id,
        sample_number,

        null::timestamptz as started_at,
        started_at_raw,

        temperature_c,
        catalyst,
        loading_molpct,
        solvent,
        yield_pct,
        purity_pct,

        

        operator,
        notes,

        null::jsonb as sample_flags,

        ingested_at

    from {{ ref('stg_labtrak_samples') }}

)

select * from rx

union all

select * from labtrak