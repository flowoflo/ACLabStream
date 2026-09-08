--deliberately not fixing catalyst aliases, impossible sensor values, or the inconsistent date formats yet; 
--this first staging model is mainly about getting the three LabTrak schemas into one consistent column layout.

--old schema
/*
INPUT payload:
{
  "RUN_ID": "R0032",
  "DATE": "2026-07-08 10:30:00",
  "TEMP": "85.1",
  "CAT": "Pd/C",
  "LOAD": "1.50",
  "YIELD": "72.4%",
  "PURITY": "96.1",
  "OP": "jsm"
}
*/

---middle schema
/*
INPUT payload:
{
  "RUN_ID": "R0091",
  "DATE": "14-Aug-2026",
  "TEMP": "70,4",
  "CAT": "PtO2",
  "LOAD": "2,00",
  "SOLVENT": "THF", -- new
  "YIELD": "81,3",
  "PURITY": "97,5",
  "OP": "M. Müller"
}
*/


--new schema
/*

INPUT payload:
{
  "RUN_ID": "R0177",
  "SAMPLE": "3", -- new
  "DATE": "2026-08-19 11:45:00",
  "TEMP_C": "110.2",
  "CATALYST": "Raney Ni",
  "LOAD_MOLPCT": "2.50",
  "SOLVENT": "MeOH",
  "YIELD_PCT": "61.8",
  "PURITY_PCT": "92.7",
  "OPERATOR": "K. Tanaka",
  "NOTES": "reagent from new bottle" -- new
}
*/

--eg output
/*
| raw_id | source_file                        | row_number | ingested_at          | run_id | sample_number | started_at_raw        | temperature_c | catalyst | loading_molpct | solvent | yield_pct | purity_pct | operator  | notes                   |
| -----: | ---------------------------------- | ---------: | -------------------- | ------ | ------------: | --------------------- | ------------: | -------- | -------------: | ------- | --------: | ---------: | --------- | ----------------------- |
|     12 | `LT3000_20260708_R0032.csv`        |          1 | 2026-08-31 14:00 UTC | R0032  |        `NULL` | `2026-07-08 10:30:00` |          85.1 | Pd/C     |           1.50 | `NULL`  |      72.4 |       96.1 | jsm       | `NULL`                  |
|    187 | `labtrak_export_14-08-2026_91.csv` |          2 | 2026-08-31 14:00 UTC | R0091  |        `NULL` | `14-Aug-2026`         |          70.4 | PtO2     |           2.00 | THF     |      81.3 |       97.5 | M. Müller | `NULL`                  |
|    364 | `LT3000_20260819_R0177.csv`        |          3 | 2026-08-31 14:00 UTC | R0177  |             3 | `2026-08-19 11:45:00` |         110.2 | Raney Ni |           2.50 | MeOH    |      61.8 |       92.7 | K. Tanaka | reagent from new bottle |

*/


--- original staging file
/*
with source as (

    select
        id,
        source_file,
        row_number,
        ingested_at,
        payload
    from {{ source('raw', 'raw_lab_csv') }}

),

normalized as (

    select
        id as raw_id,        source_file,        row_number,        ingested_at,        trim(payload ->> 'RUN_ID') as run_id,
        nullif(trim(payload ->> 'SAMPLE'), '')::integer as sample_number,
        trim(payload ->> 'DATE') as started_at_raw,
        coalesce(            trim(payload ->> 'TEMP'),            trim(payload ->> 'TEMP_C')        ) as temperature_raw,
        coalesce(            trim(payload ->> 'CAT'),            trim(payload ->> 'CATALYST')        ) as catalyst,
        coalesce(            trim(payload ->> 'LOAD'),            trim(payload ->> 'LOAD_MOLPCT')        ) as loading_raw,
        nullif(trim(payload ->> 'SOLVENT'), '') as solvent,
        coalesce(            trim(payload ->> 'YIELD'),            trim(payload ->> 'YIELD_PCT')        ) as yield_raw,
        coalesce(            trim(payload ->> 'PURITY'),            trim(payload ->> 'PURITY_PCT')        ) as purity_raw,
        coalesce(            trim(payload ->> 'OP'),            trim(payload ->> 'OPERATOR')        ) as operator,
        nullif(trim(payload ->> 'NOTES'), '') as notes

    from source
)

select
    raw_id,    source_file,    row_number,    ingested_at,    run_id,    sample_number,    started_at_raw,

    replace(temperature_raw, ',', '.')::numeric as temperature_c,    catalyst,    replace(loading_raw, ',', '.')::numeric as loading_molpct,    solvent,
    replace(        replace(yield_raw, '%', ''),        ',',        '.'    )::numeric as yield_pct,
    replace(        replace(purity_raw, '%', ''),        ',',        '.'    )::numeric as purity_pct,

    operator,    notes

from normalized

*/

--- updated to clean blanks/ "", N/As, '-: text values into real SQL NULLs before we try to convert them to numbers.
---70,4 becomes 70.4, 72.4% becomes 72.4, but "" becomes actual NULL instead of crashing the view.


with source as (

    select
        id,
        source_file,
        row_number,
        ingested_at,
        payload
    from {{ source('raw', 'raw_lab_csv') }}

),

normalized as (

    select
        id as raw_id,        source_file,        row_number,        ingested_at,        trim(payload ->> 'RUN_ID') as run_id,
        coalesce(nullif(trim(payload ->> 'SAMPLE'), '')::integer, row_number) as sample_number, -- updated at silver stage (quality control) to give NULL sample-numbers row-based enumeration
        trim(payload ->> 'DATE') as started_at_raw,
        coalesce(            trim(payload ->> 'TEMP'),            trim(payload ->> 'TEMP_C')        ) as temperature_raw,
        coalesce(            trim(payload ->> 'CAT'),            trim(payload ->> 'CATALYST')        ) as catalyst,
        coalesce(            trim(payload ->> 'LOAD'),            trim(payload ->> 'LOAD_MOLPCT')        ) as loading_raw,
        nullif(trim(payload ->> 'SOLVENT'), '') as solvent,
        coalesce(            trim(payload ->> 'YIELD'),            trim(payload ->> 'YIELD_PCT')        ) as yield_raw,
        coalesce(            trim(payload ->> 'PURITY'),            trim(payload ->> 'PURITY_PCT')        ) as purity_raw,
        coalesce(            trim(payload ->> 'OP'),            trim(payload ->> 'OPERATOR')        ) as operator,
        nullif(trim(payload ->> 'NOTES'), '') as notes

    from source
)

select
    raw_id,    source_file,    row_number,    ingested_at,    run_id,    sample_number,    started_at_raw,

    case when lower(trim(temperature_raw)) in ('', 'n/a', 'null', '-', '--', '?', '#n/a', 'nan') then null else replace(temperature_raw, ',', '.')::numeric end as temperature_c,    
    catalyst,    
    case when lower(trim(loading_raw)) in ('', 'n/a', 'null', '-', '--', '?', '#n/a', 'nan') then null else replace(loading_raw, ',', '.')::numeric end as loading_molpct,    
    solvent,
    case when lower(trim(yield_raw)) in ('', 'n/a', 'null', '-', '--', '?', '#n/a', 'nan') then null else replace(        replace(yield_raw, '%', ''),        ',',        '.'    )::numeric end as yield_pct,
    case when lower(trim(purity_raw)) in ('', 'n/a', 'null', '-', '--', '?', '#n/a', 'nan') then null else replace(        replace(purity_raw, '%', ''),        ',',        '.'    )::numeric end as purity_pct,
    operator,    
    notes

from normalized