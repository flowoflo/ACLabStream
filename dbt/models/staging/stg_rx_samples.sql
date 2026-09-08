/*

--stg_rx_batches acts like an index (one row per file) for our raw file. eg.

--raw:
{
  "batchId": "RX-2100-000124",
  "temperature": 358.35,
  "samples": [
    {"id": "S1", "yield": 0.618},
    {"id": "S2", "yield": 0.621},
    {"id": "S3", "yield": 0.609}
  ]
}


--rx batches:
batch_id         | temp_c | catalyst
RX-2100-000124   | 85.2   | Pd/C


--rx samples will use batches index to explode raw into structured table
batch_id         | sample | temp_c | yield_pct
RX-2100-000124   | 1      | 85.2   | 61.8
RX-2100-000124   | 2      | 85.2   | 62.1
RX-2100-000124   | 3      | 85.2   | 60.9


---model:
raw_lab_json
{
  batchId: "RX-2100-000066",
  temperature: ...,
  samples: [
    {id: "S1", yield: ..., purity: ...},
    {id: "S2", yield: ..., purity: ...},
    {id: "S3", yield: ..., purity: ...}
  ]
}
        │
        ├──────────────→ stg_rx_batches
        │                 RX-2100-000066 | 71.83 | Ru/Al2O3 | ...
        │
        └──────────────→ stg_rx_samples   ← NEXT
                          RX-2100-000066 | S1 | yield...
                          RX-2100-000066 | S2 | yield...
                          RX-2100-000066 | S3 | yield...

*/


-- RX sample-level staging model.
-- One output row = one laboratory sample.

with raw as (

    select
        id as raw_id,        source_file,        payload
    from {{ source('raw', 'raw_lab_json') }}

),

batches as (

    -- Clean batch-level information we already created.
    select *
    from {{ ref('stg_rx_batches') }}

),

-- ---------------------------------------------------------
-- RX firmware 1.x
--
-- Samples are represented as parallel arrays:
--
-- yield_pct:  [61.8, 62.1, 60.9]
-- purity_pct: [96.2, 96.4, 95.9]
--
-- Array position becomes our sample number.

---Firmware 1.x is trickier because it doesn't have sample objects; 
---it has parallel yield_pct and purity_pct arrays, so sample #1 is position 1 in both arrays, sample #2 is position 2, etc.

--eg firmware 2 vs firmware 1
/*
"samples": [
    {
      "id": "S1",
      "yield": 0.18981,
      "purity": 92.48,
      "flags": []
    },
    {
      "id": "S2",
      "yield": 0.17403,
      "purity": 92.627,
      "flags": []
    },
    {
      "id": "S3",
      "yield": 0.18762,
      "purity": 91.604,
      "flags": []
    },

    */

-- firmware 1 was has no IDs, yieldsand purity are arrays
 /*
  "results": {
    "yield_pct": [
      0.0,
      1.32,
      3.69,
      1.89,
      2.1
    ],
    "purity_pct": [
      92.29,
      91.64,
      92.72,
      92.89,
      93.24
    ]
  },
 */   
-- ---------------------------------------------------------

firmware_1_samples as (

    select
        r.raw_id,        y.ordinality::integer as sample_number,

        case
            when y.yield_value is null then null
            else y.yield_value::numeric
        end as yield_pct,

        case
            when r.payload #>> array[
                'results',
                'purity_pct',
                (y.ordinality - 1)::text
            ] is null
                then null
            else (
                r.payload #>> array[
                    'results',
                    'purity_pct',
                    (y.ordinality - 1)::text
                ]
            )::numeric
        end as purity_pct,

        r.payload -> 'flags' as sample_flags

    from raw r

    cross join lateral
        jsonb_array_elements_text(
            r.payload #> '{results,yield_pct}'
        )
        with ordinality as y(yield_value, ordinality)

    where r.payload ? 'schema'

),

-- ---------------------------------------------------------
-- RX firmware 2.x
--
-- Samples are proper nested JSON objects:
--
-- samples: [
--   {"id":"S1", "yield":0.618, ...},
--   {"id":"S2", "yield":0.621, ...}
-- ]
-- ---------------------------------------------------------

firmware_2_samples as (

    select
        r.raw_id,

        replace(sample ->> 'id', 'S', '')::integer            as sample_number,

        -- Firmware 2 stores yield as a fraction eg (0.618) instead of a percentage, so we multiply by 100 while staging it.
        (sample ->> 'yield')::numeric * 100           as yield_pct,

        (sample ->> 'purity')::numeric            as purity_pct,

        sample -> 'flags'            as sample_flags

    from raw r

    cross join lateral
        jsonb_array_elements(r.payload -> 'samples')
        as sample

    where r.payload ? 'schemaVersion'

),

all_samples as (

    select * from firmware_1_samples

    union all

    select * from firmware_2_samples

)

select
    b.raw_id,
    b.source_file,
    b.schema_version,
    b.batch_id,
    b.started_at,
    b.temperature_c,
    b.catalyst,
    b.loading_molpct,
    b.solvent,
    
    s.sample_number,
    s.yield_pct,
    s.purity_pct,
    s.sample_flags,
    
    b.ingested_at

from all_samples s

inner join batches b
    on s.raw_id = b.raw_id