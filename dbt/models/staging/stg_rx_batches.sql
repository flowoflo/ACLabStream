-- the sql takes the two RX jsons, and provides one consistent shape
-- firmware 1.x is flatter and Celsius-based, while firmware 2.x nests those fields and uses Kelvin.
-- eg. older RX: schema/batchID/timestamp, tempp_c, catalyst, loading_molpct, solvent
/*
{
  "schema": "1.4",
  "batch_id": "RX000123",
  "timestamp": "2026-08-10T14:30:00",
  "temp_c": 85.2,
  "catalyst": "Pd/C",
  "loading_molpct": 1.5,
  "solvent": "THF"
}
*/

-- eg. newer RX: schmaversion/batchId, startedat, conditions (temp, catalyst, name (pdc), loading - value - unit)
/*
{
  "schemaVersion": "2.1",
  "batchId": "RX-2100-000124",
  "startedAt": "2026-08-10T15:00:00.000Z",
  "conditions": {
    "temperature": {"value": 358.35, "unit": "K"},
    "catalyst": {
      "name": "Pd/C",
      "loading": {"value": 1.5, "unit": "mol%"}
    },
    "solvent": "THF"
  }
}
*/

--output
/*

batch_id          schema_version   started_at           temperature_c   catalyst   loading_molpct   solvent
RX000123          1.4              2026-08-10 14:30     85.20           Pd/C       1.50             THF
RX-2100-000124    2.1              2026-08-10 15:00     85.20           Pd/C       1.50             THF

*/

with source as (

    select
        id,
        source_file,
        ingested_at,
        payload
    from {{ source('raw', 'raw_lab_json') }}

)

select
    id as raw_id,
    source_file,
    ingested_at,

    -- Firmware/schema version
    coalesce(        payload ->> 'schema',        payload ->> 'schemaVersion'    ) as schema_version,

    -- Batch ID changed names between firmware versions
    coalesce(        payload ->> 'batch_id',        payload ->> 'batchId'    ) as batch_id,

    -- Normalize timestamps to timestamptz
    case
        when payload ? 'timestamp'            then (payload ->> 'timestamp')::timestamp at time zone 'UTC'
        else            (payload ->> 'startedAt')::timestamptz
    end as started_at,

    -- v1 gives Celsius; v2 gives Kelvin.
    case        when payload ? 'temp_c'            then (payload ->> 'temp_c')::numeric
        else            (payload #>> '{conditions,temperature,value}')::numeric - 273.15
    end as temperature_c,

    -- Catalyst moved into conditions.catalyst.name
    coalesce(        payload ->> 'catalyst',        payload #>> '{conditions,catalyst,name}'    ) as catalyst,

    -- Catalyst loading also moved/nested
    coalesce(        (payload ->> 'loading_molpct')::numeric,        (payload #>> '{conditions,catalyst,loading,value}')::numeric    ) as loading_molpct,
    coalesce(        payload ->> 'solvent',        payload #>> '{conditions,solvent}'    ) as solvent

from source