-- Final deduplication step for Silver.
-- Keep clean_lab_samples untouched so duplicate source records remain auditable.

with source as (
    select *
    from {{ ref('clean_lab_samples') }}
),

ranked as (
    select 
        *, 
        row_number() over (partition by instrument, batch_id, sample_number order by ingested_at, source_file, raw_id) as duplicate_rank
    from source
)

select *
from ranked
where duplicate_rank = 1