---Checks that each sample appears only once after the lab sample data has been cleaned and deduplicated.

--instrument	batch_id	sample_number	row_count
--RX-100	    B-102	        3	            2
--RX-200	    B-215	        7	            2
--

select
    instrument, batch_id, sample_number, count(*) as row_count

from {{ ref('deduped_lab_samples') }}

group by instrument, batch_id, sample_number

--If this query returns rows, dbt marks the test as failed because duplicates were found
having count(*) > 1