
---Checks that each instrument and batch ID combination appears only once in the batch summary.

--instrument	batch_id	row_count
--RX-100	    B-102	        2
--RX-200       B-215           2

select
    instrument, batch_id, count(*) as row_count

from {{ ref('batch_summary') }}

group by instrument,batch_id

--If this query returns rows, dbt marks the test as failed because duplicates were found
having count(*) > 1