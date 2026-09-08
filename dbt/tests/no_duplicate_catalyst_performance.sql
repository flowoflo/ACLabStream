---Checks that the catalyst performance table has only one summary row for each catalyst.

--catalyst	    row_count
--Catalyst-A	    2
--Catalyst-B       2

select
    catalyst, count(*) as row_count

from {{ ref('catalyst_performance') }}

group by catalyst

--If this query returns rows, dbt marks the test as failed because duplicates were found
having count(*) > 1