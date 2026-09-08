---Checks that each catalyst and temperature range combination appears only once in the temperature performance table.

--catalyst	    temperature_range	row_count
--Catalyst-A	    20-30C	            2
--Catalyst-B	    30-40C	            2

--

select
    catalyst, temperature_range, count(*) as row_count

from {{ ref('temperature_performance') }}

group by catalyst, temperature_range

--If this query returns rows, dbt marks the test as failed because duplicates were found
having count(*) > 1