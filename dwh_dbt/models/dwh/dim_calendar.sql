
with 
date_range as (
	{{
		dbt_utils.date_spine(
			datepart="day",
			start_date="cast('1900-01-01' as date)",
			end_date="cast('2100-01-01' as date)"
		)
	}}
)
select 
	*	
from date_range
