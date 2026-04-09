-- Transaction error dimension

with 
errors0 as (
	select split_to_array(errors, ',') as errors
	from {{ source('staging', 'transactions') }}
	group by 1
), 
errors1 as (
	select trim(cast(error as varchar) ) as error
	from errors0 as tab, tab.errors as error
	group by 1
)
select 
	{{ dbt_utils.generate_surrogate_key(['error']) }} as id 
	, error 
from errors1 
