-- Transaction error dimension

with errors as (
	select 
		error_type
	from {{ source('staging', 'transactions') }}
	group by 1
)
select 
	{{ dbt_utils.generate_surrogate_key(['error_type']) }} as id 
	, error_type
from errors
