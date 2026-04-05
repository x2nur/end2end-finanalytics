
select 
	*
from {{ source('staging', 'users') }}
