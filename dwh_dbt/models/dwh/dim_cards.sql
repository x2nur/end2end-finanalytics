
select 
	{{ dbt_utils.star(from=source('staging', 'cards'), except=['acct_open_year', 'acct_open_month']) }}
from {{ source('staging', 'cards') }}
