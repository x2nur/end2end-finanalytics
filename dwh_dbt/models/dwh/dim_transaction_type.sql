-- Transaction type dimension

with tx_types as (
	select use_chip as transaction_type
	from {{ source('staging', 'transactions') }}
	group by 1
)
select 
	{{ dbt_utils.generate_surrogate_key(['transaction_type']) }} as id
	, transaction_type
from tx_types 


