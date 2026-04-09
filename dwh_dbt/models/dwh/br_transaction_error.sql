
-- Bridge between fct_transactions and dim_errors

with 
tx as (
	select 
		id 
		, split_to_array(errors, ',') as arr_errors 
	from {{ source('staging', 'transactions') }}
)
, tx_norm as (
	select 
		tx.id 
		, trim(errors.error_name) as error_name
	from tx, unnest(tx.arr_errors) as errors(error_name) 
)
, errors as (
	select 
		id
		, error as error_name
	from {{ ref('dim_errors') }}
)
select 
	tx.id as transaction_id 
	, errors.id as error_id
from tx_norm inner join errors using (error_name)


