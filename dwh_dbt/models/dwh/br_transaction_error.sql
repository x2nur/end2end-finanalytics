
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
		error_id
		, error as error_name
	from {{ ref('dim_errors') }}
)
, fct_tx as ( -- dag dependency
	select transaction_id
	from {{ ref('fct_transactions') }}
)
select 
	tx.id as transaction_id 
	, errors.error_id
from tx_norm inner join errors using (error_name)

