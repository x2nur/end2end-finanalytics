
-- Bridge between fct_transactions and dim_errors

{{
	config(
		materialized='incremental',
		incremental_strategy='merge',
		unique_key=['transaction_id', 'error_id']
	)
}}


with 
tx as (
	select 
		id 
		, split_to_array(errors, ',') as arr_errors 
	from {{ source('staging', 'transactions') }}
	{% if is_incremental() -%}
	where event_date >= timestamp '{{ var("interval_start") }}' -- and interval_end
	{%- endif %}
)
, tx_norm as (
	select 
		tx_.id 
		, trim(errors.error_name::varchar) as error_name
	from tx as tx_, unnest(tx_.arr_errors) as errors(error_name) 
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
	tx_norm.id as transaction_id 
	, errors.error_id
from tx_norm inner join errors using (error_name)

