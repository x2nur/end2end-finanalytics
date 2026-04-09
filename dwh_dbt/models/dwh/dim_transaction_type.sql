-- Transaction type dimension

{{
	config(
		materialized='incremental',
		incremental_strategy='merge',
		unique_key='transaction_type_id'
	)
}}

with tx_types as (
	select use_chip as transaction_type
	from {{ source('staging', 'transactions') }}
	{% if is_incremental() -%}
	where event_date >= timestamp '{{ var("interval_start") }}' -- and interval_end
	{%- endif %}
	group by 1
)
select 
	{{ dbt_utils.generate_surrogate_key(['transaction_type']) }} as transaction_type_id -- PK
	, transaction_type
from tx_types 


