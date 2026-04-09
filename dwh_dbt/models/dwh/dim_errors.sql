-- Transaction error dimension

{{
	config(
		materialized='incremental',
		incremental_strategy='merge',
		unique_key='error_id'
	)
}}

with 
errors0 as (
	select split_to_array(errors, ',') as errors
	from {{ source('staging', 'transactions') }}
	{% if is_incremental() -%}
	where event_date >= timestamp '{{ var("interval_start") }}' -- add interval_end
	{%- endif %}
	group by 1
), 
errors1 as (
	select trim(error::varchar) as error
	from errors0 as tab, tab.errors as error
	group by 1
)
select 
	{{ dbt_utils.generate_surrogate_key(['error']) }} as error_id 
	, error 
from errors1 
