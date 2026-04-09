
{{
	config(
		materialized='incremental',
		incremental_strategy='merge',
		unique_key='merchant_id'
	)
}}

with 
merchant as (
	select 
		merchant_id -- PK
		, merchant_city
		, merchant_state
		, zip
		, mcc 
		, row_number() over (partition by merchant_id order by event_date desc) as row_num
	from {{ source('staging', 'transactions') }}
	{% if is_incremental() -%}
	where event_date >= timestamp '{{ var("interval_start") }}' -- and interval_end
	{%- endif %}
)
, merchant_codes as (
	select * 
	from {{ source('staging', 'mcc_codes') }}
)
select 
	merchant_id
	, merchant_city
	, merchant_state
	, zip
	, merchant.mcc as merchant_code
	, merchant_codes.description as merchant_code_description
from merchant left join merchant_codes using (mcc)
where row_num = 1

