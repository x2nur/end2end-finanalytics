
with 
merchant as (
	select 
		merchant_id -- PK
		, merchant_city
		, merchant_state
		, zip
		, mcc 
		, row_number() over (partition by merchant_id order by event_date) as row_num
	from {{ source('staging', 'transactions') }}
)
select 
	merchant_id
	, merchant_city
	, merchant_state
	, zip
	, mcc as merchant_code
from merchant
where row_num = 1

