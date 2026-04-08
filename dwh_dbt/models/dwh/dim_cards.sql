
select 
	id
	,client_id
	,card_brand
	,card_type
	,card_number
	,cvv
	,has_chip
	,num_cards_issued
	,credit_limit
	,year_pin_last_changed
	,card_on_dark_web
	,expire_year
	,expire_month
from {{ source('staging', 'cards') }}
