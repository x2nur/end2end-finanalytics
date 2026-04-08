
select 
	id integer
	,client_id int 
	,card_brand varchar 
	,card_type varchar 
	,card_number varchar 
	,cvv integer 
	,has_chip varchar 
	,num_cards_issued integer 
	,credit_limit decimal(102) 
	,year_pin_last_changed integer 
	,card_on_dark_web varchar 
	,expire_year integer 
	,expire_month integer 
from {{ source('staging', 'cards') }}
