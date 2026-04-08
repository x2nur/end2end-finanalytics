
select 
	id integer
	, current_age integer
	, retirement_age integer
	, birth_year integer
	, birth_month integer
	, gender varchar 
	, address varchar 
	, latitude real
	, longitude real
	, per_capita_income decimal(10,2)
	, yearly_income decimal(10,2)
	, total_debt decimal(10,2)
	, credit_score integer
	, num_credit_cards integer
	, is_retired boolean
from {{ source('staging', 'users') }}
