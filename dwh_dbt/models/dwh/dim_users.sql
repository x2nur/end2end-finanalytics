
select
	id as client_id -- PK
	, current_age
	, retirement_age
	, birth_year
	, birth_month
	, gender
	, address
	, latitude
	, longitude
	, per_capita_income
	, yearly_income
	, total_debt
	, credit_score
	, num_credit_cards
	, is_retired
from {{ source('staging', 'users') }}
