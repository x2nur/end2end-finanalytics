-- Trnsactions

with 
tx as (
	select 
		id
		, event_date as event_ts
		, client_id
		, card_id
		, amount
		, trim(use_chip) as transaction_type 
		, merchant_id 
	from {{ source('staging', 'transactions' )}}
)
, cards as ( -- dag dependency
	select * from {{ ref('dim_cards') }}
)
, clients as ( -- dag dependency
	select * from {{ ref('dim_users') }}
)
, merchants as ( -- dag dependency
	select * from {{ ref('dim_merchants') }}
)
, tx_types as (
	select * from {{ ref('dim_transaction_type') }}
)
select 
	tx.id as transaction_id
	, tx.event_ts 
	, tx.event_ts::date as date_id -- point to dim_calendar
	, tx.client_id 
	, tx.card_id 
	, tx.merchant_id 
	, tx.amount 
	, tx_types.transaction_type_id
from tx 
left join tx_types on using (transaction_type)
-- left join cards on using (card_id)
-- left join clients on using (client_id)
-- left join merchants using (merchant_id)

