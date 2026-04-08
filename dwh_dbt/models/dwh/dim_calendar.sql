
with 
date_range as (
	{{
		dbt_utils.date_spine(
			datepart="day",
			start_date="cast('1900-01-01' as date)",
			end_date="cast('2100-01-01' as date)"
		)
	}}
)
select 
	date_day::date as _date
	, extract(year from date_day) as year
	, extract(quarter from date_day) as quarter
	, extract(month from date_day) as month
	, to_char(date_day, 'Month') as full_month_name 
	, to_char(date_day, 'Mon') as short_month_name 
	, extract(week from date_day) as week
	, (extract(dow from date_day) + 6) % 7 as day_of_week
	, extract(day from date_day) as full_weekday_name
	, to_char(date_day, 'Day') as full_day_name
	, to_char(date_day, 'Dy') as short_day_name
from date_range
