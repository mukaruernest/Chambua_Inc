with public_holidays as (
	select
		month_of_the_year_num
	from {{ref ('dim_dates')}}
	where (day_of_the_week_num between 1 and 5) and (work_day = False)
),
agg_orders as (
    select 
		extract(month from order_date) as month_of_the_year_num,
		count(order_id) as total_orders  
	from {{ref ('stg_orders')}}
	group by 1
)
select  
	cast(now() as date) as ingestion_date,
    count(case when a.month_of_the_year_num = 1 then true end) as tt_order_hol_jan,
    count(case when a.month_of_the_year_num = 2 then True end) as tt_order_hol_feb,
    count(case when a.month_of_the_year_num = 3 then True end) as tt_order_hol_mar,
    count(case when a.month_of_the_year_num = 4 then True end) as tt_order_hol_apr,
    count(case when a.month_of_the_year_num = 5 then True end) as tt_order_hol_may,
    count(case when a.month_of_the_year_num = 6 then True end) as tt_order_hol_jun,
    count(case when a.month_of_the_year_num = 7 then True end) as tt_order_hol_jul,
    count(case when a.month_of_the_year_num = 8 then True end) as tt_order_hol_aug,
    count(case when a.month_of_the_year_num = 9 then True end) as tt_order_hol_sep,
    count(case when a.month_of_the_year_num = 10 then True end) as tt_order_hol_oct,
    count(case when a.month_of_the_year_num = 11 then True end) as tt_order_hol_nov,
    count(case when a.month_of_the_year_num = 12 then True end) as tt_order_hol_dec
from agg_orders a 
inner join public_holidays d on d.month_of_the_year_num = a.month_of_the_year_num
