with orders as (
	select
		extract(month from order_date) as month_of_the_year_num,
		extract(isodow from order_date) as day_of_the_week_num,
		count(order_id) as total_orders
	from {{ref ('stg_orders')}}
	group by 1,2
),dim_dates as(
	select * from {{ref ('dim_dates')}}
), total_orders as (
    select 
        cast(now() as date) as ingestion_date,
        o.month_of_the_year_num,
        o.day_of_the_week_num,
        count(total_orders) as total_order
    from orders o
    left join dim_dates as d on d.month_of_the_year_num = (o.month_of_the_year_num)
    where (d.work_day = False) and (o.day_of_the_week_num between 1 and 5)
    group by 1,2,3
)
select 
ingestion_date,
sum(case when month_of_the_year_num = 1 then total_order end ) as tt_order_hol_jan,
sum(case when month_of_the_year_num = 2 then total_order end ) as tt_order_hol_feb,
sum(case when month_of_the_year_num = 3 then total_order end ) as tt_order_hol_mar,
sum(case when month_of_the_year_num = 4 then total_order end ) as tt_order_hol_apr,
sum(case when month_of_the_year_num = 5 then total_order end ) as tt_order_hol_may,
sum(case when month_of_the_year_num = 6 then total_order end ) as tt_order_hol_jun,
sum(case when month_of_the_year_num = 7 then total_order end ) as tt_order_hol_jul,
sum(case when month_of_the_year_num = 8 then total_order end ) as tt_order_hol_aug,
sum(case when month_of_the_year_num = 9 then total_order end )as tt_order_hol_sep,
sum(case when month_of_the_year_num = 10 then total_order end)  as tt_order_hol_oct,
sum(case when month_of_the_year_num = 11 then total_order end)  as tt_order_hol_nov,
sum(case when month_of_the_year_num = 12 then total_order end ) as tt_order_hol_dec
from total_orders
group by 1
