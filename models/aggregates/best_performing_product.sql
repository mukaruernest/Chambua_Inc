with 
orders as (
	select * from {{ref ('stg_orders')}}
),
reviews as (
	select * from {{ref ('stg_reviews')}}
),
dim_dates as (
	select * from {{ref ('dim_dates')}} 
),
agg_shipments as (
	select * from {{ref ('agg_shipments')}}
)
,total_reviews as(
	select 
		product_id, 
		sum(review) as total_reviews, 
		rank() over(order by sum(review) desc ) as ranking
	from reviews
	group by 1	 
	
)
,get_most_ordered_date as (
	select
		tr.product_id,
		o.order_date,
		total_reviews as total_review_points,
		count(o.order_id) as number_of_orders,
		rank() over(order by count(o.order_id) desc) as order_ranking
	from total_reviews tr
	left join orders o on o.product_id = tr.product_id
	where tr.ranking = 1
	group by 1,2,3
	order by 5
)
-- select sum(review) from reviews
select
	cast(now() as date) as ingestion_date,
	gmo.product_id,
	gmo.order_date,
	case when (day_of_the_week_num between 1 and 5) and (work_day = False) then True else false end as is_public_holiday,
	gmo.total_review_points,
	(gmo.total_review_points/(select sum(review) from reviews)) * 100 as pct_dist_review_points,
	(ag.tt_late_shipments/ag.tt_undelivered_shipmnets) * 100 as pct_dist_early_to_late_shipments
from get_most_ordered_date as gmo
left join dim_dates as d on gmo.order_date = d.calender_dt
left join agg_shipments ag on ag.ingestion_date = cast(now() as date)
where gmo.order_ranking = 1
group by 1,2,3,4,5,7
	
