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
shipments_performance as (
	select 
		*
	from {{ref ('stg_shipment_performance')}}
)
,total_reviews as(
	select 
		product_id, 
		sum(review) as total_reviews, 
		rank() over(order by sum(review) desc ) as ranking
	from reviews
	group by 1	 
 ),get_orders as (
	select 
		o.product_id,
		o.order_date,
		tr.total_reviews,
		count(o.order_id) as order_count,
		rank() over(order by count(o.order_id) desc) as ranking
	from orders o
	left join total_reviews tr on tr.product_id = o.product_id
	where tr.ranking = 1
	group by 1,2,3
 ), get_late_and_early as (
	select 
		g.*,
		count(case when late_early_undelivered = 'late' then true end) as count_late,
		count(case when late_early_undelivered = 'early' then true end) as count_early
	from get_orders g
	left join shipments_performance sp on sp.product_id = g.product_id
	where ranking = 1
	group by 1,2,3,4,5
 ), total_product_reviews as (
	select	
		gle.product_id,
		sum(review) as total_product_reviews
	from reviews as r
	left join orders as o on r.product_id = o.product_id
	left join get_late_and_early as gle on gle.order_date = o.order_date
	where o.product_id = gle.product_id and o.order_date = gle.order_date
	group by 1
 ), base_table as (
	select 
	gle.*,
	total_product_reviews
	from get_late_and_early gle
	left join total_product_reviews as tpr on tpr.product_id = gle.product_id
 ), is_public_holiday as (
	select
		product_id,
		order_date,
		case when (day_of_the_week_num between 1 and 5) and work_day = false then True else False end as is_public_holiday,
		total_reviews,
		(total_reviews * 100) / (total_reviews + total_product_reviews) as pct_dist_ttl_review_points,
		(count_early * 100) / (count_early + count_late) as pct_dist_early_to_late_shipments
	from base_table bs 
	left join dim_dates as d on d.calender_dt = bs.order_date
 )
 select* from is_public_holiday
	


