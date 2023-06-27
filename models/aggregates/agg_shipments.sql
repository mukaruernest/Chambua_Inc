with orders as (
    select * from {{ref ('stg_orders')}}
), shipments as (
    select * from {{ref ('stg_shipments_deliveries')}}
),
date_difference as (	
	select 
		sd.*,
		o.order_date,
		(sd.shipment_date - o.order_date) as late_delivery_date_difference,
		cast('2022-09-06' as date) -  o.order_date as undelivered_date_difference
	from shipments sd
	left join orders as o on o.order_id = sd.order_id
)
select 
	cast(now() as date) as ingestion_date,
	count (case when (late_delivery_date_difference >= 6) and (delivery_date is null) then true end) as tt_late_shipments,
	count (case when (delivery_date is null and shipment_date is null) and (undelivered_date_difference > 15) then true end) as tt_undelivered_shipmnets
from date_difference