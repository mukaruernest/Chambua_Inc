with orders as (
    select * from {{ref ('stg_orders')}}
), shipments as (
    select * from {{ref ('stg_shipments_deliveries')}}
),
date_difference as (	
	select 
		sd.*,
		o.order_date,
        product_id,
		(sd.shipment_date - o.order_date) as late_delivery_date_difference,
		cast('2022-09-06' as date) -  o.order_date as undelivered_date_difference
	from shipments sd
	left join orders o on o.order_id = sd.order_id
)
select 
    order_id,
    product_id,
    case 
        when (late_delivery_date_difference >= 6) and (delivery_date is null) then 'late' 
        when (late_delivery_date_difference < 6) and (delivery_date is not null) then 'early'
        when (delivery_date is null and shipment_date is null) and (undelivered_date_difference > 15) then 'undelivered'
    end as late_early_undelivered
from date_difference