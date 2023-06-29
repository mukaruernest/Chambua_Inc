with shipment_performance as (
	select * from {{ref ('stg_shipment_performance')}}
)
select 
	cast(now() as date) as ingestion_date,
	count(case when late_early_undelivered = 'late' then True end) as tt_late_shipments,
	count(case when late_early_undelivered = 'undelivered' then True end) as tt_undelivered_shipmnets
from shipment_performance