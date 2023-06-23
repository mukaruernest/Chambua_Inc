with shipment_deliveries as (
    select 
        shipment_id,
        order_id,
        cast(shipment_date as date) as shipment_date,
        cast(delivery_date as date) as delivery_date
    from {{source ('chambua_inc', 'shipment_deliveries')}}
)
select * from shipment_deliveries