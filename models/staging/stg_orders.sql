with orders as (
    select 
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        cast(product_id as varchar) as product_id,
        unit_price,
        quantity,
        total_price as amount
    from {{source ('chambua_inc', 'orders')}}
)
select * from orders