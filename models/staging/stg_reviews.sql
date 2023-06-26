with reviews as(
    select 
        cast(review as integer),
        cast(product_id as varchar)
    from {{source ('chambua_inc', 'reviews')}}
)
select * from reviews