with reviews as(
    select * from {{source ('chambua_inc', 'reviews')}}
)
select * from reviews