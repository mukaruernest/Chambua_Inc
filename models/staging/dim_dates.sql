with order_date as (
    select
        order_date
    from {{ref ('stg_orders')}}
), date_numbers as(
    select
        order_date as calender_dt,
        extract(year from order_date) as year_num,
        extract(month from order_date) as month_of_the_year_num,
        extract(day from order_date) as day_of_the_month_num,
        extract(isodow from order_date) as day_of_the_week_num
    from order_date
), working_day_bool_logic as (
    select
        *,
        case when day_of_the_week_num between 1 and 5 then True else False end as work_day
    from date_numbers
)
select * from working_day_bool_logic

