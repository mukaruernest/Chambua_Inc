{% test ingestion_date(model, column_name) %}

with validation as (

    select
        {{ column_name }} as ingestion_date

    from {{ model }}

),

validation_errors as (

    select
        ingestion_date

    from validation
    -- if this is true, then ingestion date is not correct
    where ingestion_date != cast(now() as date)

)

select *
from validation_errors

{% endtest %}