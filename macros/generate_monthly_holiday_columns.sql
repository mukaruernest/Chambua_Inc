
{% macro generate_monthly_holiday_columns(column, year_num) %}
    {% set holiday_columns = [] %}
    {% for month_num in range(1, 13) %}
        {% set column_name = 'tt_order_hol_' ~ ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'][month_num-1] %}
        {% set holiday_column = sum_public_holidays(column, month_num, column_name) %}
        {% set holiday_columns = holiday_columns + [holiday_column] %}
    {% endfor %}
    {{ holiday_columns | join(',\n') }}
{% endmacro %}
