{% macro extract_datetime_components(datetime_column) %}

-- Extract year
extract(year from {{ datetime_column }}) as year,

-- Extract month
strftime({{ datetime_column }}, '%B') as month, 

-- Extract day of month
extract(day from {{ datetime_column }})::float / (date_diff('day', date_trunc('month', {{ datetime_column }}), date_trunc('month', {{ datetime_column }}) + interval '1 month'))::float as month_pct,


-- Extract day of year
extract(doy from {{ datetime_column }}) as day_of_year,

-- Extract day of week
CASE 
    WHEN dayofweek({{ datetime_column }}) = 0 THEN 'Sunday'
    WHEN dayofweek({{ datetime_column }}) = 1 THEN 'Monday'
    WHEN dayofweek({{ datetime_column }}) = 2 THEN 'Tuesday'
    WHEN dayofweek({{ datetime_column }}) = 3 THEN 'Wednesday'
    WHEN dayofweek({{ datetime_column }}) = 4 THEN 'Thursday'
    WHEN dayofweek({{ datetime_column }}) = 5 THEN 'Friday'
    WHEN dayofweek({{ datetime_column }}) = 6 THEN 'Saturday'
END as day_of_week,

-- Extract hour
extract(hour from {{ datetime_column }}) as hour

{% endmacro %}
