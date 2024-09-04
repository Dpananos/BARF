{% test all_timestamps(model, column_name, expected_count) %}

-- This test rolls up a timestamp to the day level
-- And ensures all 24 timestamps are present in each date
with count_of_timestamps as (
    select
        {{ column_name }}::date as date, 
        count(*) as cnt
    from {{ model }}
    group by {{ column_name }}::date
)

select  
    * 
from count_of_timestamps 
where cnt != {{ expected_count }}

{% endtest %}
