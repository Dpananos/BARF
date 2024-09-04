
{{
    config(
    materialized = 'table', 
    sort = 'datetime'
    )
}}


with stg_trips as (
    select * from {{ ref('stg_trips') }}
), 

stg_trips_tz_fixed as (
    select 
    *,
    (datetime::timestamp at time zone 'utc')::timestamp as datetime_toronto
     from stg_trips
),

dim_timestamp as (
    select * from {{ ref('dim_timestamp') }}
)

select
    a.datetime,
    b.station_trips
from dim_timestamp a
left join stg_trips_tz_fixed b
    on a.datetime = b.datetime_toronto
order by a.datetime