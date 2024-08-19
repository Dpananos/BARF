{{ config(materialized='table', sort='datetime') }}

with stg_trips as (
    select 
    datetime, 
    station_trips 
    from {{ ref('stg_fact_trip')}}
), 


final as (

select 
    cast("datetime" as date) as date, 
    sum(station_trips) as station_trips
from stg_trips
group by 1 
having count(*)=24
order by 1
)

select * from final