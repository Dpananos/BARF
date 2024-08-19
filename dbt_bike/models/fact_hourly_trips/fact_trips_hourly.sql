{{ config(materialized='table', sort='datetime') }}

with stg_trips as (
    select 
    datetime, 
    station_trips 
    from {{ ref('stg_fact_trip')}}
)

select 
    datetime, 
    station_trips
from stg_trips