{{ config(materialized='table', sort='datetime') }}

with stg_trips as (
    select 
    datetime as datetime, 
    station_trips 
    from {{ ref('stg_fact_trip')}}
)

select * from stg_trips