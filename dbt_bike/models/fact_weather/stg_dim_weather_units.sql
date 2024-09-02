{{ config(materialized='table', sort='datetime') }}

with source as (
    select * from {{ source('raw', 'raw_weather_data') }}
)

select 
    unnest(hourly.time) as datetime,
    unnest(hourly_units),
    _etl_loaded_at
from source
