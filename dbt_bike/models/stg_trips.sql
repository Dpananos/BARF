
{{
    config(
    materialized = 'table', 
    sort = 'datetime'
    )
}}

with source_trips as (
    select 
        datetime, 
        "station trips" as station_trips, 
        _etl_loaded_at
    from {{source('raw', 'raw_trip_data')}}
)

select * from source_trips