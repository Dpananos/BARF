{{ config(materialized='table', sort='datetime') }}


with source as (
      select * from {{ source('raw', 'raw_trip_data') }}
),

most_recent_etl as (
    select
        max(_etl_loaded_at)
    from source

), 


renamed as (
    select
        {{ adapter.quote("datetime") }},
        {{ adapter.quote("station_trips") }},
        {{ adapter.quote("_etl_loaded_at") }}
    from source
    where _etl_loaded_at = (select * from most_recent_etl)
)

select * from renamed
  