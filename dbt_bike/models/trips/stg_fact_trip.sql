with source as (
      select * from {{ source('raw', 'raw_trip_data') }}
),
renamed as (
    select
        {{ adapter.quote("datetime") }},
        {{ adapter.quote("station_trips") }},
        {{ adapter.quote("__inserted_at") }}

    from source
)
select * from renamed
  