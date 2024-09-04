{{
    config(
    materialized = 'table', 
    sort = 'datetime'
    )
}}

with raw_weather as (

    select 
        *
    from {{ source('raw','raw_weather_data') }}

)

select * from raw_weather
