{{
    config(
    materialized = 'table', 
    sort = 'created_at'
    )
}}


with weather_forecasts as (
    select 
    *
    from 
    {{ source('raw', 'raw_weather_forecast_data') }}
)

select * from weather_forecasts