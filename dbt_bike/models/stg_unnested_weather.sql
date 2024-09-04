{{
    config(
    materialized = 'table', 
    sort = 'datetime'
    )
}}

{% set columns = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "wind_speed_10m",
    "wind_speed_80m",
    "wind_speed_120m",
    "wind_speed_180m",
    "wind_direction_10m",
    "wind_direction_80m",
    "wind_direction_120m",
    "wind_direction_180m",
    "wind_gusts_10m",
    "shortwave_radiation",
    "direct_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "global_tilted_irradiance",
    "vapour_pressure_deficit",
    "cape",
    "evapotranspiration",
    "et0_fao_evapotranspiration",
    "precipitation",
    "snowfall",
    "precipitation_probability"
] %}


with stg_weather as (
    select * from {{ ref('stg_weather')}}
)



select 
    latitude, 
    longitude, 
    generationtime_ms, 
    utc_offset_seconds, 
    timezone, 
    timezone_abbreviation, 
    elevation, 
    {% for column in columns %}
        UNNEST(hourly.{{ column }}) AS {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %},
    _etl_loaded_at
from {{ ref('stg_weather') }}
