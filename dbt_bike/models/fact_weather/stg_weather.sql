{{ config(materialized='table', sort='datetime') }}

with source as (
    select * from {{ source('raw', 'raw_weather_data') }}
)

select 
    latitude, 
    longitude, 
    generationtime_ms, 
    utc_offset_seconds, 
    timezone, 
    timezone_abbreviation, 
    elevation, 
    UNNEST(hourly.time) AS datetime,
    UNNEST(hourly.temperature_2m) AS temperature_2m,
    UNNEST(hourly.relative_humidity_2m) AS relative_humidity_2m,
    UNNEST(hourly.dew_point_2m) AS dew_point_2m,
    UNNEST(hourly.apparent_temperature) AS apparent_temperature,
    UNNEST(hourly.pressure_msl) AS pressure_msl,
    UNNEST(hourly.surface_pressure) AS surface_pressure,
    UNNEST(hourly.cloud_cover) AS cloud_cover,
    UNNEST(hourly.cloud_cover_low) AS cloud_cover_low,
    UNNEST(hourly.cloud_cover_mid) AS cloud_cover_mid,
    UNNEST(hourly.cloud_cover_high) AS cloud_cover_high,
    UNNEST(hourly.wind_speed_10m) AS wind_speed_10m,
    UNNEST(hourly.wind_speed_80m) AS wind_speed_80m,
    UNNEST(hourly.wind_speed_120m) AS wind_speed_120m,
    UNNEST(hourly.wind_speed_180m) AS wind_speed_180m,
    UNNEST(hourly.wind_direction_10m) AS wind_direction_10m,
    UNNEST(hourly.wind_direction_80m) AS wind_direction_80m,
    UNNEST(hourly.wind_direction_120m) AS wind_direction_120m,
    UNNEST(hourly.wind_direction_180m) AS wind_direction_180m,
    UNNEST(hourly.wind_gusts_10m) AS wind_gusts_10m,
    UNNEST(hourly.shortwave_radiation) AS shortwave_radiation,
    UNNEST(hourly.direct_radiation) AS direct_radiation,
    UNNEST(hourly.direct_normal_irradiance) AS direct_normal_irradiance,
    UNNEST(hourly.diffuse_radiation) AS diffuse_radiation,
    UNNEST(hourly.global_tilted_irradiance) AS global_tilted_irradiance,
    UNNEST(hourly.vapour_pressure_deficit) AS vapour_pressure_deficit,
    UNNEST(hourly.cape) AS cape,
    UNNEST(hourly.evapotranspiration) AS evapotranspiration,
    UNNEST(hourly.et0_fao_evapotranspiration) AS et0_fao_evapotranspiration,
    UNNEST(hourly.precipitation) AS precipitation,
    UNNEST(hourly.snowfall) AS snowfall,
    UNNEST(hourly.precipitation_probability) AS precipitation_probability, 
    _etl_loaded_at
from source
