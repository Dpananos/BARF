{{
    config(
    materialized = 'table', 
    sort = 'toronto_local_time'
    )
}}



select
  a.datetime::timestamp as toronto_local_time, 
  {{extract_datetime_components('a.datetime::timestamp')}},
  b.station_trips, 
  c.temperature_2m, 
  c.relative_humidity_2m,
  c.apparent_temperature, 
  c.cloud_cover,
  c.wind_speed_10m, 
  c.precipitation, 
  c.snowfall
  from {{ref('dim_timestamp')}} a
  left join {{ref('fact_weather')}} c on a.datetime = c.datetime
  left join {{ref('fact_trips')}} b on a.datetime = b.datetime
  order by 1

