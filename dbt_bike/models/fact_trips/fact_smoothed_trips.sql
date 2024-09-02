{{ config(materialized='table', sort='datetime') }}


with fact_smoothed_trips as (
    select 
    datetime, 
    station_trips as station_trips,
    avg(station_trips) over (order by datetime rows between 6 PRECEDING AND CURRENT ROW) as station_trips_7_day_rolling_avg, 
    avg(station_trips) over (order by datetime rows between 13 PRECEDING AND CURRENT ROW) as station_trips_14_day_rolling_avg,
    avg(station_trips) over (order by datetime rows between 29 PRECEDING AND CURRENT ROW) as station_trips_30_day_rolling_avg
    from {{ ref('fact_trips_daily') }}
)

select * from fact_smoothed_trips