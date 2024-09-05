{{
    config(
    materialized = 'table', 
    sort = 'datetime'
    )
}}

with joined_data as (
    select 
    a.datetime, 
    b.* exclude time
    from {{ref('dim_timestamp')}} a
    left join {{ref('stg_unnested_weather')}} b
    on a.datetime = STRPTIME(b.time, '%Y-%m-%dT%H:%M')
    order by a.datetime
)

select * from joined_data

