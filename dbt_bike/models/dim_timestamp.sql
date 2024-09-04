
{{
    config(
    materialized = 'table', 
    sort = 'datetime'
    )
}}


select unnest(generate_series(
	(select (min(datetime)::timestamp at time zone 'utc')::timestamp from {{ref('stg_trips')}} ),
	(select (max(datetime)::timestamp at time zone 'utc')::timestamp from {{ref('stg_trips')}} ),
	interval '1 hour'
)) as datetime