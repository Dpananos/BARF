{{
    config(
        materialized = 'table',
    )
}}

with hours as (

    {{
        dbt.date_spine(
            'hour',
            "strptime('2020-01-01','%Y-%m-%d')",
            "strptime('2030-01-01','%Y-%m-%d')"
        )
    }}

),

final as (
    select 
    cast(date_hour as timestamp) as timestamp,
    {{ extract_datetime_components('timestamp') }}
    from hours
)

select * from final
