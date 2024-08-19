{{
    config(
        materialized = 'table',
    )
}}

with days as (

    {{
        dbt.date_spine(
            'day',
            "strptime('2020-01-01','%Y-%m-%d')",
            "strptime('2030-01-01','%Y-%m-%d')"
        )
    }}

),

final as (
    select cast(date_day as date) as date,
    {{ extract_datetime_components('date') }}
    from days
)

select * from final
