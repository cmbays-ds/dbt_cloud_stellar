-- models/marts/kimball/dim_date_spine_day.sql
{{
    config(
        materialized="table",
        description="dimension table for dates using a date spine for dates missing in data",
    )
}}

{% set action_avail_dates = dbt_utils.get_column_values(ref('stg_actions_available'), 'date_day') | reject('equalto', none) | list %}
{% set attested_dates = dbt_utils.get_column_values(ref('stg_actions_attested_as_complete'), 'date_day') | reject('equalto', none) | list %}
{% set claim_dates = dbt_utils.get_column_values(ref('stg_claims_awv'), 'awv_date_of_service') | reject('equalto', none) | list %}
{% set all_dates = action_avail_dates + attested_dates + claim_dates %}
{% set min_date = all_dates | min %}
{% set max_date = all_dates | max %}
with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="'" ~ min_date|string ~ "'",
            end_date="'" ~ max_date|string ~ "'"
        )
    }}
)

select
    cast(date_day as date) as date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    day(date_day) as day,
    to_char(date_day, 'YYYY-MM') as year_month,
    weekiso(date_day) as week_number,
    dayofweekiso(date_day) as day_of_week,  -- Day of week (1=Monday, 7=Sunday)
    case when dayofweekiso(date_day) in (6, 7) then 1 else 0 end as is_weekend  -- Is weekend (1 if Saturday or Sunday, else 0)
from date_spine
