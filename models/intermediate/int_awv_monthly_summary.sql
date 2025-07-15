-- models/intermediate/int_awv_monthly_summary.sql
{{
  config(
    materialized='table',
    description='Monthly summary of AWV actions by medical group and plan'
  )
}}

with awv_actions as (
  select
    date_day,
    medical_group_id,
    plan_id,
    count_actions_available,
    count_actions_attested_as_complete,
    aac_percent
  from {{ ref('fct_awv_action') }}
),

date_dim as (
  select
    date_day,
    year,
    month,
    year_month
  from {{ ref('dim_date_spine_day') }}
),

monthly_aggregation as (
  select
    d.year,
    d.month,
    d.year_month,
    a.medical_group_id,
    a.plan_id,
    sum(a.count_actions_available) as total_actions_available,
    sum(a.count_actions_attested_as_complete) as total_actions_attested_as_complete,
    case
      when sum(a.count_actions_available) = 0 then null
      else round(100.0 * sum(a.count_actions_attested_as_complete) / sum(a.count_actions_available), 2)
    end as monthly_aac_percent
  from awv_actions a
  join date_dim d on a.date_day = d.date_day
  group by d.year, d.month, d.year_month, a.medical_group_id, a.plan_id
),

final as (
  select
    year,
    month,
    year_month,
    medical_group_id,
    plan_id,
    total_actions_available,
    total_actions_attested_as_complete,
    monthly_aac_percent,
    case
      when monthly_aac_percent >= 4.0 then 'Good'
      when monthly_aac_percent between 3.0 and 3.999 then 'Needs Improvement'
      when monthly_aac_percent < 3.0 then 'Critically Low'
      else 'No Data'
    end as aac_performance_status
  from monthly_aggregation
)

select * from final
