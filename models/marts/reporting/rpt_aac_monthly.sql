-- models/marts/reporting/rpt_aac_monthly.sql
{{
  config(
    materialized='table',
    description='Monthly AAC percentage reporting for operations team'
  )
}}

with medical_group_dim as (
  select
    medical_group_id,
    medical_group_name
  from {{ ref('dim_medical_group') }}
),

plan_dim as (
  select
    plan_id,
    plan_name
  from {{ ref('dim_plan') }}
),

monthly_performance as (
  select
    year,
    month,
    year_month,
    medical_group_id,
    plan_id,
    total_actions_available,
    total_actions_attested_as_complete,
    monthly_aac_percent,
    aac_performance_status
  from {{ ref('int_awv_monthly_summary') }}
),

-- Combined medical group performance (all plans aggregated)
medical_group_monthly_totals as (
  select
    year,
    month,
    year_month,
    medical_group_id,
    sum(total_actions_available) as mg_total_actions_available,
    sum(total_actions_attested_as_complete) as mg_total_actions_attested_as_complete,
    case
      when sum(total_actions_available) = 0 then null
      else round(100.0 * sum(total_actions_attested_as_complete) / sum(total_actions_available), 2)
    end as mg_monthly_aac_percent,
    case
      when sum(total_actions_available) = 0 then 'No Data'
      when round(100.0 * sum(total_actions_attested_as_complete) / sum(total_actions_available), 2) >= 4.0 then 'Good'
      when round(100.0 * sum(total_actions_attested_as_complete) / sum(total_actions_available), 2) < 3.0 then 'Critically Low'
      else 'Needs Improvement'
    end as mg_aac_performance_status
  from monthly_performance
  group by year, month, year_month, medical_group_id
),

-- Plan-level details
plan_level_details as (
  select
    m.year,
    m.month,
    m.year_month,
    m.medical_group_id,
    mg.medical_group_name,
    m.plan_id,
    p.plan_name,
    m.total_actions_available,
    m.total_actions_attested_as_complete,
    m.monthly_aac_percent,
    m.aac_performance_status
  from monthly_performance m
  join medical_group_dim mg on m.medical_group_id = mg.medical_group_id
  join plan_dim p on m.plan_id = p.plan_id
),

final as (
  select
    pld.year,
    pld.month,
    pld.year_month,
    pld.medical_group_id,
    pld.medical_group_name,
    pld.plan_id,
    pld.plan_name,
    pld.total_actions_available,
    pld.total_actions_attested_as_complete,
    pld.monthly_aac_percent,
    pld.aac_performance_status,
    -- Medical group totals
    mgt.mg_total_actions_available,
    mgt.mg_total_actions_attested_as_complete,
    mgt.mg_monthly_aac_percent,
    mgt.mg_aac_performance_status,
    -- Target indicators
    4.0 as target_aac_percent,
    case
      when pld.monthly_aac_percent >= 4.0 then 0
      else 1
    end as plan_intervention_needed,
    case
      when mgt.mg_monthly_aac_percent >= 4.0 then 0
      else 1
    end as medical_group_intervention_needed
  from plan_level_details pld
  join medical_group_monthly_totals mgt
    on pld.year = mgt.year
    and pld.month = mgt.month
    and pld.medical_group_id = mgt.medical_group_id
)

select * from final
