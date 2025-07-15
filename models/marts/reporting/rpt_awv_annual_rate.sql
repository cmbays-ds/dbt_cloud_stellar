-- models/marts/reporting/rpt_awv_annual_rate.sql
{{
  config(
    materialized='table',
    description='Annual wellness visit rate reporting for operations team'
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

annual_performance as (
  select
    year,
    medical_group_id,
    plan_id,
    unique_patients_with_awv,
    total_awv_visits,
    patient_panel_size,
    annual_awv_rate_percent,
    awv_rate_performance_status
  from {{ ref('int_awv_annual_summary') }}
),

-- Combined medical group performance (all plans aggregated)
medical_group_totals as (
  select
    year,
    medical_group_id,
    sum(unique_patients_with_awv) as mg_unique_patients_with_awv,
    sum(total_awv_visits) as mg_total_awv_visits,
    sum(patient_panel_size) as mg_patient_panel_size,
    case
      when sum(patient_panel_size) = 0 then null
      else round(100.0 * sum(unique_patients_with_awv) / sum(patient_panel_size), 2)
    end as mg_annual_awv_rate_percent,
    case
      when sum(patient_panel_size) = 0 then 'No Data'
      when round(100.0 * sum(unique_patients_with_awv) / sum(patient_panel_size), 2) >= 70.0 then 'Good'
      when round(100.0 * sum(unique_patients_with_awv) / sum(patient_panel_size), 2) < 60.0 then 'Critically Low'
      else 'Needs Improvement'
    end as mg_awv_rate_performance_status
  from annual_performance
  group by year, medical_group_id
),

-- Plan-level details
plan_level_details as (
  select
    a.year,
    a.medical_group_id,
    mg.medical_group_name,
    a.plan_id,
    p.plan_name,
    a.unique_patients_with_awv,
    a.total_awv_visits,
    a.patient_panel_size,
    a.annual_awv_rate_percent,
    a.awv_rate_performance_status
  from annual_performance a
  join medical_group_dim mg on a.medical_group_id = mg.medical_group_id
  join plan_dim p on a.plan_id = p.plan_id
),

final as (
  select
    pld.year,
    pld.medical_group_id,
    pld.medical_group_name,
    pld.plan_id,
    pld.plan_name,
    pld.unique_patients_with_awv,
    pld.total_awv_visits,
    pld.patient_panel_size,
    pld.annual_awv_rate_percent,
    pld.awv_rate_performance_status,
    -- Medical group totals
    mgt.mg_unique_patients_with_awv,
    mgt.mg_total_awv_visits,
    mgt.mg_patient_panel_size,
    mgt.mg_annual_awv_rate_percent,
    mgt.mg_awv_rate_performance_status,
    -- Target indicators
    70.0 as target_awv_rate_percent,
    case
      when pld.annual_awv_rate_percent >= 70.0 then 0
      else 1
    end as plan_intervention_needed,
    case
      when mgt.mg_annual_awv_rate_percent >= 70.0 then 0
      else 1
    end as medical_group_intervention_needed
  from plan_level_details pld
  join medical_group_totals mgt
    on pld.year = mgt.year
    and pld.medical_group_id = mgt.medical_group_id
)

select * from final
