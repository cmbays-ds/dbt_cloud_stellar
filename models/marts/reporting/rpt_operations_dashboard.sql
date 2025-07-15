-- models/marts/reporting/rpt_operations_dashboard.sql
{{
  config(
    materialized='table',
    description='Operations dashboard view for medical group performance monitoring'
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

combined_metrics as (
  select
    m.year,
    m.month,
    m.year_month,
    m.medical_group_id,
    m.plan_id,
    m.total_actions_available,
    m.total_actions_attested_as_complete,
    m.monthly_aac_percent,
    m.aac_performance_status,
    a.unique_patients_with_awv,
    a.total_awv_visits,
    a.patient_panel_size,
    a.annual_awv_rate_percent,
    a.awv_rate_performance_status
  from monthly_performance m
  left join annual_performance a
    on m.year = a.year
    and m.medical_group_id = a.medical_group_id
    and m.plan_id = a.plan_id
),

final as (
  select
    c.year,
    c.month,
    c.year_month,
    c.medical_group_id,
    mg.medical_group_name,
    c.plan_id,
    p.plan_name,
    c.total_actions_available,
    c.total_actions_attested_as_complete,
    c.monthly_aac_percent,
    c.aac_performance_status,
    c.unique_patients_with_awv,
    c.total_awv_visits,
    c.patient_panel_size,
    c.annual_awv_rate_percent,
    c.awv_rate_performance_status,
    -- Performance indicators for dashboard
    case
      when c.monthly_aac_percent >= 4.0 and c.annual_awv_rate_percent >= 70.0 then 'Excellent'
      when c.monthly_aac_percent >= 4.0 or c.annual_awv_rate_percent >= 70.0 then 'Good'
      when c.monthly_aac_percent < 4.0 and c.annual_awv_rate_percent < 70.0 then 'Needs Attention'
      else 'Insufficient Data'
    end as overall_performance_status,
    -- Intervention flags
    case
      when c.monthly_aac_percent < 4.0 then 1
      else 0
    end as aac_intervention_needed,
    case
      when c.annual_awv_rate_percent < 70.0 then 1
      else 0
    end as awv_rate_intervention_needed
  from combined_metrics c
  join medical_group_dim mg on c.medical_group_id = mg.medical_group_id
  join plan_dim p on c.plan_id = p.plan_id
)

select * from final
