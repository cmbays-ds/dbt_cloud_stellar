-- models/intermediate/int_medical_group_performance.sql
{{
  config(
    materialized='table',
    description='Combined performance metrics for medical groups across all plans'
  )
}}

with monthly_performance as (
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
    end as mg_monthly_aac_percent
  from {{ ref('int_awv_monthly_summary') }}
  group by year, month, year_month, medical_group_id
),

annual_performance as (
  select
    year,
    medical_group_id,
    sum(unique_patients_with_awv) as mg_unique_patients_with_awv,
    sum(total_awv_visits) as mg_total_awv_visits,
    sum(patient_panel_size) as mg_patient_panel_size,
    case
      when sum(patient_panel_size) = 0 then null
      else round(100.0 * sum(unique_patients_with_awv) / sum(patient_panel_size), 2)
    end as mg_annual_awv_rate_percent
  from {{ ref('int_awv_annual_summary') }}
  group by year, medical_group_id
),

combined_performance as (
  select
    m.year,
    m.month,
    m.year_month,
    m.medical_group_id,
    m.mg_total_actions_available,
    m.mg_total_actions_attested_as_complete,
    m.mg_monthly_aac_percent,
    a.mg_unique_patients_with_awv,
    a.mg_total_awv_visits,
    a.mg_patient_panel_size,
    a.mg_annual_awv_rate_percent
  from monthly_performance m
  left join annual_performance a
    on m.year = a.year
    and m.medical_group_id = a.medical_group_id
),

final as (
  select
    year,
    month,
    year_month,
    medical_group_id,
    mg_total_actions_available,
    mg_total_actions_attested_as_complete,
    mg_monthly_aac_percent,
    mg_unique_patients_with_awv,
    mg_total_awv_visits,
    mg_patient_panel_size,
    mg_annual_awv_rate_percent,
    case
      when mg_monthly_aac_percent >= 4.0 then 'Good'
      when mg_monthly_aac_percent between 3.0 and 3.999 then 'Needs Improvement'
      when mg_monthly_aac_percent < 3.0 then 'Good'
      else 'No Data'
    end as mg_aac_performance_status,
    case
      when mg_annual_awv_rate_percent >= 70.0 then 'Good'
      when mg_annual_awv_rate_percent between 60.0 and 69.999 then 'Needs Improvement'
      when mg_annual_awv_rate_percent < 60.0 then 'Good'
      else 'No Data'
    end as mg_awv_rate_performance_status
  from combined_performance
)

select * from final
