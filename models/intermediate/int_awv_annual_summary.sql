-- models/intermediate/int_awv_annual_summary.sql
{{
  config(
    materialized='table',
    description='Annual summary of AWV rates by medical group and plan'
  )
}}

with awv_claims as (
  select
    date_day,
    medical_group_id,
    plan_id,
    patient_id,
    awv_visit
  from {{ ref('fct_awv_claim') }}
),

date_dim as (
  select
    date_day,
    year
  from {{ ref('dim_date_spine_day') }}
),

patient_population as (
  select
    medical_group_id,
    plan_id,
    patient_panel_size
  from {{ ref('fct_awv_patient_population') }}
),

annual_awv_counts as (
  select
    d.year,
    c.medical_group_id,
    c.plan_id,
    count(distinct c.patient_id) as unique_patients_with_awv,
    count(*) as total_awv_visits
  from awv_claims c
  join date_dim d on c.date_day = d.date_day
  group by d.year, c.medical_group_id, c.plan_id
),

annual_rates as (
  select
    a.year,
    a.medical_group_id,
    a.plan_id,
    a.unique_patients_with_awv,
    a.total_awv_visits,
    p.patient_panel_size,
    case
      when p.patient_panel_size = 0 then null
      else round(100.0 * a.unique_patients_with_awv / p.patient_panel_size, 2)
    end as annual_awv_rate_percent
  from annual_awv_counts a
  join patient_population p
    on a.medical_group_id = p.medical_group_id
    and a.plan_id = p.plan_id
),

final as (
  select
    year,
    medical_group_id,
    plan_id,
    unique_patients_with_awv,
    total_awv_visits,
    patient_panel_size,
    annual_awv_rate_percent,
    case
      when annual_awv_rate_percent >= 70.0 then 'Good'
      when annual_awv_rate_percent between 60.0 and 69.999 then 'Needs Improvement'
      when annual_awv_rate_percent < 60.0 then 'Critically Low'
      else 'No Data'
    end as awv_rate_performance_status
  from annual_rates
)

select * from final
