-- models/marts/reporting/rpt_medical_group_performance_annual.sql
{{
  config(
    materialized='table',
    description='Annual AAC percentage and AWV claims reporting for operations team'
  )
}}

with import_medical_group_dim as (
  SELECT * FROM {{ ref('dim_medical_group') }}
),

import_date_spine_dim as (
    SELECT DISTINCT date_year from {{ ref('dim_date_spine_day') }} where year = 2024
),

int_medical_group_awv_summary_annual as (
    SELECT * FROM {{ ref('int_medical_group_awv_summary_annual') }}
),

-- Combined medical group avw performance (all plans aggregated)
int_medical_group_awv_summary_annual as (
    SELECT * FROM {{ ref('int_medical_group_awv_summary_annual') }}
),

final as (
    select
        mg.medical_group_id,
        ds.date_year,
        mg.medical_group_name,

        -- medical group plan additive measures
        mgv.visit_count,

        -- medical_group plan static dataset measure (same across all months)
        mgv.patient_count,

        -- medical group plan running total measures
        mgv.visit_target_gap,
        mgv.remaining_patients_without_visit,

        -- medical group annual rate and indicators (aggregated across plans)
        mgv.awv_rate AS awv_rate,
        mgv.awv_performance_status as awv_performance_status

    from import_medical_group_dim mg
    cross join import_date_spine_dim ds

    left join int_medical_group_awv_summary_annual mgv
        on ds.date_year = mgv.date_year
        and mg.medical_group_id = mgv.medical_group_id
)

select * from final
