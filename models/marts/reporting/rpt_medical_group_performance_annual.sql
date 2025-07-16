-- models/marts/reporting/rpt_medical_groups_performance_annual.sql
{{
  config(
    materialized='table',
    description='Annual AAC percentage and AWV claims reporting for operations team'
  )
}}

with medical_group_dim as (
  SELECT * FROM {{ ref('dim_medical_group') }}
),

plan_dim as (
    SELECT * FROM {{ ref('dim_plan') }}
),

int_medical_group_plan_awv_summary_annual as (
    SELECT * FROM {{ ref('int_medical_group_plan_awv_summary_annual') }}
),

-- Combined medical group avw performance (all plans aggregated)
int_medical_group_awv_summary_annual as (
    SELECT * FROM {{ ref('int_medical_group_awv_summary_annual') }}
),

final as (
    select
        mgpv.medical_group_id,
        mgpv.plan_id,
        mgpv.date_year,
        mg.medical_group_name,
        p.plan_name,

        -- medical group plan additive measures
        mgpv.visit_count,

        -- medical_group plan static dataset measure (same across all months)
        mgpv.patient_count,

        -- medical group plan running total measures
        mgpv.visit_target_gap,
        mgpv.remaining_patients_without_visit,

        -- medical group plan annual rates
        mgpv.awv_rate AS mgp_awv_rate,

        -- medical group plan annual indicators
        mgpv.awv_performance_status as mgp_awv_performance_status, -- target >= 0.7

        -- medical group annual indicators (aggregated across plans)
        mgv.awv_performance_status as mg_awv_performance_status

    from int_medical_group_plan_awv_summary_annual mgpv
    left join medical_group_dim mg
        on mgpv.medical_group_id = mg.medical_group_id
    left join plan_dim p
        on mgpv.plan_id = p.plan_id
    left join int_medical_group_awv_summary_annual mgv
        on mgpv.date_year = mgv.date_year
        and mgpv.medical_group_id = mgv.medical_group_id
)

select * from final
