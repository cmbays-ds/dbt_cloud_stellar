-- models/marts/reporting/rpt_medical_group_plan_performance_annual.sql
{{
    config(
        materialized="table",
        description="Annual AAC percentage and AWV claims reporting for operations team",
    )
}}

with
    import_medical_group_plan_dim as (
        select * from {{ ref("dim_medical_group_plan") }}
    ),

    import_date_spine_dim as (
        select distinct date_year from {{ ref("dim_date_spine_day") }} where year = 2024
    ),

    int_medical_group_plan_awv_summary_annual as (
        select * from {{ ref("int_medical_group_plan_awv_summary_annual") }}
    ),

    -- Combined medical group avw performance (all plans aggregated)
    int_medical_group_awv_summary_annual as (
        select * from {{ ref("int_medical_group_awv_summary_annual") }}
    ),

    final as (
        select
            mg.medical_group_id,
            mg.plan_id,
            mgpv.date_year,
            mg.medical_group_name,
            mg.plan_name,

            -- medical group plan additive measures
            mgpv.visit_count,

            -- medical_group plan static dataset measure (same across all months)
            mgpv.patient_count,

            -- medical group plan running total measures
            mgpv.visit_target_gap,
            mgpv.remaining_patients_without_visit,

            -- medical group plan annual rate and indicator
            cast(mgpv.awv_rate as float) as mgp_awv_rate,
            mgpv.awv_performance_status as mgp_awv_performance_status,  -- target >= 0.7

            -- medical group annual rate and indicators (aggregated across plans)
            mgv.awv_rate as mg_awv_rate,
            mgv.awv_performance_status as mg_awv_performance_status

        from import_medical_group_plan_dim mg
        cross join import_date_spine_dim ds
        left join
            int_medical_group_plan_awv_summary_annual mgpv
            on mgpv.medical_group_id = mg.medical_group_id
            and mg.plan_id = mgpv.plan_id
            and ds.date_year = mgpv.date_year

        left join
            int_medical_group_awv_summary_annual mgv
            on mgpv.date_year = mgv.date_year
            and mg.medical_group_id = mgv.medical_group_id
            and ds.date_year = mgv.date_year
    )

select *
from final
