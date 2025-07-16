-- models/marts/reporting/rpt_medical_group_plan_performance_monthly.sql
{{
    config(
        materialized="table",
        description="Monthly AAC percentage reporting for operations team",
    )
}}

with
    import_medical_group_plan_dim as (
        select * from {{ ref("dim_medical_group_plan") }}
    ),

    import_date_spine_dim as (
        select distinct date_month from {{ ref('dim_date_spine_day') }}
    ),

    -- Combined medical group action performance (all plans aggregated)
    int_medical_group_plan_action_summary_monthly as (
        select * from {{ ref("int_medical_group_plan_action_summary_monthly") }}
    ),

    -- Combined medical group avw performance (all plans aggregated)
    int_medical_group_plan_awv_summary_monthly as (
        select * from {{ ref("int_medical_group_plan_awv_summary_monthly") }}
    ),

    final as (
        select
            mg.medical_group_id,
            mg.medical_group_name,
            mg.plan_id,
            mg.plan_name,
            ds.date_month,
            COALESCE(mga.actions_available_start_month, 0) AS actions_available_start_month,
            COALESCE(mga.actions_available_end_of_month, 0) AS actions_available_end_of_month,
            COALESCE(mga.monthly_net_change_of_actions, 0) AS  monthly_net_change_of_actions,
            COALESCE(mga.days_with_data, 0) AS days_with_data,
            COALESCE(mga.weekend_days, 0) AS weekend_days,

            -- medical group additive measures
             COALESCE(mga.monthly_actions_completed, 0) AS monthly_actions_completed,
             COALESCE(mga.monthly_actions_became_ineligible, 0) AS monthly_actions_became_ineligible,
             COALESCE(mgv.visit_count_per_month, 0) AS visit_count_per_month,

            -- medical_group static dataset measure (same across all months)
             COALESCE(mgv.total_patient_count, 0) AS total_patient_count,

            -- medical group running total measures
             COALESCE(mga.monthly_running_total_actions_completed, 0) AS monthly_running_total_actions_completed,
             COALESCE(mga.monthly_running_total_actions_became_ineligible, 0) AS monthly_running_total_actions_became_ineligible,
             COALESCE(mgv.cumulative_monthly_visits, 0) AS cumulative_monthly_visits,
             COALESCE(mgv.visit_target_gap, 0) AS visit_target_gap,
             COALESCE(mgv.remaining_patients_without_visit, 0) AS remaining_patients_without_visit,



            -- medical group monthly rates (aggregated across plans)
            COALESCE(mga.monthly_completion_rate, 0) as monthly_completion_rate,  -- aka AAC%
            COALESCE(mga.running_monthly_completion_rate, 0) as running_monthly_completion_rate,
            COALESCE(mgv.awv_cumulative_monthly_visit_rate, 0) as cumulative_monthly_awv_visit_rate,
            COALESCE(mga.monthly_lost_opportunity_rate, 0) as monthly_lost_opportunity_rate,
            COALESCE(mga.running_monthly_lost_opportunity_rate, 0) as running_monthly_lost_opportunity_rate,

            -- medical group monthly indicators (aggregated across plans)
            COALESCE(mgv.monthly_awv_performance_status, 'Missing Data') as mg_cumulative_monthly_awv_performance_status,
            COALESCE(mga.lost_opportunity_status, 'Missing Data') as mg_lost_opportunity_status,
            COALESCE(mga.aac_performance_status, 'Missing Data') as mg_aac_performance_status,  -- monthly completion rate indicator -- Target = 0.04

            -- medical group month over month measures (aggregated across plans)
            mga.monthly_completion_rate_prev_month,
            mga.monthly_lost_opportunity_rate_prev_month,
            mga.monthly_completion_rate_change_mom,
            mga.lost_opportunity_rate_change_mom

        from import_medical_group_plan_dim mg
        cross join import_date_spine_dim ds
        left join
            int_medical_group_plan_action_summary_monthly mga
            on mg.medical_group_id = mga.medical_group_id
            and mg.plan_id = mga.plan_id
            and ds.date_month = mga.date_month

        left join
            int_medical_group_plan_awv_summary_monthly mgv
            on ds.date_month = mgv.date_month
            and mg.medical_group_id = mgv.medical_group_id
            and mg.plan_id = mgv.plan_id
    )

select *
from final
