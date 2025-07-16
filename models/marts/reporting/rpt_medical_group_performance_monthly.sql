-- models/marts/reporting/rpt_medical_groups_performance_monthly.sql
{{
  config(
    materialized='table',
    description='Monthly AAC percentage reporting for operations team'
  )
}}

WITH 
medical_group_dim as (
  SELECT * FROM {{ ref('dim_medical_group') }}
),

plan_dim as (
    SELECT * FROM {{ ref('dim_plan') }}
),

int_medical_group_plan_action_summary_monthly as (
    SELECT * FROM {{ ref('int_medical_group_plan_action_summary_monthly') }}
),

int_medical_group_plan_awv_summary_monthly as (
    SELECT * FROM {{ ref('int_medical_group_plan_awv_summary_monthly') }}
),
-- Combined medical group action performance (all plans aggregated)
int_medical_group_action_summary_monthly as (
    SELECT * FROM {{ ref('int_medical_group_action_summary_monthly') }}
),

-- Combined medical group avw performance (all plans aggregated)
int_medical_group_awv_summary_monthly as (
    SELECT * FROM {{ ref('int_medical_group_awv_summary_monthly') }}
),

final as (
    SELECT
        mgpa.medical_group_id,
        mgpa.plan_id,
        mg.medical_group_name,
        p.plan_name,
        mgpa.date_month,
        mgpa.actions_available_start_month,
        mgpa.actions_available_end_of_month,
        mgpa.monthly_net_change_of_actions,
        mgpa.days_with_data,
        mgpa.weekend_days,

        -- medical group plan additive measures
        mgpa.monthly_actions_completed,
        mgpa.monthly_actions_became_ineligible,
        mgpv.visit_count_per_month,

        -- medical_group plan static dataset measure (same across all months)
        mgpv.total_patient_count,

        -- medical group plan running total measures
        mgpa.monthly_running_total_actions_completed,
        mgpa.monthly_running_total_actions_became_ineligible,
        mgpv.cumulative_monthly_visits,
        mgpv.visit_target_gap,
        mgpv.remaining_patients_without_visit,

        -- medical group plan monthly rates
        mgpa.monthly_completion_rate AS mgp_monthly_completion_rate, -- aka AAC%
        mgpa.running_monthly_completion_rate AS mgp_running_monthly_completion_rate,
        mgpv.awv_cumulative_monthly_visit_rate AS mgp_cumulative_monthly_awv_visit_rate,
        mgpa.monthly_lost_opportunity_rate AS mgp_monthly_lost_opportunity_rate,
        mgpa.running_monthly_lost_opportunity_rate AS mgp_running_monthly_lost_opportunity_rate,

        -- medical group plan monthly indicators
        mgpv.monthly_awv_performance_status as mgp_cumulative_monthly_awv_performance_status,
        mgpa.lost_opportunity_status as mgp_lost_opportunity_status,
        mgpa.aac_performance_status as mgp_aac_performance_status, -- monthly completion rate indicator -- Target = 0.04

        -- medical group plan month over month measures
        mgpa.monthly_completion_rate_prev_month AS mgp_monthly_completion_rate_prev_month, 
        mgpa.monthly_lost_opportunity_rate_prev_month AS mgp_monthly_lost_opportunity_rate_prev_month,
        mgpa.monthly_completion_rate_change_mom AS mgp_monthly_completion_rate_change_mom,
        mgpa.lost_opportunity_rate_change_mom AS mgp_lost_opportunity_rate_change_mom,

        -- medical group monthly rates (aggregated across plans)
        mga.monthly_completion_rate AS mg_monthly_completion_rate, -- aka AAC%
        mga.running_monthly_completion_rate AS mg_running_monthly_completion_rate,
        mgv.awv_cumulative_monthly_visit_rate AS mg_cumulative_monthly_awv_visit_rate,
        mga.monthly_lost_opportunity_rate AS mg_monthly_lost_opportunity_rate,
        mga.running_monthly_lost_opportunity_rate AS mg_running_monthly_lost_opportunity_rate,

        -- medical group monthly indicators (aggregated across plans)
        mgv.monthly_awv_performance_status as mg_cumulative_monthly_awv_performance_status,
        mga.lost_opportunity_status as mg_lost_opportunity_status,
        mga.aac_performance_status as mg_aac_performance_status, -- monthly completion rate indicator -- Target = 0.04

        -- medical group month over month measures (aggregated across plans)
        mga.monthly_completion_rate_prev_month AS mg_monthly_completion_rate_prev_month, 
        mga.monthly_lost_opportunity_rate_prev_month AS mg_monthly_lost_opportunity_rate_prev_month,
        mga.monthly_completion_rate_change_mom AS mg_monthly_completion_rate_change_mom,
        mga.lost_opportunity_rate_change_mom AS mg_lost_opportunity_rate_change_mom

    FROM int_medical_group_plan_action_summary_monthly mgpa
    left join int_medical_group_plan_awv_summary_monthly mgpv
        on mgpa.date_month = mgpv.date_month
        and mgpa.medical_group_id = mgpv.medical_group_id
        and mgpa.plan_id = mgpv.plan_id
    left join medical_group_dim mg
        on mgpa.medical_group_id = mg.medical_group_id
    left join plan_dim p
        on mgpa.plan_id = p.plan_id
    left join int_medical_group_action_summary_monthly mga
        on mgpa.date_month = mga.date_month
        and mgpa.medical_group_id = mga.medical_group_id
    left join int_medical_group_awv_summary_monthly mgv
        on mgpa.date_month = mgv.date_month
        and mgpa.medical_group_id = mgv.medical_group_id
)

select * from final
