-- models/intermediate/int_medical_group_action_summary_monthly.sql

{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['medical_group_id', 'date_month'], 'unique': True},
            {'columns': ['date_month']},
            {'columns': ['medical_group_id']},
        ]
    )
}}

/*
    Monthly Action Summary Model
    
    This model aggregates daily action decomposition data into monthly metrics,
    providing the foundation for AAC% calculations and lost opportunity analysis.
    
    Key Metrics:
    - AAC% = actions_completed_month / actions_available_start_month
    - Lost Opportunity Rate = actions_no_longer_eligible_month / actions_available_start_month
*/

WITH import_daily_action_decomposition AS (
    SELECT 
        date_day,
        medical_group_id,
        actions_start_of_day,
        actions_end_of_day,
        count_actions_completed_in_day as actions_completed,
        daily_net_change_of_actions,
        cumulative_actions_completed,
        count_actions_became_ineligible_in_day,
        cumulative_actions_became_ineligible,
        daily_net_new_available_actions,
        is_weekend
    FROM {{ ref('int_medical_group_action_decomposition_daily') }}
),

int_agg_monthly_actions AS (
    SELECT
        medical_group_id,
        DATE_TRUNC('month', date_day) date_month,
        MAX(actions_start_of_day) AS actions_available_start_month,
        MAX(actions_end_of_day) AS actions_available_end_of_month,
        SUM(actions_completed) AS monthly_actions_completed,
        SUM(count_actions_became_ineligible_in_day) AS monthly_actions_became_ineligible,
        MAX(cumulative_actions_completed) AS monthly_running_total_actions_completed,
        MAX(cumulative_actions_became_ineligible) AS monthly_running_total_actions_became_ineligible,
        
        -- Calculate monthly net change of actions (completions + lost opportunities)
        SUM(daily_net_change_of_actions) as monthly_net_change_of_actions,

        -- Calculate monthly net new actions that became available subtracting completed and ineligible actions
        SUM(daily_net_new_available_actions) as monthly_net_new_available_actions,

        -- Calculate monthly progress of completed actions
        CASE
            WHEN MAX(actions_start_of_day) > 0
            THEN SUM(actions_completed)
                / MAX(actions_start_of_day)
            ELSE 0 END
            AS monthly_completion_rate,

        -- Calculate monthly progress of lost opportunities
        CASE 
            WHEN MAX(actions_start_of_day) > 0
            THEN SUM(count_actions_became_ineligible_in_day)
                / MAX(actions_start_of_day)
            ELSE 0 END
            AS monthly_lost_opportunity_rate,

        -- Count of days with data
        COUNT(*) AS days_with_data,
        
        -- Count of weekdays vs weekends
        SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_days
    FROM import_daily_action_decomposition
    GROUP BY medical_group_id, date_month
),

int_agg_running_available_actions AS (
    SELECT 
        medical_group_id,
        date_month,
        SUM(actions_available_start_month) OVER (
            PARTITION BY medical_group_id
            ORDER BY date_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as monthly_running_total_actions_available
    FROM int_agg_monthly_actions
),

final AS (
    SELECT 
        medical_group_id,
        date_month,
        actions_available_start_month,
        actions_available_end_of_month,
        monthly_actions_completed,
        monthly_actions_became_ineligible,
        monthly_running_total_actions_completed,
        monthly_running_total_actions_became_ineligible,
        monthly_net_change_of_actions,
        monthly_net_new_available_actions,
        monthly_completion_rate,
        monthly_lost_opportunity_rate,
        monthly_running_total_actions_available,

        -- Calculate running total monthly progress of completed actions
        CASE
            WHEN monthly_running_total_actions_available > 0
            THEN monthly_running_total_actions_completed
                / monthly_running_total_actions_available
            ELSE 0 END
            AS running_monthly_completion_rate,

        -- Calculate running total monthly progress of lost opportunities
        CASE
            WHEN monthly_running_total_actions_available > 0
            THEN monthly_running_total_actions_became_ineligible
                / monthly_running_total_actions_available 
            ELSE 0 END
            AS running_monthly_lost_opportunity_rate,

        -- Performance status flags
        CASE 
            WHEN monthly_completion_rate >= 0.04 THEN 'Meeting Target'
            WHEN monthly_completion_rate >= 0.02 THEN 'Below Target'
            ELSE 'Significantly Below Target'
        END AS aac_performance_status,
        
        CASE 
            WHEN monthly_lost_opportunity_rate <= 0.10 THEN 'Low Loss'
            WHEN monthly_lost_opportunity_rate <= 0.20 THEN 'Moderate Loss'
            ELSE 'High Loss'
        END AS lost_opportunity_status,
        
        -- Month-over-month comparison flags
        LAG(monthly_completion_rate, 1) OVER (
            PARTITION BY medical_group_id
            ORDER BY date_month
        ) AS monthly_completion_rate_prev_month,
        
        LAG(monthly_lost_opportunity_rate, 1) OVER (
            PARTITION BY medical_group_id
            ORDER BY date_month
        ) AS monthly_lost_opportunity_rate_prev_month,

        days_with_data,
        weekend_days
        
    FROM int_agg_monthly_actions
JOIN int_agg_running_available_actions
    USING(medical_group_id, date_month)
)

SELECT 
    *,
    -- Calculate month-over-month changes
    monthly_completion_rate - monthly_completion_rate_prev_month AS monthly_completion_rate_change_mom,
    monthly_lost_opportunity_rate - monthly_lost_opportunity_rate_prev_month AS lost_opportunity_rate_change_mom  
FROM final
ORDER BY medical_group_id, date_month