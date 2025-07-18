-- models/intermediate/int_medical_group_action_decomposition_daily.sql

{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['medical_group_id', 'date_day'], 'unique': True},
            {'columns': ['date_day']},
            {'columns': ['medical_group_id']},
        ]
    )
}}

/*
    Daily Action Decomposition Model
    
    This model tracks the daily flow of actions for each medical_group,
    decomposing the change in actions available into:
    1. Actions completed (positive business outcome)
    2. Actions no longer eligible (lost opportunity)
    
    Business Logic:
    actions_start_of_day - actions_end_of_day = actions_completed + actions_no_longer_eligible
*/

WITH 
import_actions_available_daily AS (
    SELECT 
        date_day,
        medical_group_id,
        count_actions_available
    FROM {{ ref('stg_actions_available') }}
),

import_actions_completed_daily AS (
    SELECT 
        date_day,
        medical_group_id,
        count_actions_attested_as_complete
    FROM {{ ref('stg_actions_attested_as_complete') }}
),

int_agg_actions_available_across_plans_daily AS (
    SELECT
        date_day,
        medical_group_id,
        SUM(count_actions_available) AS count_actions_available
    FROM import_actions_available_daily
    GROUP BY date_day, medical_group_id
),

int_actions_with_lag AS (
    SELECT 
        date_day,
        medical_group_id,
        count_actions_available AS actions_end_of_day,
        LAG(count_actions_available, 1) OVER (
            PARTITION BY medical_group_id
            ORDER BY date_day
        ) AS actions_start_of_day_prev,
        -- For first day of each group, use current count as start
        COALESCE(
            LAG(count_actions_available, 1) OVER (
                PARTITION BY medical_group_id 
                ORDER BY date_day
            ), 
            count_actions_available
        ) AS actions_start_of_day
    FROM int_agg_actions_available_across_plans_daily
)

, int_agg_actions_completed_across_plans_daily AS (
    SELECT
        date_day,
        medical_group_id,
        SUM(count_actions_attested_as_complete) AS count_actions_attested_as_complete
    FROM import_actions_completed_daily
    GROUP BY date_day, medical_group_id
),

daily_decomposition AS (
    SELECT 
        aa.date_day,
        aa.medical_group_id,
        aa.actions_start_of_day,
        aa.actions_end_of_day,
        COALESCE(ac.count_actions_attested_as_complete, 0) AS count_actions_completed_in_day,
        
        -- Calculate actions no longer eligible
        GREATEST(
            aa.actions_start_of_day 
            - aa.actions_end_of_day 
            - COALESCE(ac.count_actions_attested_as_complete, 0),
            0
        ) AS count_actions_became_ineligible_in_day,
        
        -- Calculate daily net change
        aa.actions_end_of_day - aa.actions_start_of_day AS daily_net_change_of_actions
        
    FROM int_actions_with_lag aa
    LEFT JOIN int_agg_actions_completed_across_plans_daily ac
        ON aa.date_day = ac.date_day
        AND aa.medical_group_id = ac.medical_group_id
),

final AS (
    SELECT 
        date_day,
        medical_group_id,
        actions_start_of_day,
        actions_end_of_day,
        daily_net_change_of_actions,
        count_actions_completed_in_day,
        count_actions_became_ineligible_in_day,

        -- Calculate cumulative actions completed
        SUM(count_actions_completed_in_day) OVER (
            PARTITION BY medical_group_id
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_actions_completed,

        -- Calculate cumulative lost opportunities
        sum(count_actions_became_ineligible_in_day) OVER (
            PARTITION BY medical_group_id
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_actions_became_ineligible,

        -- Calculate diff between complete+ineligible and daily net change (net new remaining)
        CASE
            WHEN daily_net_change_of_actions - count_actions_completed_in_day - count_actions_became_ineligible_in_day > 0 
            THEN daily_net_change_of_actions - count_actions_completed_in_day - count_actions_became_ineligible_in_day
            ELSE 0
            END AS daily_net_new_available_actions,

        -- Add data quality flags
        CASE 
            WHEN actions_start_of_day - actions_end_of_day 
                = count_actions_completed_in_day + count_actions_became_ineligible_in_day 
            THEN TRUE
            ELSE FALSE
        END AS decomposition_balance_check,
        
        -- Weekend flag for analysis
        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
        
    FROM daily_decomposition
)

SELECT * FROM final
ORDER BY medical_group_id, date_day