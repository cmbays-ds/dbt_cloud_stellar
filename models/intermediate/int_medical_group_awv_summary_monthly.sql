-- models/intermediate/int_medical_group_awv_summary_monthly.sql
{{
    config(
        materialized='table',
    )
}}

/*
    AWV Annual Summary Model
    
    This model calculates Annual Wellness Visit (AWV) rates at the 
    medical group level by month.
    
    Business Logic:
    Cumulative Monthly AWV Rate = Cumulative patients with AWV visits each month / Total patient population
    Target: 70% Cumulative Monthly AWV rate
*/

WITH 
import_agg_patients AS (
    SELECT 
        medical_group_id,
        COUNT(*) total_patient_count
    FROM {{ ref('fct_awv_claim') }}
    GROUP BY medical_group_id
),

import_awv_visits AS (
    SELECT 
        medical_group_id,
        patient_id,
        had_visit,
        date_trunc('month', date_day) as date_month
    FROM {{ ref('fct_awv_claim') }}
    WHERE date_day IS NOT NULL
),

int_aggregate_visits AS (
    SELECT 
        medical_group_id,
        date_month,
        sum(had_visit) as visit_count_per_month
    FROM import_awv_visits
    GROUP BY medical_group_id, date_month
),

int_joined_visits_and_patients AS (
    SELECT 
        a.medical_group_id,
        b.date_month,
        b.visit_count_per_month,
        sum(b.visit_count_per_month) OVER (
            PARTITION BY a.medical_group_id
            ORDER BY b.date_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_monthly_visits,
        total_patient_count
    FROM import_agg_patients a
    JOIN int_aggregate_visits b
        ON a.medical_group_id=b.medical_group_id
),
        
final AS (
    SELECT
        medical_group_id,
        date_month,
        visit_count_per_month,
        cumulative_monthly_visits,
        total_patient_count,

        -- Calculate Monthly AWV Rate as a running total in the numerator
        CASE 
            WHEN total_patient_count > 0 THEN cumulative_monthly_visits / CAST(total_patient_count AS FLOAT)
            ELSE 0
        END AS awv_cumulative_monthly_visit_rate,

        -- Calculate AWV Status
        CASE 
            WHEN total_patient_count <= 0 THEN 'Missing Data'
            WHEN cumulative_monthly_visits / CAST(total_patient_count AS FLOAT) >= 0.7 THEN 'Good'
            WHEN cumulative_monthly_visits / CAST(total_patient_count AS FLOAT) < 0.7 THEN 'Needs Improvement'
            ELSE 'Missing Data'
        END AS monthly_awv_performance_status,

        -- Calculate gap of visits to hit target
        GREATEST(0, CEIL(total_patient_count * 0.70) - cumulative_monthly_visits) AS visit_target_gap,

        -- Calculate remaining patients without visits
        GREATEST(0, total_patient_count - cumulative_monthly_visits) AS remaining_patients_without_visit

    FROM int_joined_visits_and_patients
)

SELECT * FROM final