-- models/intermediate/int_medical_group_awv_summary_annual.sql
{{
    config(
        materialized='table',
    )
}}

/*
    AWV Annual Summary Model
    
    This model calculates Annual Wellness Visit claims visits at the 
    medical group level by year.
    
    Business Logic:
    AWV Rate = Number of unique patients with AWV visits / Total patient population
    Target: 70% annual AWV rate
*/

WITH 
import_awv_claims AS (
    SELECT
        medical_group_id,
        patient_id,
        had_visit,
        date_trunc('year', 
            CASE
                WHEN date_day IS NULL THEN DATE('2024-12-31') -- coercing nulls to static date, assuming all data is only for 2024
                ELSE date_day
            END) AS date_year
    FROM {{ ref('fct_awv_claim') }} a
),

int_aggregate_claims AS (
    SELECT 
        medical_group_id,
        date_year,
        count(patient_id) as patient_count,
        sum(had_visit) as visit_count
    FROM import_awv_claims
    GROUP BY medical_group_id, date_year
),

final AS (
    SELECT 
        medical_group_id,
        date_year,
        visit_count,
        patient_count,
        
        -- Calculate AWV Rate
        CASE 
            WHEN patient_count > 0 THEN CAST(visit_count AS FLOAT) / CAST(patient_count AS FLOAT)
            ELSE 0
        END AS awv_rate,

        -- Calculate AWV Status
        CASE 
            WHEN patient_count <= 0 THEN 'Missing Data'
            WHEN visit_count / CAST(patient_count AS FLOAT) >= 0.7 THEN 'Good'
            WHEN visit_count / CAST(patient_count AS FLOAT) < 0.7 THEN 'Needs Improvement'
            ELSE 'Missing Data'
        END AS awv_performance_status,

        -- Calculate gap of visits to hit target
        GREATEST(0, CEIL(patient_count * 0.70) - visit_count) AS visit_target_gap,

        -- Calculate remaining patients without visits
        GREATEST(0, patient_count - visit_count) AS remaining_patients_without_visit

    FROM int_aggregate_claims
)

SELECT * FROM final