-- models/marts/kimball/fct_awv_claim.sql
{{ config(
    materialized='table',
    description= 'fact table for annual wellness visit claims'
    ) 
}}

with
import_claims as ( 
    select
        awv_date_of_service,
        medical_group_id,
        plan_id,
        patient_id
    from {{ ref('stg_claims_awv') }}
),
final as (
    select
        awv_date_of_service as date_day,
        medical_group_id,
        plan_id,
        patient_id,
        1 as awv_visit -- flag for agg
    from import_claims
)

select * from final
