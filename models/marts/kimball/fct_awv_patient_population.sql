-- models/marts/kimball/fct_awv_patient_population.sql
{{
    config(
        materialized="table",
        description="fact table for annual wellness patient populations by medical group and plan",
    )
}}

-- Patient pop per group/plan (distinct patients in claims)
with
import_patient_claims as (
    select medical_group_id, plan_id, patient_id from {{ ref("stg_claims_awv") }}
),
final as (
    select 
        medical_group_id, plan_id, 
        count(distinct patient_id) as patient_panel_size
    from import_patient_claims
    group by medical_group_id, plan_id
)

select *
from final
