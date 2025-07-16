-- models/marts/kimball/dim_medical_group_plan.sql
{{ config(materialized="table", description="dimension table for medical groups") }}

with
    import_medical_group_plans as (select * from {{ ref("stg_plan_medical_group") }}
    )

select distinct
    medical_group_id, 
    plan_id,
    medical_group_name,
    plan_name
from import_medical_group_plans
