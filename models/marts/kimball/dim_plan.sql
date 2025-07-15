-- models/marts/kimball/dim_plan.sql
{{ config(materialized="table", description="dimension table for plans") }}

with
    import_plans as (
        select distinct plan_id, plan_name from {{ ref("stg_plan_medical_group") }}
    )

select 
    plan_id, 
    plan_name
from import_plans
