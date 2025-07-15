-- models/marts/kimball/dim_medical_group.sql
{{ config(materialized="table", description="dimension table for medical groups") }}

with
    import_medical_groups as (
        select distinct medical_group_id, medical_group_name
        from {{ ref("stg_plan_medical_group") }}
    )

select 
    medical_group_id, 
    medical_group_name
from import_medical_groups
