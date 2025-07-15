-- models/staging/stg_plan_medical_group.sql
{{ config(materialized="table", description="staging table for plans and medical groups") }}

with source as (select * from {{ source("raw_data", "raw__AWVs_plan_medical_group") }})

select
    cast(medical_group_id as integer) as medical_group_id,
    cast(plan_id as integer) as plan_id,
    trim(medical_group_name) as medical_group_name,
    trim(plan_name) as plan_name
from source
