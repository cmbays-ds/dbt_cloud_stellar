-- models/staging/stg_actions_available.sql
{{ config(materialized="table", description="staging table for available actions") }}

with source as (select * from {{ source("raw_data", "raw__AWVs_actions_available") }})

select
    cast(date_day as date) as date_day,
    cast(medical_group_id as integer) as medical_group_id,
    cast(plan_id as integer) as plan_id,
    cast(count_actions_available as integer) as count_actions_available
from source
