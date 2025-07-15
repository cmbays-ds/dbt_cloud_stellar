-- models/staging/stg_actions_attested_as_complete.sql
{{
    config(
        materialized="table",
        description="staging table for actions attested as complete",
    )
}}

with
    source as (select * from {{ source("raw_data", "raw__AWVs_attested_as_complete") }})

select
    cast(date_day as date) as date_day,
    cast(medical_group_id as integer) as medical_group_id,
    cast(plan_id as integer) as plan_id,
    cast(
        count_actions_attested_as_complete as integer
    ) as count_actions_attested_as_complete
from source
