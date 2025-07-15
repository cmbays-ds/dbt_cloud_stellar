-- models/staging/stg_claims_awv.sql
{{
    config(
        materialized="table",
        description="staging table for annual wellness visit claims",
    )
}}

with source as (select * from {{ source("raw_data", "raw__AWV_as_seen_in_claims") }})

select
    cast(medical_group_id as integer) as medical_group_id,
    cast(patient_id as varchar) as patient_id,
    cast(plan_id as integer) as plan_id,
    cast(awv_date_of_service as date) as awv_date_of_service -- if null then patient has not had AWV in claims yet
from source
