-- models/marts/kimball/dim_patient.sql
{{ config(materialized="table", description="dimension table for patients") }}

with import_patients as (select distinct patient_id from {{ ref("stg_claims_awv") }})

select 
    patient_id
from import_patients
