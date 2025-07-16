-- models/marts/kimball/fct_awv_action.sql
{{ config(
    materialized='table',
    description= 'fact table for annual wellness visit actions'
    ) 
}}

with 
import_available_actions as (
    select
        date_day,
        medical_group_id,
        plan_id,
        count_actions_available
    from {{ ref('stg_actions_available') }}
),
import_attested_actions as (
    select
        date_day,
        medical_group_id,
        plan_id,
        count_actions_attested_as_complete
    from {{ ref('stg_actions_attested_as_complete') }}
),
int_joined_actions as (
    select
        coalesce(a.date_day, c.date_day) as date_day,
        coalesce(a.medical_group_id, c.medical_group_id) as medical_group_id,
        coalesce(a.plan_id, c.plan_id) as plan_id,
        a.count_actions_available,
        c.count_actions_attested_as_complete
    from import_available_actions a
    full outer join import_attested_actions c
        on a.date_day = c.date_day
        and a.medical_group_id = c.medical_group_id
        and a.plan_id = c.plan_id
),
final as (
select
    date_day,
    medical_group_id,
    plan_id,
    coalesce(count_actions_available, 0) as count_actions_available,
    coalesce(count_actions_attested_as_complete, 0) as count_actions_attested_as_complete
from int_joined_actions
)

select * from final