{{
    config(
        materialized='table'
    )
}}

with source as (

    select * from {{ ref('stg__atomic_events') }}

),

get_previous_timestamp as (

    select
        *,
        LAG(event_timestamp) OVER (PARTITION BY cookie_id ORDER BY event_timestamp) as  previous_timestamp
    from source

),

flag_new_session as (

    select
        *,
        CASE WHEN date_diff('minute', previous_timestamp, event_timestamp) >= 30 THEN 1 ELSE 0 END as new_session
    from get_previous_timestamp

),

get_session_idx as (

    select
        *,
        SUM(new_session) OVER (PARTITION BY cookie_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_idx
    from flag_new_session

),

create_session_id as (

select
    *,
    {{ dbt_utils.surrogate_key(['cookie_id', 'session_idx']) }} as session_id
from get_session_idx

),

calculate_conversion as (

    select
        session_id,
        cookie_id,
        session_idx,
        FIRST_VALUE(event_timestamp) OVER (PARTITION BY session_id ORDER BY event_timestamp rows between unbounded preceding and unbounded following) as session_start_at,
        LAST_VALUE(event_timestamp) OVER (PARTITION BY session_id ORDER BY event_timestamp rows between unbounded preceding and unbounded following) as session_end_at,
        page_url_path = '/confirmation' as is_conversion
    from create_session_id

)

select * from calculate_conversion