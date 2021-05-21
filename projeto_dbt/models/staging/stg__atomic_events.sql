with source as (

    select * from {{ source('data_lake_raw', 'atomic_events') }}

)

select
    event_id::varchar,
    event_timestamp::timestamp,
    event_type::varchar,
    browser_language::varchar,
    browser_name::varchar,
    browser_user_agent::varchar,
    click_id::varchar,
    device_is_mobile::boolean,
    device_type::varchar,
    geo_country::varchar,
    geo_latitude::float,
    geo_longitude::float,
    geo_region_name::varchar,
    geo_timezone::varchar,
    ip_address::varchar,
    os::varchar,
    os_name::varchar,
    os_timezone::varchar,
    page_url::varchar,
    page_url_path::varchar,
    referer_medium::varchar,
    referer_url::varchar,
    referer_url_port::varchar,
    referer_url_scheme::varchar,
    user_custom_id::varchar,
    user_domain_id::varchar as cookie_id,
    utm_campaign::varchar,
    utm_content::varchar,
    utm_medium::varchar,
    utm_source::varchar,
	landing_date::date
from source
