{{ config(schema="epc_ppd_address_matching") }}

with

epc as (

    select * from {{ ref('stg_epc__england_wales_certificates') }}

),

addresses as (
    -- Building reference numbers are supposed to unique identify a property.
    -- Unfortunately, some properties have been assigned multiple BRNs.

    select distinct
        building_reference_number,
        address_line_1,
        address_line_2,
        address_line_3,
        post_town,
        postcode

    from epc

),

final as (

    select
        md5(to_json_string(addresses)) as epc_id,
        *

    from addresses

)

select * from final
