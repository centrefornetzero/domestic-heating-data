{{ config(schema="epc_ppd_address_matching") }}

with

ppd as (

    select * from {{ ref('stg_ppd__england_wales_sales') }}

),

addresses as (

    select distinct
        primary_addressable_object_name,
        secondary_addressable_object_name,
        street,
        town_city,
        postcode

    from ppd
),

final as (

    select
        md5(to_json_string(addresses)) as ppd_id,
        *

    from addresses

)

select * from final
