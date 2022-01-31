{{ config(schema="nsul") }}

with

nsul_addresses as (

    select * from {{ source("nsul", "addresses") }}

),

local_authority_districts as (

    select * from {{ source("nsul", "local_authority_districts") }}

),

final as (

    select
        nsul_addresses.uprn,
        nsul_addresses.postcode,
        nsul_addresses.lad21cd as local_authority_district_code,
        local_authority_districts.local_authority_district_name

    from nsul_addresses
    left join local_authority_districts
        on nsul_addresses.lad21cd = local_authority_districts.lad21cd

)

select * from final
