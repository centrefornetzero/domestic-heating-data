{{ config(schema="nsul") }}

with

nsul_addresses as (

    select * from {{ source("nsul", "addresses") }}

),

local_authority_districts as (

    select * from {{ source("nsul", "local_authority_districts") }}

),

final as (

    select * from nsul_addresses
    join local_authority_districts
        on nsul_addresses.lad20cd = local_authority_districts.lad20cd

)

select * from final
