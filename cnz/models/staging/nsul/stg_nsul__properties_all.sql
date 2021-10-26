{{ config(schema="nsul") }}

with

nsul_properties as (

    select * from {{ source("nsul", "nsul") }}

),

final as (

    select * from nsul_properties

)

select * from final
