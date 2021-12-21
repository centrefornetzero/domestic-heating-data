{{ config(schema="domestic_heating", materialized="ephemeral") }}

with

uk_hpi as (

    select * from {{ ref("stg_uk_house_price_index__full") }}

),

final as (

    select
        period,
        area_code,
        first_value(index) over (partition by area_code order by period desc) / index as price_factor

    from uk_hpi

)

select * from final
