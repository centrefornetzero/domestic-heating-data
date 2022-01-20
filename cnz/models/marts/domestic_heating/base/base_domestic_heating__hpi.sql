{{ config(schema="domestic_heating", materialized="ephemeral") }}

with

uk_hpi as (

    select * from {{ ref("stg_uk_house_price_index__full") }}

),

final as (

    select
        period,
        area_code,
        first_value(index) over area / index as price_factor,
        first_value(detached_index) over area / detached_index as detached_price_factor,
        first_value(semi_detached_index) over area / semi_detached_index as semi_detached_price_factor,
        first_value(terraced_index) over area / terraced_index as terraced_price_factor,
        first_value(flat_index) over area / flat_index as flat_price_factor

    from uk_hpi

    window area as (partition by area_code order by period desc)

)

select * from final
