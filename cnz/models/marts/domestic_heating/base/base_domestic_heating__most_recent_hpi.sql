{{ config(schema="domestic_heating", materialized="ephemeral") }}

with

uk_hpi as (

    select * from {{ ref("stg_uk_house_price_index__full") }}

),

final as (

    select
        * except(row_number_)

    from (

        select
            *,
            row_number() over (
                partition by area_code
                order by period desc
            ) as row_number_

        from uk_hpi

    )

    where row_number_ = 1

)

select * from final
