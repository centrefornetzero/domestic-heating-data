{{ config(schema="epc", materialized="table") }}

with

cleaned_england_wales_certificates as (

    select * from {{ ref('base_epc__cleaned_england_wales_certificates') }}

),

most_recently_lodged_certificate_of_inspection as (
    -- some certificates are repeatedly lodged in the register for the same inspection date

    select
        * except(row_number_)

    from (

        select
            *,
            row_number() over (
                partition by building_reference_number, inspection_date
                order by lodged_at desc
            ) as row_number_

        from cleaned_england_wales_certificates

    )

    where row_number_ = 1
),

final as (

    select
        *,

        -- main heating
        case
            when regexp_contains(main_heat_description, '\\b(mains gas|nwy prif gyflenwad)') then 'gas'
            when contains_substr(main_heat_description, 'electric') then 'electricity'
            when regexp_contains(main_heat_description, '\\b(bottled gas|lpg)') then 'lpg'
            when regexp_contains(main_heat_description, '\\b(oil)') then 'oil'
            when regexp_contains(main_heat_description, '\\b(mineral|wood|coal|smokeless|anthracite|solid)') then 'solid fuel'
            when contains_substr(main_heat_description, 'biomass') then 'biomass'
        end as main_heating_fuel,

        contains_substr(main_heat_description, 'boiler') as has_boiler,
        contains_substr(main_heat_description, 'warm air') as has_warm_air
        contains_substr(main_heat_description, 'storage heaters') as has_storage_heaters,
        contains_substr(main_heat_description, 'room heaters') as has_room_heaters
        contains_substr(main_heat_description, 'community scheme') as has_district_heating,
        contains_substr(main_heat_description, 'heat pump') as has_heat_pump,
        regexp_extract(main_heat_description, "(\\w+) source heat pump") as heat_pump_source
        contains_substr(main_heat_description, 'radiators') as has_radiators,
        contains_substr(main_heat_description, 'underfloor') as has_underfloor_heating,

        -- TODO: secondary_heat_description
        -- TODO: hot_water_description

    from most_recently_lodged_certificate_of_inspection

)

select * from final
