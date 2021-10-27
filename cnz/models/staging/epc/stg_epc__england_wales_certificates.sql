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
        {{ get_heating_fuel('main_heat_description') }} as main_heating_fuel,
        contains_substr(main_heat_description, 'boiler') as has_boiler,
        contains_substr(main_heat_description, 'warm air') as has_warm_air,
        contains_substr(main_heat_description, 'storage heaters') as has_storage_heaters,
        contains_substr(main_heat_description, 'room heaters') or contains_substr(main_heat_description, 'gwresogyddion ystafell') as has_room_heaters,
        contains_substr(main_heat_description, 'community scheme') as has_district_heating,
        contains_substr(main_heat_description, 'heat pump') as has_heat_pump,
        regexp_extract(main_heat_description, "(\\w+) source heat pump") as heat_pump_source,
        contains_substr(main_heat_description, 'radiators') as has_radiators,
        contains_substr(main_heat_description, 'underfloor') as has_underfloor_heating,

        -- secondary heating
        {{ get_heating_fuel('secondary_heat_description') }} as secondary_heating_fuel,
        contains_substr(secondary_heat_description, 'room heaters') or contains_substr(secondary_heat_description, 'gwresogyddion ystafell') as has_secondary_room_heaters,
        contains_substr(secondary_heat_description, 'portable heaters') or contains_substr(secondary_heat_description, 'cludadwy') con as has_secondary_portable_heaters,

        -- hot water
        contains_substr(hot_water_description, 'from main system') as has_hot_water_from_heating_system,
        contains_substr(hot_water_description, 'electric immersion') as has_electric_immersion_heater

    from most_recently_lodged_certificate_of_inspection

)

select * from final
