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

        md5(
            to_json_string(
                struct(building_reference_number, address_line_1, address_line_2, address_line_3, post_town, postcode)
            )
        ) as address_matching_id,

        -- main heating
        contains_substr(main_heat_description, 'boiler') as has_boiler,
        contains_substr(main_heat_description, 'warm air') as has_warm_air,
        contains_substr(main_heat_description, 'storage heaters') as has_storage_heaters,
        contains_substr(main_heat_description, 'room heaters')
        or contains_substr(main_heat_description, 'gwresogyddion ystafell') as has_room_heaters,
        contains_substr(main_heat_description, 'community scheme') as has_district_heating,
        contains_substr(main_heat_description, 'heat pump') as has_heat_pump,
        regexp_extract(main_heat_description, "(\\w+) source heat pump") as heat_pump_source,
        contains_substr(main_heat_description, 'radiators') as has_radiators,
        contains_substr(main_heat_description, 'underfloor') as has_underfloor_heating,
        {{ get_fuel('main_heat_description')|indent(8) }} as main_heat_fuel,
        {{ get_fuel('hot_water_description')|indent(8) }} as main_hotwater_fuel,

        -- secondary heating
        case
            when secondary_heat_description is null then false
            else contains_substr(secondary_heat_description, 'room heaters')
                or contains_substr(secondary_heat_description, 'gwresogyddion ystafell')
        end as has_secondary_room_heaters,

        case
            when secondary_heat_description is null then false
            else contains_substr(secondary_heat_description, 'portable heaters')
                or contains_substr(secondary_heat_description, 'cludadwy')
        end as has_secondary_portable_heaters,

        {{ get_fuel('secondary_heat_description')|indent(8) }} as secondary_fuel,

        -- hot water
        contains_substr(hot_water_description, 'from main system') as has_hot_water_from_heating_system,
        contains_substr(hot_water_description, 'electric immersion') as has_electric_immersion_heater,

        -- property features
        -- matches 'other' and 'another'
        contains_substr(roof_description, 'other dwelling above')
        or contains_substr(roof_description, 'other premises above')
        or contains_substr(roof_description, 'arall uwchben')  -- other above
        as has_premises_above

    from most_recently_lodged_certificate_of_inspection

)

select * from final
