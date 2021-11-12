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

        -- energy efficiency measures
        case
            when regexp_contains(lower(windows_description), r'single|sengl') then 'single'
            when regexp_contains(lower(windows_description), r'double|secondary|multiple|high performance|dwbl|lluosog|perfformiad') then 'double'
            when regexp_contains(lower(windows_description), r'triple|triphlyg') then 'triple'
            else null
        end as glazed_type_category,

       -- building attributes
        case
            when contains_substr(lower(roof_description), 'above') then false
            when roof_description != '' then true
            else null
        end as has_loft,

        case
            when contains_substr(lower(walls_description), 'cavity') then true
            when walls_description = '' then null
            else false
        end as has_cavity_wall

    from most_recently_lodged_certificate_of_inspection

)

select * from final
