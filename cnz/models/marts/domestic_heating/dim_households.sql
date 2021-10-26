{{ config(schema="domestic_heating") }}

with certificates as (

    select * from {{ ref('stg_epc__england_wales_certificates') }}

),

latest_building_certificates as (

    select
        * except (most_recent_building_certificate_ordinal)

    from (

        select
            *,
            rank() over (
                partition by building_reference_number order by inspection_date desc
            ) as most_recent_building_certificate_ordinal

        from certificates

    )

    where most_recent_building_certificate_ordinal = 1

),

final as (

    select
        address_line_1,
        address_line_2,
        address_line_3,
        post_town,
        county,
        postcode,
        building_reference_number as epc_building_reference_number,
        property_type,
        built_form,
        mains_gas_available,
        main_fuel,
        secondary_fuel,
        has_boiler,
        has_warm_air,
        has_storage_heaters,
        has_district_heating,
        has_heat_pump,
        heat_pump_source,
        has_radiators,
        has_underfloor_heating,
        has_secondary_room_heaters,
        has_secondary_portable_heaters,
        has_hot_water_from_heating_system,
        has_electric_immersion_heater

    from latest_building_certificates

)

select * from final
