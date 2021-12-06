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
            row_number() over (
                partition by building_reference_number order by inspection_date desc
            ) as most_recent_building_certificate_ordinal

        from certificates

    )

    where most_recent_building_certificate_ordinal = 1

),

off_gas_postcodes as (
    select postcode from {{ ref('stg_xoserve__off_gas_postcodes') }}
),

epc_features as (

    select
        -- Address and ID information
        building_reference_number as epc_building_reference_number,
        address_line_1,
        address_line_2,
        address_line_3,
        post_town,
        county,
        postcode,

        -- Property features
        case
            when total_floor_area <= 0 then null else total_floor_area
        end as floor_area_sqm,

        null as construction_age_band, --todo: ingest this column into src_epc --todo: remapping values

        case
            when property_type in ('flat', 'maisonette') then 'flat'
            else property_type
        end as property_type,

        case
            when contains_substr(built_form, 'mid-terrace') then 'mid_terrace'
            when built_form = 'enclosed end-terrace' then 'end_terrace'
            else replace(built_form, '-', '_')
        end as built_form,

        case
            when contains_substr(walls_description, 'cavity') then 'cavity'
            when regexp_contains(walls_description, 'solid|timber|granite|stone') then 'solid'
        end as wall_type,

        current_energy_rating as epc_rating,
        potential_energy_rating as potential_epc_rating,

        -- Heating system modelling
        case
            when heat_pump_source = 'air' then 'air_source_heat_pump'
            when heat_pump_source = 'ground' then 'ground_source_heat_pump'
            when main_fuel = 'gas' or main_hotwater_fuel = 'gas' then 'gas_boiler'
            when main_fuel = 'electricity' or main_hotwater_fuel = 'electricity' then 'electric_boiler'
            -- treat all oil/lpg/solid fuel/biomass houses as 'oil boiler' houses in our modelling
            when main_fuel is not null or main_hotwater_fuel is not null then 'oil_boiler'
        end as heating_system,

        -- Energy efficiencies
        {{ convert_efficiency_to_integer('walls_energy_efficiency')|indent(8) }} as walls_energy_efficiency,
        {{ convert_efficiency_to_integer('windows_energy_efficiency')|indent(8) }} as windows_energy_efficiency,
        case
            when has_premises_above then -1 -- Roof energy efficiency is "not applicable" in this case
            else {{ convert_efficiency_to_integer('roof_energy_efficiency')|indent(8) }}
        end as roof_energy_efficiency,

        -- Occupant features
        case
            when tenure = 'owner-occupied' then 'owner_occupied'
            when tenure = 'rented (private)' then 'rented_private'
            when tenure = 'rented (social)' then 'rented_social'
        end as occupant_type


    from latest_building_certificates

),

final as (
    select
        null as local_authority_district_name_2020, -- todo: join on postcode <> lad20cd <> lad20nm using nsul
        epc_features.*,
        null as property_value_gbp, --todo: join on ppd data
        epc_features.property_type not in ("flat", "park home", "mid_terrace") as is_heat_pump_suitable_archetype,
        off_gas_postcodes.postcode is not null as off_gas_grid

    from epc_features
    left join off_gas_postcodes on epc_features.postcode = off_gas_postcodes.postcode
)

select * from final
