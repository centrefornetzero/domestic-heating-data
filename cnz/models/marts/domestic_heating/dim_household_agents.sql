{{ config(schema="domestic_heating", materialized="table") }}

with households as (

    select * from {{ ref('dim_households') }}

),

final as (

    select
        address_cluster_id as id,
        local_authority_district_name_2020 as location,
        current_property_value_gbp as property_value_gbp,
        total_floor_area_m2,
        is_off_gas_grid,
        construction_year_band,
        property_type,
        built_form,
        heating_system,
        epc_rating,
        potential_epc_rating,
        occupant_type,
        is_solid_wall,
        walls_energy_efficiency,
        roof_energy_efficiency,
        windows_energy_efficiency,
        is_heat_pump_suitable_archetype

    from households

    where
        local_authority_district_name_2020 is not null
        and current_property_value_gbp is not null
        and total_floor_area_m2 is not null
        and construction_year_band is not null
        and property_type is not null
        and built_form is not null
        and wall_type is not null
        and epc_rating is not null
        and potential_epc_rating is not null
        and heating_system is not null
        and occupant_type is not null
        and walls_energy_efficiency is not null
        and windows_energy_efficiency is not null
        -- Exclude records which the ABM can't model
        and (roof_energy_efficiency is not null or has_premises_above = true)

)

select * from final
