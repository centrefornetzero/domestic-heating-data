{{ config(schema="epc") }}

select
    lmk_key as lodgement_identifier,
    nullif(address1, "") as address_line_1,
    nullif(address2, "") as address_line_2,
    nullif(address3, "") as address_line_3,
    upper(trim(posttown)) as post_town,
    postcode,
    building_reference_number,
    nullif(current_energy_rating, 'INVALID!') as current_energy_rating,
    nullif(potential_energy_rating, 'INVALID!') as potential_energy_rating,
    current_energy_efficiency,
    potential_energy_efficiency,
    lower(property_type) as property_type,
    case
        when built_form in ('NO DATA!', '') then null
        else lower(built_form)
    end as built_form,
    inspection_date,
    nullif(local_authority, '') as ons_local_authority_code,
    nullif(constituency, '') as ons_constituency_code,
    nullif(county, '') as county,
    lodgement_date as register_lodgement_date,
    transaction_type,  -- needs clean up and tests,
    environment_impact_current,
    environment_impact_potential,
    energy_consumption_current,
    energy_consumption_potential,
    co2_emissions_current,
    co2_emiss_curr_per_floor_area as co2_emissions_current_per_floor_area,
    co2_emissions_potential,
    lighting_cost_current,
    lighting_cost_potential,
    heating_cost_current,
    heating_cost_potential,
    hot_water_cost_current,
    hot_water_cost_potential,
    total_floor_area,
    case energy_tariff
        when '' then null
        when 'NO DATA!' then null
        when 'INVALID!' then null
        when 'Unknown' then null
        else lower(energy_tariff)
    end as energy_tariff,
    case mains_gas_flag
        when 'Y' then true
        when 'N' then false
        else null
    end as mains_gas_available,
    nullif(floor_level, '') as floor_level,  -- requires *major* cleaning
    case flat_top_storey
        when 'Y' then true
        when 'N' then false
        else null
    end as flat_top_storey,
    flat_storey_count,
    main_heating_controls,  -- values make no sense
    cast(multi_glaze_proportion as int) as multi_glaze_proportion, -- all values are 0.0, 1.0, ...  98.0.
    case
        when contains_substr(glazed_type, 'single') then 'single'
        when contains_substr(glazed_type, 'double') then 'double'
        when contains_substr(glazed_type, 'triple') then 'triple'
        else null
    end as glazed_type,
    case
        when contains_substr(glazed_type, 'during or after 2002') then true
        when contains_substr(glazed_type, 'before 2002') then false
        else null
    end as glazed_post_2002,
    case
        when glazed_area in ('NO DATA!', 'Not Defined', '') then null
        else lower(glazed_area)
    end as glazed_area,
    cast(extension_count as int) as extension_count,
    cast(number_habitable_rooms as int) as number_habitable_rooms,
    cast(number_heated_rooms as int) as number_heated_rooms,
    case
        when low_energy_lighting > 100 or low_energy_lighting < 0 then null
        else low_energy_lighting
    end as percentage_low_energy_lighting,  -- drop nonsensical percentages
    case
        when number_open_fireplaces < 0 then null
        else number_open_fireplaces
    end as number_open_fireplaces,
    hot_water_description,
    nullif(lower(hot_water_energy_eff), 'n/a') as hot_water_energy_efficiency,
    nullif(lower(hot_water_env_eff), 'n/a') as hot_water_environmental_efficiency,
    floor_description,
    case floor_energy_eff
        when 'N/A' then null
        when 'NO DATA!' then null
        else lower(floor_energy_eff)
    end as floor_energy_efficiency,
    nullif(lower(floor_env_eff), 'n/a') as floor_environmental_efficiency,
    windows_description,
    nullif(lower(windows_energy_eff), 'n/a') as windows_energy_efficiency,
    nullif(lower(windows_env_eff), 'n/a') as windows_environmental_efficiency,
    walls_description,
    nullif(lower(walls_energy_eff), 'n/a') as walls_energy_efficiency,
    nullif(lower(walls_env_eff), 'n/a') as walls_environmental_efficiency,
    secondheat_description as secondary_heat_description,
    nullif(lower(sheating_energy_eff), 'n/a') as secondary_heating_energy_efficiency,
    nullif(lower(sheating_env_eff), 'n/a') as secondary_heating_environmental_efficiency,
    roof_description,
    nullif(lower(roof_energy_eff), 'n/a') as roof_energy_efficiency,
    nullif(lower(roof_env_eff), 'n/a') as roof_environmental_efficiency,
    lower(mainheat_description) as main_heat_description,
    nullif(lower(mainheat_energy_eff), 'n/a') as main_heat_energy_efficiency,
    nullif(lower(mainheat_env_eff), 'n/a') as main_heat_environmental_efficiency,
    mainheatcont_description as main_heat_control_description,
    nullif(lower(mainheatc_energy_eff), 'n/a') as main_heat_control_energy_efficiency,
    nullif(lower(mainheatc_env_eff), 'n/a') as main_heat_control_environmental_efficiency,
    lighting_description,
    case lighting_energy_eff
        when 'N/A' then null
        when '' then null
        else lower(lighting_energy_eff)
    end as lighting_energy_efficiency,
    nullif(lower(lighting_env_eff), 'n/a') as lighting_environmental_efficiency,
    main_fuel,
    case
        when wind_turbine_count < 0 then null
        else cast(wind_turbine_count as int)
    end as wind_turbine_count,
    case heat_loss_corridor
        when '' then null
        when 'NO DATA!' then null
        else heat_loss_corridor
    end as heat_loss_corridor,
    unheated_corridor_length,
    floor_height,
    case
        when photo_supply > 0 then photo_supply
        else null
    end as percentage_roof_photovoltaics,
    case solar_water_heating_flag
        when 'Y' then true
        when 'N' then false
        else null
    end as solar_water_heating,
    case mechanical_ventilation
        when 'NO DATA!' then null
        when '' then null
        else mechanical_ventilation
    end as mechanical_ventilation,
    construction_age_band,  -- needs cleaning
    lodgement_datetime,
    case
        when tenure in ('NO DATA!', 'unknown', '') then null
        when starts_with(tenure, 'Not defined') then null
        else replace(lower(tenure), 'rental', 'rented')
    end as tenure,
    -- these counts look suspiciously like percentages
    cast(fixed_lighting_outlets_count as int) as fixed_lighting_outlets_count,
    case
        when low_energy_fixed_light_count < 0 then null
        else cast(low_energy_fixed_light_count as int)
    end as low_energy_fixed_light_count

from {{ source("epc", "england_wales_certificates") }}
