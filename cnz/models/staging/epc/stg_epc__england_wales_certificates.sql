{{ config(schema="epc") }}

select
    lmk_key as lodgement_identifier,
    nullif(ADDRESS1, "") as address_line_1,
    nullif(ADDRESS2, "") as address_line_2,
    nullif(ADDRESS3, "") as address_line_3,
    upper(trim(POSTTOWN)) as post_town,
    postcode,
    building_reference_number,
    nullif(CURRENT_ENERGY_RATING, 'INVALID!') as current_energy_rating,
    nullif(POTENTIAL_ENERGY_RATING, 'INVALID!') as potential_energy_rating,
    current_energy_efficiency,
    potential_energy_efficiency,
    lower(PROPERTY_TYPE) as property_type,
    case
        when BUILT_FORM in ('NO DATA!', '') then null
        else lower(BUILT_FORM)
    end as built_form,
    inspection_date,
    nullif(LOCAL_AUTHORITY, '') as ons_local_authority_code,
    nullif(CONSTITUENCY, '') as ons_constituency_code,
    nullif(COUNTY, '') as county,
    LODGEMENT_DATE as register_lodgement_date,
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
    case ENERGY_TARIFF
        when '' then null
        when 'NO DATA!' then null
        when 'INVALID!' then null
        when 'Unknown' then null
        else lower(ENERGY_TARIFF)
    end as energy_tariff,
    case MAINS_GAS_FLAG
        when 'Y' then true
        when 'N' then false
        else null
    end as mains_gas_available,
    nullif(FLOOR_LEVEL, '') as floor_level,  -- requires *major* cleaning
    case FLAT_TOP_STOREY
        when 'Y' then true
        when 'N' then false
        else null
    end as flat_top_storey,
    flat_storey_count,
    main_heating_controls,  -- values make no sense
    cast(MULTI_GLAZE_PROPORTION as int) as multi_glaze_proportion, -- all values are 0.0, 1.0, ...  98.0.
    case
        when contains_substr(GLAZED_TYPE, 'single') then 'single'
        when contains_substr(GLAZED_TYPE, 'double') then 'double'
        when contains_substr(GLAZED_TYPE, 'triple') then 'triple'
        else null
    end as glazed_type,
    case
        when contains_substr(GLAZED_TYPE, 'during or after 2002') then true
        when contains_substr(GLAZED_TYPE, 'before 2002') then false
        else null
    end as glazed_post_2002,
    case
        when GLAZED_AREA in ('NO DATA!', 'Not Defined', '') then null
        else lower(GLAZED_AREA)
    end as glazed_area,
    cast(EXTENSION_COUNT as int) as extension_count,
    cast(NUMBER_HABITABLE_ROOMS as int) as number_habitable_rooms,
    cast(NUMBER_HEATED_ROOMS as int) as number_heated_rooms,
    case
        when LOW_ENERGY_LIGHTING > 100 or LOW_ENERGY_LIGHTING < 0 then null
        else LOW_ENERGY_LIGHTING
    end as percentage_low_energy_lighting,  -- drop nonsensical percentages
    case
        when NUMBER_OPEN_FIREPLACES < 0 then null
        else NUMBER_OPEN_FIREPLACES
    end as number_open_fireplaces,
    hot_water_description,
    nullif(lower(HOT_WATER_ENERGY_EFF), 'n/a') as hot_water_energy_efficiency,
    nullif(lower(HOT_WATER_ENV_EFF), 'n/a') as hot_water_environmental_efficiency,
    floor_description,
    case FLOOR_ENERGY_EFF
        when 'N/A' then null
        when 'NO DATA!' then null
        else lower(FLOOR_ENERGY_EFF)
    end as floor_energy_efficiency,
    nullif(lower(FLOOR_ENV_EFF), 'n/a') as floor_environmental_efficiency,
    windows_description,
    nullif(lower(WINDOWS_ENERGY_EFF), 'n/a') as windows_energy_efficiency,
    nullif(lower(WINDOWS_ENV_EFF), 'n/a') as windows_environmental_efficiency,
    walls_description,
    nullif(lower(WALLS_ENERGY_EFF), 'n/a') as walls_energy_efficiency,
    nullif(lower(WALLS_ENV_EFF), 'n/a') as walls_environmental_efficiency,
    SECONDHEAT_DESCRIPTION as secondary_heat_description,
    nullif(lower(SHEATING_ENERGY_EFF), 'n/a') as secondary_heating_energy_efficiency,
    nullif(lower(SHEATING_ENV_EFF), 'n/a') as secondary_heating_environmental_efficiency,
    roof_description,
    nullif(lower(ROOF_ENERGY_EFF), 'n/a') as roof_energy_efficiency,
    nullif(lower(ROOF_ENV_EFF), 'n/a') as roof_environmental_efficiency,
    lower(MAINHEAT_DESCRIPTION) as main_heat_description,
    nullif(lower(MAINHEAT_ENERGY_EFF), 'n/a') as main_heat_energy_efficiency,
    nullif(lower(MAINHEAT_ENV_EFF), 'n/a') as main_heat_environmental_efficiency,
    MAINHEATCONT_DESCRIPTION as main_heat_control_description,
    nullif(lower(MAINHEATC_ENERGY_EFF), 'n/a') as main_heat_control_energy_efficiency,
    nullif(lower(MAINHEATC_ENV_EFF), 'n/a') as main_heat_control_environmental_efficiency,
    lighting_description,
    case LIGHTING_ENERGY_EFF
        when 'N/A' then null
        when '' then null
        else lower(LIGHTING_ENERGY_EFF)
    end as lighting_energy_efficiency,
    nullif(lower(LIGHTING_ENV_EFF), 'n/a') as lighting_environmental_efficiency,
    main_fuel,
    case
        when WIND_TURBINE_COUNT < 0 then null
        else cast(WIND_TURBINE_COUNT as int)
    end as wind_turbine_count,
    case HEAT_LOSS_CORRIDOR
        when '' then null
        when 'NO DATA!' then null
        else HEAT_LOSS_CORRIDOR
    end as heat_loss_corridor,
    unheated_corridor_length,
    floor_height,
    case
        when PHOTO_SUPPLY > 0 then PHOTO_SUPPLY
        else null
    end as percentage_roof_photovoltaics,
    case SOLAR_WATER_HEATING_FLAG
        when 'Y' then true
        when 'N' then false
        else null
    end as solar_water_heating,
    case MECHANICAL_VENTILATION
        when 'NO DATA!' then null
        when '' then null
        else MECHANICAL_VENTILATION
    end as mechanical_ventilation,
    construction_age_band,  -- needs cleaning
    lodgement_datetime,
    case
        when tenure in ('NO DATA!', 'unknown', '') then null
        when starts_with(tenure, 'Not defined') then null
        else replace(lower(tenure), 'rental', 'rented')
    end as tenure,
    -- these counts look suspiciously like percentages
    cast(FIXED_LIGHTING_OUTLETS_COUNT as int) as fixed_lighting_outlets_count,
    case
        when LOW_ENERGY_FIXED_LIGHT_COUNT < 0 then null
        else cast(LOW_ENERGY_FIXED_LIGHT_COUNT as int)
    end as low_energy_fixed_light_count

from {{ source("epc", "england_wales_certificates") }}
