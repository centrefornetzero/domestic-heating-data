{{ config(schema="epc", materialized="ephemeral") }}

with

england_wales_certificates as (

    select * from {{ source("epc", "england_wales_certificates") }}

),

final as (

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
        if(total_floor_area > 0, total_floor_area, null) as total_floor_area_m2,
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
        end as mains_gas_available,
        nullif(floor_level, '') as floor_level,  -- requires *major* cleaning
        case flat_top_storey
            when 'Y' then true
            when 'N' then false
        end as flat_top_storey,
        flat_storey_count,
        main_heating_controls,  -- values make no sense
        cast(multi_glaze_proportion as int) as multi_glaze_proportion,
        case
            when contains_substr(glazed_type, 'single') then 'single'
            when contains_substr(glazed_type, 'double') then 'double'
            when contains_substr(glazed_type, 'triple') then 'triple'
        end as glazed_type,
        case
            when contains_substr(glazed_type, 'during or after 2002') then true
            when contains_substr(glazed_type, 'before 2002') then false
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
        lower(hotwater_description) as hot_water_description,
        nullif(lower(hot_water_energy_eff), 'n/a') as hot_water_energy_efficiency,
        nullif(lower(hot_water_env_eff), 'n/a') as hot_water_environmental_efficiency,
        lower(floor_description) as floor_description,
        case floor_energy_eff
            when 'N/A' then null
            when 'NO DATA!' then null
            else lower(floor_energy_eff)
        end as floor_energy_efficiency,
        nullif(lower(floor_env_eff), 'n/a') as floor_environmental_efficiency,
        lower(windows_description) as windows_description,
        nullif(lower(windows_energy_eff), 'n/a') as windows_energy_efficiency,
        nullif(lower(windows_env_eff), 'n/a') as windows_environmental_efficiency,
        lower(walls_description) as walls_description,
        nullif(lower(walls_energy_eff), 'n/a') as walls_energy_efficiency,
        nullif(lower(walls_env_eff), 'n/a') as walls_environmental_efficiency,
        case secondheat_description
            when '' then null
            when 'None' then null
            when 'Dim' then null  -- dim is Welsh for none
            when 'None|Dim' then null
            else lower(secondheat_description)
        end as secondary_heat_description,
        nullif(lower(sheating_energy_eff), 'n/a') as secondary_heating_energy_efficiency,
        nullif(lower(sheating_env_eff), 'n/a') as secondary_heating_environmental_efficiency,
        lower(roof_description) as roof_description,
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
        {{ get_fuel('main_fuel')|indent(8) }} as main_fuel,
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
        end as percentage_roof_photovoltaics,
        case solar_water_heating_flag
            when 'Y' then true
            when 'N' then false
        end as solar_water_heating,
        case mechanical_ventilation
            when 'NO DATA!' then null
            when '' then null
            else mechanical_ventilation
        end as mechanical_ventilation,
        coalesce(
            case construction_age_band
                when 'NO DATA!' then null
                when 'INVALID!' then null
                when 'Not applicable' then null
                when '' then null
                when 'England and Wales: before 1900' then 'pre-1900'
                when 'England and Wales: 1900-1929' then '1900-1929'
                when 'England and Wales: 1930-1949' then '1930-1949'
                when 'England and Wales: 1950-1966' then '1950-1966'
                when 'England and Wales: 1967-1975' then '1967-1975'
                when 'England and Wales: 1976-1982' then '1976-1982'
                when 'England and Wales: 1983-1990' then '1983-1990'
                when 'England and Wales: 1991-1995' then '1991-1995'
                when 'England and Wales: 1996-2002' then '1996-2002'
                when 'England and Wales: 2003-2006' then '2003-2006'
                when 'England and Wales: 2007-2011' then '2007-2011'
                when 'England and Wales: 2007 onwards' then '2007-onwards'
                when 'England and Wales: 2012 onwards' then '2012-onwards'
            end,
            case
                -- deal with outliers first
                when safe_cast(construction_age_band as int) > (
                    select extract(year from max(inspection_date)) from england_wales_certificates
                ) then null
                -- Convert numerical values to bands
                when safe_cast(construction_age_band as int) < 1900 then 'pre-1900'
                when safe_cast(construction_age_band as int) between 1900 and 1929 then '1900-1929'
                when safe_cast(construction_age_band as int) between 1930 and 1949 then '1930-1949'
                when safe_cast(construction_age_band as int) between 1950 and 1996 then '1950-1966'
                when safe_cast(construction_age_band as int) between 1967 and 1975 then '1967-1975'
                when safe_cast(construction_age_band as int) between 1976 and 1982 then '1976-1982'
                when safe_cast(construction_age_band as int) between 1983 and 1990 then '1983-1990'
                when safe_cast(construction_age_band as int) between 1991 and 1995 then '1991-1995'
                when safe_cast(construction_age_band as int) between 1996 and 2002 then '1996-2002'
                when safe_cast(construction_age_band as int) between 2003 and 2006 then '2003-2006'
                when safe_cast(construction_age_band as int) between 2007 and 2011 then '2007-2011'
                when safe_cast(construction_age_band as int) between 2012 and 2099 then '2012-onwards'
            end
        ) as construction_year_band,
        lodgement_datetime as lodged_at,
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
        end as low_energy_fixed_light_count,
        uprn,
        nullif(lower(uprn_source), '') as uprn_source

    from england_wales_certificates

)

select * from final
