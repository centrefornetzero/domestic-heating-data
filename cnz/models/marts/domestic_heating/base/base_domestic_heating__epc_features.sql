{{ config(schema="domestic_heating", materialized="ephemeral") }}

with

epc_pdd_address_matches as (

    select * from {{ ref('stg_epc_ppd_address_matching__matches') }}

),

certificates as (

    select * from {{ ref('stg_epc__england_wales_certificates') }}

),

latest_building_certificates as (

    select
        * except (most_recent_building_certificate_ordinal)

    from (

        select
            *,
            row_number() over (
                partition by uprn order by inspection_date desc
            ) as most_recent_building_certificate_ordinal

        from certificates
        where uprn is not null

    )

    where most_recent_building_certificate_ordinal = 1

),

final as (

    select
        uprn,
        postcode,
        epc_pdd_address_matches.cluster_id as address_cluster_id,

        -- Property features
        total_floor_area_m2,
        case
            when construction_age_band in ('2007-onwards', '2012-onwards', '2007-2011') then 'built_2007_onwards'
            else concat('built_', replace(construction_age_band, '-', '_'))
        end as construction_age_band,
        has_premises_above,

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
            -- Prioritise main_fuel column first
            when main_fuel = 'gas' then 'gas_boiler'
            when main_fuel = 'electricity' then 'electric_boiler'
            when main_fuel is not null then 'oil_boiler'
            -- Then main_heat_fuel second
            when main_heat_fuel = 'gas' then 'gas_boiler'
            when main_heat_fuel = 'electricity' then 'electric_boiler'
            when main_heat_fuel is not null then 'oil_boiler'
            -- and finally main_hotwater_fuel
            when main_hotwater_fuel = 'gas' then 'gas_boiler'
            when main_hotwater_fuel = 'electricity' then 'electric_boiler'
            when main_hotwater_fuel is not null then 'oil_boiler'
        end as heating_system,

        -- Energy efficiencies
        {{ convert_efficiency_to_integer('walls_energy_efficiency')|indent(8) }} as walls_energy_efficiency,
        {{ convert_efficiency_to_integer('windows_energy_efficiency')|indent(8) }} as windows_energy_efficiency,
        case
            when has_premises_above then null
            else {{ convert_efficiency_to_integer('roof_energy_efficiency')|indent(8) }}
        end as roof_energy_efficiency,

        case tenure
            when 'owner-occupied' then 'owner_occupied'
            when 'rented (private)' then 'rented_private'
            when 'rented (social)' then 'rented_social'
        end as occupant_type


    from latest_building_certificates

    left join epc_pdd_address_matches
        on latest_building_certificates.address_matching_id = epc_pdd_address_matches.address_id
)

select * from final
