{{ config(schema="domestic_heating") }}

with

off_gas_postcodes as (

    select postcode from {{ ref('stg_xoserve__off_gas_postcodes') }}

),

nsul as (

    select
        uprn,
        local_authority_district_code,
        local_authority_district_name
    from {{ ref('stg_nsul__addresses') }}

),

epc_features as (

    select * from {{ ref('base_domestic_heating__epc_features') }}

),

final as (

    select
        nsul.local_authority_district_code as local_authority_district_code_2021,
        nsul.local_authority_district_name as local_authority_district_name_2020,
        epc_features.* except (postcode),
        null as property_value_gbp,
        epc_features.property_type not in (
            'flat', 'park home'
        ) and epc_features.built_form != 'mid_terrace' as is_heat_pump_suitable_archetype,
        off_gas_postcodes.postcode is not null as is_off_gas_grid

    from epc_features
    left join off_gas_postcodes on epc_features.postcode = off_gas_postcodes.postcode
    join nsul on nsul.uprn = epc_features.uprn

)

select * from final
