{{ config(schema="domestic_heating", materialized="table") }}

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

sales as (

    select * from {{ ref("base_domestic_heating__ppd_matched_sales") }}

),

hpi as (

    select * from {{ ref("base_domestic_heating__hpi") }}

),


final as (

    select
        epc_features.* except (postcode),

        epc_features.wall_type = 'solid' as is_solid_wall,

        off_gas_postcodes.postcode is not null as is_off_gas_grid,

        epc_features.property_type not in (
            'flat', 'park home'
        ) and epc_features.built_form != 'mid_terrace' as is_heat_pump_suitable_archetype,

        nsul.local_authority_district_code as local_authority_district_code_2021,
        nsul.local_authority_district_name as local_authority_district_name_2020,

        sales.price * hpi.price_factor as current_property_value_gbp

    from epc_features

    left join off_gas_postcodes on epc_features.postcode = off_gas_postcodes.postcode

    left join nsul on nsul.uprn = epc_features.uprn

    left join sales on epc_features.address_cluster_id = sales.address_cluster_id

    left join hpi on nsul.local_authority_district_code = hpi.area_code
        and date_trunc(sales.date_of_transfer, month) = date_trunc(hpi.period, month)

)

select * from final
