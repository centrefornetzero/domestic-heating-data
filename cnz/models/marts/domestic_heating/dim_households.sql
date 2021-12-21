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

sales as (

    select * from {{ ref("base_domestic_heating__ppd_matched_sales") }}

),

uk_hpi as (

    select
        period,
        area_code,
        index
    from {{ ref('stg_uk_house_price_index__full') }}

),

most_recent_hpi as (

    select
        period,
        area_code,
        index
    from {{ ref('base_domestic_heating__most_recent_hpi') }}

),

joined as (

    select
        epc_features.* except (postcode),

        off_gas_postcodes.postcode is not null as is_off_gas_grid,

        epc_features.property_type not in (
            'flat', 'park home'
        ) and epc_features.built_form != 'mid_terrace' as is_heat_pump_suitable_archetype,

        nsul.local_authority_district_code as local_authority_district_code_2021,
        nsul.local_authority_district_name as local_authority_district_name_2020,

        sales.price as property_value_gbp,
        sales.date_of_transfer as date_sold

    from epc_features

    left join off_gas_postcodes on epc_features.postcode = off_gas_postcodes.postcode

    left join nsul on nsul.uprn = epc_features.uprn

    left join sales on epc_features.address_cluster_id = sales.address_cluster_id

),

final as (
    select

        joined.*,
        joined.property_value_gbp * (most_recent_hpi.index / uk_hpi.index) as property_value_hpi_adjusted_gbp

    from joined

    left join uk_hpi on joined.local_authority_district_code_2021 = uk_hpi.area_code
        and date_trunc(joined.date_sold, month) = date_trunc(uk_hpi.period, month)

    left join most_recent_hpi on joined.local_authority_district_code_2021 = uk_hpi.area_code

)

select * from final
