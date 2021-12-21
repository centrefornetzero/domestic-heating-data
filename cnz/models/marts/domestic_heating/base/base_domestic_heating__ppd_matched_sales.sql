{{ config(schema="domestic_heating", materialized="ephemeral") }}

with

sales as (

    select * from {{ ref("stg_ppd__england_wales_sales") }}

),


epc_ppd_address_matches as (

    select * from {{ ref("stg_epc_ppd_address_matching__matches") }}

),

matched_sales as (

    select
        epc_ppd_address_matches.cluster_id as address_cluster_id,
        sales.*

    from sales
    join epc_ppd_address_matches
        on sales.address_matching_id = epc_ppd_address_matches.address_id

),

final as (

    select
        * except(row_number_)

    from (

        select
            *,
            row_number() over (
                partition by address_cluster_id
                order by date_of_transfer desc
            ) as row_number_

        from matched_sales

    )

    where row_number_ = 1

)

select * from final
