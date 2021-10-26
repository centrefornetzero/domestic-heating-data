{{ config(schema="nsul") }}

with

nsul_properties_all as (

    select * from {{ ref("stg_nsul__properties_all") }}

),

nsul_properties_all_with_rank_by_dataset_date as (

    select
        rank() over (order by dataset_date desc) as rank_,
        *

    from nsul_properties_all

),

final as (

    select
        * except(rank_)

    from nsul_properties_all_with_rank_by_dataset_date

    where rank_ = 1

)

select * from final
