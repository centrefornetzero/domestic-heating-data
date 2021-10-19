{{ config(schema="nsul", materialized="table") }}

select
    * except(rank_)

from (
    select
        rank() over (order by dataset_date desc) as rank_,
        *

    from {{ ref("nsul_all" ) }}
)

where rank_ = 1
