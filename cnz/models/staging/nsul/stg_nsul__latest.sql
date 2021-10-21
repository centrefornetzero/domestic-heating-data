{{ config(schema="nsul") }}

select
    * except(rank_)

from (
    select
        rank() over (order by dataset_date desc) as rank_,
        *

    from {{ ref("stg_nsul__all" ) }}
)

where rank_ = 1
