{{ config(schema="epc_ppd_address_matches") }}

with

matches as (

    select * from {{ source("epc_ppd_address_matching", "matches") }}

)

select
    address_id,
    cluster_id

from matches
