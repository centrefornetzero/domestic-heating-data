{{
    config(
        schema="nsul"
    )
}} 

select
    dataset_date,
    uprn,
    lad21cd as local_authority_district_code

from {{ source("raw_nsul", "nsul") }}
