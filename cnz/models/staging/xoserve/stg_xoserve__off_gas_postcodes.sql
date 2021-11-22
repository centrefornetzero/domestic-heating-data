{{ config(schema="xoserve") }}

with

off_gas_postcodes as (

    select * from {{ source("xoserve", "off_gas_postcodes") }}

),

cleaned as (

    select postcode

    from off_gas_postcodes

    where postcode != 'Postcode'    -- CSV header
        and postcode != 'GIR 0AA'   -- Obsolete Girobank postcode

)

select * from cleaned
