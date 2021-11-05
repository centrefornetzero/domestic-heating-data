{{ config(schema="ppd", materialized="table") }}

with

ppd_england_wales_sales as (

    select * from {{ source("ppd", "england_wales_sales") }}

),

cleaned as (

    select
        transaction_unique_identifier,
        parse_date('%Y-%m-%d', substr(date_of_transfer, 0, 11)) as date_of_transfer,
        price,
        nullif(primary_addressable_object_name, '') as primary_addressable_object_name,
        nullif(secondary_addressable_object_name, '') as secondary_addressable_object_name,
        nullif(street, '') as street,
        nullif(locality, '') as locality,
        town_city,
        district,
        county,
        nullif(postcode, '') as postcode,

        case ppd_category_type
            when 'A' then 'standard'
            when 'B' then 'additional'
        end as entry_type,

        case property_type
            when 'D' then 'detatched'
            when 'F' then 'flat/maisonette'
            when 'O' then 'other'
            when 'S' then 'semi-detatched'
            when 'T' then 'terraced'
        end as property_type,

        old_new = 'Y' as is_new_build,

        case duration
            when 'F' then 'freehold'
            when 'L' then 'leasehold'
        end as tenure

    from ppd_england_wales_sales

)

select * from cleaned
