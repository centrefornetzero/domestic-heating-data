{{ config(schema="epc") }}

with

cleaned_england_wales_certificates as (

    select * from {{ ref('base_epc__cleaned_england_wales_certificates') }}

),


most_recently_lodged_certificate_of_inspection as (
    -- some certificates are repeatedly lodged in the register for the same inspection date

    select
        * except(row_number_)

    from (

        select
            *,
            row_number() over (
                partition by building_reference_number, inspection_date
                order by lodged_at desc
            ) as row_number_

        from cleaned_england_wales_certificates

    )

    where row_number_ = 1
),

final as (

    select * from most_recently_lodged_certificate_of_inspection

)

select * from final
