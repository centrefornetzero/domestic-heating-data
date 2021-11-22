{{ config(schema="epc") }}

with

recommendations as (

    select * from {{ source('epc', 'england_wales_recommendations') }}

),

certificates as (

    select * from {{ ref('stg_epc__england_wales_certificates') }}

),

cleaned_recommendations as (

    select
        lmk_key as lodgement_identifier,
        improvement_item as improvement_item_number,
        improvement_id,
        coalesce(
            nullif(improvement_summary_text, ""),
            nullif(improvement_id_text, "")
        ) as improvement_summary,
        nullif(improvement_descr_text, "") as improvement_description,
        nullif(indicative_cost, "") as indicative_cost

    from recommendations

),

final as (
    -- Some certificates are lodged repeatedly in register.
    -- Certificates aready de-duplicated so join here to de-dupe recommendations.
    select cleaned_recommendations.*

    from cleaned_recommendations

    join certificates on cleaned_recommendations.lodgement_identifier
        = certificates.lodgement_identifier

)

select * from final
