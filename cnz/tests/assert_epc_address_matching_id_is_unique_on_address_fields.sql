select
    -- Building reference numbers are supposed to unique identify a property.
    -- Unfortunately, some properties have been assigned multiple BRNs.
    building_reference_number,
    address_line_1,
    address_line_2,
    address_line_3,
    post_town,
    postcode

from {{ ref('stg_epc__england_wales_certificates') }}

group by 1, 2, 3, 4, 5, 6

having count(distinct address_matching_id) > 1
