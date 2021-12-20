select
    primary_addressable_object_name,
    secondary_addressable_object_name,
    street,
    town_city,
    postcode

from {{ ref('stg_ppd__england_wales_sales') }}

group by 1, 2, 3, 4, 5

having count(distinct address_matching_id) > 1
