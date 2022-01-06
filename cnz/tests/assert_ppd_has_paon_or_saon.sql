select transaction_unique_identifier

from {{ ref("stg_ppd__england_wales_sales") }}

where primary_addressable_object_name is null
    and secondary_addressable_object_name is null
