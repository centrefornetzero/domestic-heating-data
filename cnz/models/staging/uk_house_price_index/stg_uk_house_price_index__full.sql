{{ config(schema="uk_house_price_index") }}

with uk_hpi as (

    select * from {{ source("uk_house_price_index", "full") }}

),

cleaned as (

    select
        parse_date("%d/%m/%Y", date) as period,
        regionname as region_name,
        areacode as area_code,
        averageprice as average_price,
        index,
        index12mpercentchange as index_12_m_percent_change,
        averagepriceseasonallyadjusted as average_price_seasonally_adjusted,
        salesvolume as sales_volume,
        detachedprice as detached_price,
        detachedindex as detached_index,
        detached1mpercentchange as detached_1m_percent_change,
        detached12mpercentchange as detached_12m_percent_change,
        semidetachedprice as semi_detached_price,
        semidetachedindex as semi_detached_index,
        semidetached1mpercentchange as semi_detached_1m_percent_change,
        semidetached12mpercentchange as semi_detached_12m_percent_change,
        terracedprice as terraced_price,
        terracedindex as terraced_index,
        terraced1mpercentchange as terraced_1m_percent_change,
        terraced12mpercentchange as terraced_12m_percent_change,
        flatprice as flat_price,
        flatindex as flat_index,
        flat1mpercentchange as flat_1m_percent_change,
        flat12mpercentchange as flat_12m_percent_change,
        cashprice as cash_price,
        cashindex as cash_index,
        cash1mpercentchange as cash_1m_percent_change,
        cash12mpercentchange as cash_12m_percent_change,
        cashsalesvolume as cash_sales_volume,
        mortgageprice as mortgage_price,
        mortgageindex as mortgage_index,
        mortgage1mpercentchange as mortgage_1m_percent_change,
        mortgage12mpercentchange as mortgage_12m_percent_change,
        mortgagesalesvolume as mortgage_sales_volume,
        ftbprice as first_time_buyer_price,
        ftbindex as first_time_buyer_index,
        ftb1mpercentchange as first_time_buyer_1m_percent_change,
        ftb12mpercentchange as first_time_buyer_12m_percent_change,
        fooprice as former_owner_occupier_price,
        fooindex as former_owner_occupier_index,
        foo1mpercentchange as former_owner_occupier_1m_percent_change,
        foo12mpercentchange as former_owner_occupier_12m_percent_change,
        newprice as new_price,
        newindex as new_index,
        new1mpercentchange as new_1m_percent_change,
        new12mpercentchange as new_12m_percent_change,
        newsalesvolume as new_sales_volume,
        oldprice as old_price

    from uk_hpi
)

select * from cleaned
