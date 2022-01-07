with agents as (

    select * from {{ ref('dim_household_agents') }}

),

households as (

    select * from {{ ref('dim_households') }}

),

final as (

    select agents.id from agents

    join households on agents.id = households.address_cluster_id

    where agents.roof_energy_efficiency is null
        and not households.has_premises_above

)

select * from final
