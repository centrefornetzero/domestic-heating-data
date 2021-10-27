{% macro get_heating_fuel(heat_description) %}
case
    when regexp_contains({{ heat_description }}, '\\b(mains gas|nwy prif gyflenwad)') then 'gas'
    when contains_substr({{ heat_description }}, 'electric')
        or contains_substr({{ heat_description }}, 'trydan') then 'electricity'
    when regexp_contains({{ heat_description }}, '\\b(bottled gas|lpg)') then 'lpg'
    when regexp_contains({{ heat_description }}, '\\b(oil)') then 'oil'
    when regexp_contains({{ heat_description }}, '\\b(mineral|wood|coal|smokeless|anthracite|solid)') then 'solid fuel'
    when contains_substr({{ heat_description }}, 'biomass') then 'biomass'
end
{% endmacro %}
