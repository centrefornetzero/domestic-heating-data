{% macro get_fuel(column_name) -%}
case
    when regexp_contains({{ column_name }}, '\\b(mains gas|nwy prif gyflenwad)')
        then 'gas'
    when contains_substr({{ column_name }}, 'electric')
        or contains_substr({{ column_name }}, 'trydan') then 'electricity'
    when regexp_contains({{ column_name }}, '\\b(bottled gas|lpg)') then 'lpg'
    when regexp_contains({{ column_name }}, '\\b(oil)') then 'oil'
    when
        regexp_contains(
            {{ column_name }}, '\\b(mineral|wood|coal|smokeless|anthracite|solid)'
        ) then 'solid fuel'
    when contains_substr({{ column_name }}, 'biomass') then 'biomass'
end
{%- endmacro %}

