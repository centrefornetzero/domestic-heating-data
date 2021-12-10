{% macro convert_efficiency_to_integer(column_name) -%}
case {{ column_name }}
    when 'very good' then 5
    when 'good' then 4
    when 'average' then 3
    when 'poor' then 2
    when 'very poor' then 1
end
{%- endmacro %}
