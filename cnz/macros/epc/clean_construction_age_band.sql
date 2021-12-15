{% macro clean_construction_age_band(column_name) -%}
coalesce(
    case {{ column_name }}
        when 'england and wales: before 1900' then 'built_pre_1900'
        when 'england and wales: 1900-1929' then 'built_1900_1929'
        when 'england and wales: 1930-1949' then 'built_1930_1949'
        when 'england and wales: 1950-1966' then 'built_1950_1966'
        when 'england and wales: 1967-1975' then 'built_1967_1975'
        when 'england and wales: 1976-1982' then 'built_1976_1982'
        when 'england and wales: 1983-1990' then 'built_1983_1990'
        when 'england and wales: 1991-1995' then 'built_1991_1995'
        when 'england and wales: 1996-2002' then 'built_1996_2002'
        when 'england and wales: 2003-2006' then 'built_2003_2006'
        when 'england and wales: 2007-2011' then 'built_2007_2011'
        when 'england and wales: 2007 onwards' then 'built_2007_onwards'
        when 'england and wales: 2012 onwards' then 'built_2012_onwards'
    end,
    case
        -- deal with outliers first: what is the best way here? Really we want to link it back to the dataset year, not the current year...
        when safe_cast({{ column_name }} as int) > 2021 then null
        -- Convert numerical values to bands
        when safe_cast({{ column_name }} as int) < 1900 then 'built_pre_1900'
        when safe_cast({{ column_name }} as int) between 1900 and 1929 then 'built_1900_1929'
        when safe_cast({{ column_name }} as int) between 1930 and 1949 then 'built_1930_1949'
        when safe_cast({{ column_name }} as int) between 1950 and 1996 then 'built_1950_1966'
        when safe_cast({{ column_name }} as int) between 1967 and 1975 then 'built_1967_1975'
        when safe_cast({{ column_name }} as int) between 1976 and 1982 then 'built_1976_1982'
        when safe_cast({{ column_name }} as int) between 1983 and 1990 then 'built_1983_1990'
        when safe_cast({{ column_name }} as int) between 1991 and 1995 then 'built_1991_1995'
        when safe_cast({{ column_name }} as int) between 1996 and 2002 then 'built_1996_2002'
        when safe_cast({{ column_name }} as int) between 2003 and 2006 then 'built_2003_2006'
        when safe_cast({{ column_name }} as int) between 2007 and 2011 then 'built_2007_2011'
        when safe_cast({{ column_name }} as int) between 2012 and 2099 then 'built_2012_onwards'
    end
) 
{%- endmacro %}
