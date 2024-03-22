{#
    This macro defines and returns the part of the day in which the ride started for further analysis
#}

{% macro get_part_of_the_day(start_time) -%}

    case 
        when extract(hour from {{ start_time }}) between 6 and 11 then 'Morning'
        when extract(hour from {{ start_time }}) between 12 and 17 then 'Afternoon'
        when extract(hour from {{ start_time }}) between 18 and 23 then 'Evening'
        when extract(hour from {{ start_time }}) between 0 and 5 then 'Night'
        else 'Not defined'
    end

{%- endmacro %}