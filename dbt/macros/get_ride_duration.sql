{#
    This macro returns duration time ofthe ride 
#}

{% macro get_ride_duration(started, ended) -%}

    timestamp_diff(ended, started, minute)

{%- endmacro %}