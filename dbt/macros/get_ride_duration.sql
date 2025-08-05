{#
    This macro returns duration time of the ride 
#}

{% macro get_ride_duration(started, ended) -%}

    timestamp_diff(ended, started, minute)

{%- endmacro %}
