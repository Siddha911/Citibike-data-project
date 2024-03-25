{{ config(materialized="table") }}

select 
    station_id_int,
    name
from {{ ref('citibike_stations') }}