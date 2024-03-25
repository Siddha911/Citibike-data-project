{{
    config(
        materialized='table'
    )
}}

with citibike_rides as (
    select * 
    from {{ ref('stg_bike_rides_data') }}
),

stations as (
    select *
    from {{ ref('dim_stations') }}
)

select 
    citibike_rides.ride_id,			
    citibike_rides.rideable_type,	
    citibike_rides.member_casual,				
    citibike_rides.started_at,			
    citibike_rides.ended_at,
    timestamp_diff(citibike_rides.ended_at, citibike_rides.started_at, minute) as duration,
    {{ get_part_of_the_day('started_at') }} as day_part_start,	
    citibike_rides.start_station_name,			
    start_station.station_id_int as start_station_id,			
    citibike_rides.end_station_name,				
    end_station.station_id_int as end_station_id,
    citibike_rides.start_lat,		
    citibike_rides.start_lng,			
    citibike_rides.end_lat,				
    citibike_rides.end_lng
from citibike_rides
inner join stations as start_station
on citibike_rides.start_station_name = start_station.name
inner join stations as end_station
on citibike_rides.end_station_name = end_station.name