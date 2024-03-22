with 

source as (

    select * from {{ source('staging', 'bike_rides_data') }}

),

renamed as (

    select
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        {{ get_part_of_the_day('started_at')}} as started_day_part

    from source

)

select * from renamed
