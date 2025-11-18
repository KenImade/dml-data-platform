with source as (
    select * from raw.nyc_taxi_trips
),

cleaned as (
    select
        -- Generate unique trip ID
        row_number() over (order by tpep_pickup_datetime) as trip_id,
        
        -- Timestamps
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        
        -- Trip details
        passenger_count,
        trip_distance,
        
        -- Locations
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        
        -- Fares
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        
        -- Payment
        payment_type
        
    from source
    where 
        -- Filter out invalid records
        tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and passenger_count > 0
        and trip_distance > 0
        and fare_amount > 0
)

select * from cleaned