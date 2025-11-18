with trips as (
    select * from {{ ref('stg_nyc_taxi_trips') }}
),

hourly_aggregates as (
    select
        extract(hour from pickup_datetime) as pickup_hour,
        count(*) as total_trips,
        avg(trip_distance) as avg_trip_distance,
        avg(fare_amount) as avg_fare_amount,
        avg(total_amount) as avg_total_amount,
        sum(passenger_count) as total_passengers
        
    from trips
    group by 1
)

select * from hourly_aggregates
order by pickup_hour