with trips as (
    select * from {{ ref('stg_nyc_taxi_trips') }}
),

daily_aggregates as (
    select
        cast(pickup_datetime as date) as trip_date,
        count(*) as total_trips,
        sum(total_amount) as total_revenue,
        avg(trip_distance) as avg_trip_distance,
        avg(fare_amount) as avg_fare_amount,
        avg(tip_amount) as avg_tip_amount,
        sum(passenger_count) as total_passengers,
        min(trip_distance) as min_trip_distance,
        max(trip_distance) as max_trip_distance
        
    from trips
    group by 1
)

select * from daily_aggregates
order by trip_date desc