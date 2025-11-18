with trips as (
    select * from {{ ref('stg_nyc_taxi_trips') }}
),

payment_analysis as (
    select
        payment_type,
        count(*) as total_trips,
        sum(total_amount) as total_revenue,
        avg(fare_amount) as avg_fare,
        avg(tip_amount) as avg_tip,
        avg(trip_distance) as avg_distance,
        round(avg(tip_amount) / nullif(avg(fare_amount), 0) * 100, 2) as avg_tip_percentage
        
    from trips
    group by 1
)

select * from payment_analysis
order by total_trips desc