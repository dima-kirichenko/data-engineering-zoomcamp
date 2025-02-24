{{ config(materialized='table') }}

with filtered_trips as (
    select 
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) = 2020 
        and extract(month from pickup_datetime) = 4
        and fare_amount > 0 
        and trip_distance > 0 
        and payment_type in (1,2)  -- 1=Credit card, 2=Cash
),
percentiles as (
    select distinct
        service_type,
        year,
        month,
        count(*) over(partition by service_type) as trip_count,
        round(percentile_cont(fare_amount, 0.97) over(partition by service_type), 1) as p97_fare,
        round(percentile_cont(fare_amount, 0.95) over(partition by service_type), 1) as p95_fare,
        round(percentile_cont(fare_amount, 0.90) over(partition by service_type), 1) as p90_fare
    from filtered_trips
)
select *
from percentiles
order by service_type