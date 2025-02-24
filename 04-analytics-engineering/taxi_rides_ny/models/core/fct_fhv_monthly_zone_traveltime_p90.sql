{{ config(materialized='table') }}

with fhv_trips as (
    select 
        pickup_locationid,
        pickup_zone,
        dropoff_locationid,
        dropoff_zone,
        year,
        month,
        timestamp_diff(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ ref('dim_fhv_trips') }}
),
trip_stats as (
    select distinct
        year,
        month,
        pickup_locationid,
        pickup_zone,
        dropoff_locationid,
        dropoff_zone,
        percentile_cont(trip_duration, 0.9) over (
            partition by year, month, pickup_locationid, dropoff_locationid
        ) as p90_duration
    from fhv_trips
    where year = 2019 
        and month = 11 
        and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
        and trip_duration > 0
),
ranked_zones as (
    select 
        *,
        row_number() over (partition by pickup_zone order by p90_duration desc) as duration_rank
    from trip_stats
)
select 
    pickup_zone,
    dropoff_zone,
    round(p90_duration, 0) as p90_duration_seconds
from ranked_zones
where duration_rank = 2  -- Get the second longest duration for each pickup zone
order by pickup_zone