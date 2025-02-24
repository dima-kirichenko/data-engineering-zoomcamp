{{ config(materialized='table') }}

with quarterly_revenue as (
    select 
        service_type,
        year,
        quarter,
        year_quarter,
        sum(total_amount) as quarterly_revenue,
        count(*) as trip_count
    from {{ ref('fact_trips') }}
    where year in (2019, 2020)  -- Filter for only 2019-2020 data
    group by 1,2,3,4
),
yoy_growth as (
    select 
        curr.service_type,
        curr.year,
        curr.quarter,
        curr.year_quarter,
        curr.quarterly_revenue as curr_revenue,
        curr.trip_count as curr_trip_count,
        prev.quarterly_revenue as prev_revenue,
        prev.trip_count as prev_trip_count,
        round(100.0 * (curr.quarterly_revenue - prev.quarterly_revenue) / prev.quarterly_revenue, 2) as yoy_growth
    from quarterly_revenue curr
    left join quarterly_revenue prev 
        on curr.service_type = prev.service_type 
        and curr.quarter = prev.quarter 
        and curr.year = prev.year + 1
)
select * from yoy_growth
where year = 2020  -- Only show 2020 results since we want YoY growth for 2020
order by service_type, quarter