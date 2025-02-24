## Module 4 Homework

For this homework, you will need the following datasets:
* [Green Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)
* [Yellow Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow)
* [For Hire Vehicle dataset (2019)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)

### Before you start

1. Make sure you, **at least**, have them in GCS with a External Table **OR** a Native Table - use whichever method you prefer to accomplish that (Workflow Orchestration with [pandas-gbq](https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple), [dlt for gcs](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem), [dlt for BigQuery](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery), [gsutil](https://cloud.google.com/storage/docs/gsutil), etc)
2. You should have exactly `7,778,101` records in your Green Taxi table
3. You should have exactly `109,047,518` records in your Yellow Taxi table
4. You should have exactly `43,244,696` records in your FHV table
5. Build the staging models for green/yellow as shown in [here](../../../04-analytics-engineering/taxi_rides_ny/models/staging/)
6. Build the dimension/fact for taxi_trips joining with `dim_zones`  as shown in [here](../../../04-analytics-engineering/taxi_rides_ny/models/core/fact_trips.sql)

**Note**: If you don't have access to GCP, you can spin up a local Postgres instance and ingest the datasets above

### Solution

* [load_taxi_data.py](./load_taxi_data.py)
* [gcs_to_bigquery.py](./gcs_to_bigquery.py)


### Question 1: Understanding dbt model resolution

Provided you've got the following sources.yaml
```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

with the following env variables setup where `dbt` runs:
```shell
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

What does this .sql model compile to?
```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

- [ ] `select * from dtc_zoomcamp_2025.raw_nyc_tripdata.ext_green_taxi`
- [ ] `select * from dtc_zoomcamp_2025.my_nyc_tripdata.ext_green_taxi`
- [x] `select * from myproject.raw_nyc_tripdata.ext_green_taxi`
- [ ] `select * from myproject.my_nyc_tripdata.ext_green_taxi`
- [ ] `select * from dtc_zoomcamp_2025.raw_nyc_tripdata.green_taxi`


### Question 2: dbt Variables & Dynamic Models

Say you have to modify the following dbt_model (`fct_recent_taxi_trips.sql`) to enable Analytics Engineers to dynamically control the date range. 

- In development, you want to process only **the last 7 days of trips**
- In production, you need to process **the last 30 days** for analytics

```sql
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
```

What would you change to accomplish that in a such way that command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?

- [ ] Add `ORDER BY pickup_datetime DESC` and `LIMIT {{ var("days_back", 30) }}`
- [ ] Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", 30) }}' DAY`
- [ ] Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", "30") }}' DAY`
- [x] Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`
- [ ] Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY`


### Question 3: dbt Data Lineage and Execution

Considering the data lineage below **and** that taxi_zone_lookup is the **only** materialization build (from a .csv seed file):

![image](./homework_q2.png)

Select the option that does **NOT** apply for materializing `fct_taxi_monthly_zone_revenue`:

- [ ] `dbt run`
- [ ] `dbt run --select +models/core/dim_taxi_trips.sql+ --target prod`
- [ ] `dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql`
- [ ] `dbt run --select +models/core/`
- [x] `dbt run --select models/staging/+`


### Question 4: dbt Macros and Jinja

Consider you're dealing with sensitive data (e.g.: [PII](https://en.wikipedia.org/wiki/Personal_data)), that is **only available to your team and very selected few individuals**, in the `raw layer` of your DWH (e.g: a specific BigQuery dataset or PostgreSQL schema), 

 - Among other things, you decide to obfuscate/masquerade that data through your staging models, and make it available in a different schema (a `staging layer`) for other Data/Analytics Engineers to explore

- And **optionally**, yet  another layer (`service layer`), where you'll build your dimension (`dim_`) and fact (`fct_`) tables (assuming the [Star Schema dimensional modeling](https://www.databricks.com/glossary/star-schema)) for Dashboarding and for Tech Product Owners/Managers

You decide to make a macro to wrap a logic around it:

```sql
{% macro resolve_schema_for(model_type) -%}

    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET'  -%}
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

    {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}
    {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}
    {%- endif -%}

{%- endmacro %}
```

And use on your staging, dim_ and fact_ models as:
```sql
{{ config(
    schema=resolve_schema_for('core'), 
) }}
```

That all being said, regarding macro above, **select all statements that are true to the models using it**:
- [x] Setting a value for  `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile
- [ ] Setting a value for `DBT_BIGQUERY_STAGING_DATASET` env var is mandatory, or it'll fail to compile
- [x] When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- [x] When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- [x] When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`


## Serious SQL

Alright, in module 1, you had a SQL refresher, so now let's build on top of that with some serious SQL.

These are not meant to be easy - but they'll boost your SQL and Analytics skills to the next level.  
So, without any further do, let's get started...

You might want to add some new dimensions `year` (e.g.: 2019, 2020), `quarter` (1, 2, 3, 4), `year_quarter` (e.g.: `2019/Q1`, `2019-Q2`), and `month` (e.g.: 1, 2, ..., 12), **extracted from pickup_datetime**, to your `fct_taxi_trips` OR `dim_taxi_trips.sql` models to facilitate filtering your queries


### Question 5: Taxi Quarterly Revenue Growth

1. Create a new model `fct_taxi_trips_quarterly_revenue.sql`
2. Compute the Quarterly Revenues for each year for based on `total_amount`
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
  * e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
  * e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow

- [ ] green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- [ ] green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q3, worst: 2020/Q4}
- [ ] green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- [x] green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
- [ ] green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q3, worst: 2020/Q4}

### Solution

* [fct_taxi_trips_quarterly_revenue.sql](../../../04-analytics-engineering/taxi_rides_ny/models/core/fct_taxi_trips_quarterly_revenue.sql)

```json
[{
  "service_type": "Green",
  "year": "2020",
  "quarter": "1",
  "year_quarter": "2020/Q1",
  "curr_revenue": "11480845.79",
  "curr_trip_count": "771967",
  "prev_revenue": "26440852.61",
  "prev_trip_count": "1604976",
  "yoy_growth": "-56.58"
}, {
  "service_type": "Green",
  "year": "2020",
  "quarter": "2",
  "year_quarter": "2020/Q2",
  "curr_revenue": "1544036.31",
  "curr_trip_count": "92030",
  "prev_revenue": "21498354",
  "prev_trip_count": "1353154",
  "yoy_growth": "-92.82"
}, {
  "service_type": "Green",
  "year": "2020",
  "quarter": "3",
  "year_quarter": "2020/Q3",
  "curr_revenue": "2360835.79",
  "curr_trip_count": "135061",
  "prev_revenue": "17651033.84",
  "prev_trip_count": "1125486",
  "yoy_growth": "-86.62"
}, {
  "service_type": "Green",
  "year": "2020",
  "quarter": "4",
  "year_quarter": "2020/Q4",
  "curr_revenue": "2441470.26",
  "curr_trip_count": "143369",
  "prev_revenue": "15680616.87",
  "prev_trip_count": "1028597",
  "yoy_growth": "-84.43"
}, {
  "service_type": "Yellow",
  "year": "2020",
  "quarter": "1",
  "year_quarter": "2020/Q1",
  "curr_revenue": "144118740.68",
  "curr_trip_count": "7919762",
  "prev_revenue": "182726465.78",
  "prev_trip_count": "10489552",
  "yoy_growth": "-21.13"
}, {
  "service_type": "Yellow",
  "year": "2020",
  "quarter": "2",
  "year_quarter": "2020/Q2",
  "curr_revenue": "15560725.84",
  "curr_trip_count": "935075",
  "prev_revenue": "200295029.75",
  "prev_trip_count": "10411584",
  "yoy_growth": "-92.23"
}, {
  "service_type": "Yellow",
  "year": "2020",
  "quarter": "3",
  "year_quarter": "2020/Q3",
  "curr_revenue": "41404401.54",
  "curr_trip_count": "2476467",
  "prev_revenue": "186983365.59",
  "prev_trip_count": "9680791",
  "yoy_growth": "-77.86"
}, {
  "service_type": "Yellow",
  "year": "2020",
  "quarter": "4",
  "year_quarter": "2020/Q4",
  "curr_revenue": "56283852.03",
  "curr_trip_count": "3437267",
  "prev_revenue": "191426503.99",
  "prev_trip_count": "9997848",
  "yoy_growth": "-70.6"
}]
```


### Question 6: P97/P95/P90 Taxi Monthly Fare

1. Create a new model `fct_taxi_trips_monthly_fare_p95.sql`
2. Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit Card')`)
3. Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month

Now, what are the values of `p97`, `p95`, `p90` for Green Taxi and Yellow Taxi, in April 2020?

- [ ] green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- [x] green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- [ ] green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- [ ] green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- [ ] green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 25.5, p90: 19.0}

### Solution

* [fct_taxi_trips_monthly_fare_p95.sql](../../../04-analytics-engineering/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql)

```json
[{
  "service_type": "Green",
  "year": "2020",
  "month": "4",
  "trip_count": "22354",
  "p97_fare": "55.0",
  "p95_fare": "45.0",
  "p90_fare": "26.5"
}, {
  "service_type": "Yellow",
  "year": "2020",
  "month": "4",
  "trip_count": "199342",
  "p97_fare": "31.5",
  "p95_fare": "25.5",
  "p90_fare": "19.0"
}]
```


### Question 7: Top #Nth longest P90 travel time Location for FHV

Prerequisites:
* Create a staging model for FHV Data (2019), and **DO NOT** add a deduplication step, just filter out the entries where `where dispatching_base_num is not null`
* Create a core model for FHV Data (`dim_fhv_trips.sql`) joining with `dim_zones`. Similar to what has been done [here](../../../04-analytics-engineering/taxi_rides_ny/models/core/fact_trips.sql)
* Add some new dimensions `year` (e.g.: 2019) and `month` (e.g.: 1, 2, ..., 12), based on `pickup_datetime`, to the core model to facilitate filtering for your queries

Now...
1. Create a new model `fct_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in `dim_fhv_trips.sql`, compute the [timestamp_diff](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff) in seconds between dropoff_datetime and pickup_datetime - we'll call it `trip_duration` for this exercise
3. Compute the **continous** `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id

For the Trips that **respectively** started from `Newark Airport`, `SoHo`, and `Yorkville East`, in November 2019, what are **dropoff_zones** with the 2nd longest p90 trip_duration ?

- [x] LaGuardia Airport, Chinatown, Garment District
- [ ] LaGuardia Airport, Park Slope, Clinton East
- [ ] LaGuardia Airport, Saint Albans, Howard Beach
- [ ] LaGuardia Airport, Rosedale, Bath Beach
- [ ] LaGuardia Airport, Yorkville East, Greenpoint

### Solution

* [dim_fhv_trips.sql](../../../04-analytics-engineering/taxi_rides_ny/models/core/dim_fhv_trips.sql)
* [stg_fhv_tripdata.sql](../../../04-analytics-engineering/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql)
* [fct_fhv_monthly_zone_traveltime_p90.sql](../../../04-analytics-engineering/taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql)

```json
[{
  "pickup_zone": "Newark Airport",
  "dropoff_zone": "LaGuardia Airport",
  "p90_duration_seconds": "7029.0"
}, {
  "pickup_zone": "SoHo",
  "dropoff_zone": "Chinatown",
  "p90_duration_seconds": "19496.0"
}, {
  "pickup_zone": "Yorkville East",
  "dropoff_zone": "Garment District",
  "p90_duration_seconds": "13846.0"
}]
```


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw4


## Solution 

* To be published after deadline
