# Predicting NYC Taxi Cab Rides

# Forecasting time series on a map

In this lab, we aim to show you how to predict the number of trips in the coming hours in each area of New York. To accomplish this, you will ingest the raw data and then aggregate it by hour and region. For simplicity, you will use [Discrete Global Grid H3](https://www.uber.com/en-DE/blog/h3/). The result will be an hourly time series, each representing the count of trips originating from distinct areas. Before running prediction and visualizing results, you will enrich data with third-party signals, such as information about holidays and offline sports events.

In this lab you will learn how to:

  * Work with geospatial data
  * Enrich data with new features
  * Predict time-series of complex structure

This approach is not unique to trip forecasting but is equally applicable in various scenarios where predictive analysis is required. Examples include forecasting scooter or bike pickups, food delivery orders, sales across multiple retail outlets, or predicting the volume of cash withdrawflows across an ATM network. Such models are invaluable for planning and optimization across various industries and services.

## Setup your Account

## Set Your Account Permission

Switch your current role from the default SYSADMIN to ACCOUNTADMIN . This increase in permissions will allow you to create shared databases from Snowflake Marketplace listings.

<img src="images/image_001.png" alt="alt text" width="45%">

## Create a Virtual Warehouse

You will need to create a Virtual Warehouse to run queries.

```sql
use role accountadmin;

CREATE WAREHOUSE IF NOT EXISTS MY_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_RESUME = true
    AUTO_SUSPEND = 60
    ENABLE_QUERY_ACCELERATION = false
    WAREHOUSE_TYPE = 'STANDARD'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD';

USE WAREHOUSE my_wh;
```
Create a new database and schema where you will store datasets in the GEOMETRY data type.

```sql
CREATE or Replace Database advanced_analytics;
-- Set the working database schema
USE advanced_analytics.public;
ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = 'WKT';
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
```

## Step 1. Data acquisition

> **⚠️ IMPORTANT UPDATE:** The original CARTO Academy Marketplace listing is no longer available. Follow the instructions below to load the NYC Taxi data from alternative sources.

The New York Taxi and Limousine Commission (TLC) has provided detailed, anonymized customer travel data since 2009. Painted yellow cars can pick up passengers in any of the city's five boroughs. Raw data on yellow taxi rides can be found on the [TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). This data is divided into files by month. Each file contains detailed trip information, you can read about it [here](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

### Alternative Data Loading Process

Since the CARTO Academy Marketplace listing is unavailable, we'll load the NYC Taxi data directly from parquet files. The modern TLC data uses Location IDs instead of lat/long coordinates, so we'll convert them using NYC taxi zone centroids.

**What you'll need (provided separately):**
- `taxi_zone_centroids.csv` - Lookup table for converting location IDs to coordinates
- Access to NYC TLC parquet files for 2014-2015 (24 files, ~4GB total)

### Step 1a: Create Database Structure

First, create the CARTO_ACADEMY database to match the original lab structure:

```sql
-- Create CARTO_ACADEMY database and schema
CREATE DATABASE IF NOT EXISTS CARTO_ACADEMY;
CREATE SCHEMA IF NOT EXISTS CARTO_ACADEMY.CARTO;
```

### Step 1b: Upload Data Files

Create stages for the data files:

```sql
-- Create stage for taxi zone centroids
CREATE OR REPLACE STAGE ADVANCED_ANALYTICS.PUBLIC.taxi_zone_stage;

-- Create stage for taxi parquet files
CREATE OR REPLACE STAGE CARTO_ACADEMY.CARTO.taxi_data_stage;
```

**Upload files using SnowSQL:**

```bash
# Upload taxi zone centroids CSV
snow stage put file:///<path_to>/taxi_zone_centroids.csv \
  @ADVANCED_ANALYTICS.PUBLIC.taxi_zone_stage

# Upload all 24 taxi parquet files (2014-2015)
snow stage put file:///<path_to>/nyc_taxi/*.parquet \
  @CARTO_ACADEMY.CARTO.taxi_data_stage --parallel 4
```

**Or use Snowsight Web UI:**
1. Navigate to Data > Databases > ADVANCED_ANALYTICS > PUBLIC > Stages > taxi_zone_stage
2. Click "+ Files" and upload `taxi_zone_centroids.csv`
3. Navigate to CARTO_ACADEMY > CARTO > Stages > taxi_data_stage
4. Click "+ Files" and upload all 24 parquet files

Verify uploads:
```sql
LIST @ADVANCED_ANALYTICS.PUBLIC.taxi_zone_stage;
LIST @CARTO_ACADEMY.CARTO.taxi_data_stage;
```

### Step 1c: Load Taxi Zone Centroids

Create and populate the zone centroid lookup table:

```sql
USE ADVANCED_ANALYTICS.PUBLIC;

-- Create centroid lookup table
CREATE OR REPLACE TABLE taxi_zone_centroids (
    LocationID INT,
    borough VARCHAR,
    zone VARCHAR,
    centroid_lon FLOAT,
    centroid_lat FLOAT
);

-- Load from stage
COPY INTO taxi_zone_centroids
FROM @taxi_zone_stage/taxi_zone_centroids.csv
FILE_FORMAT = (
    TYPE = 'CSV' 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
);

-- Verify (should show 263 zones)
SELECT COUNT(*) AS zone_count FROM taxi_zone_centroids;
```

### Step 1d: Load 2014 Taxi Data

Load 2014 data and convert location IDs to coordinates:

```sql
-- Create parquet file format
CREATE OR REPLACE FILE FORMAT parquet_format TYPE = 'PARQUET';

-- Load 2014 data with coordinate conversion
CREATE OR REPLACE TABLE CARTO_ACADEMY.CARTO.TLC_YELLOW_TRIPS_2014 AS
WITH raw_trips AS (
    SELECT 
        $1:VendorID::INT AS VendorID,
        $1:tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime,
        $1:tpep_dropoff_datetime::TIMESTAMP AS tpep_dropoff_datetime,
        $1:passenger_count::INT AS passenger_count,
        $1:trip_distance::FLOAT AS trip_distance,
        $1:RatecodeID::INT AS RatecodeID,
        $1:store_and_fwd_flag::STRING AS store_and_fwd_flag,
        $1:PULocationID::INT AS PULocationID,
        $1:DOLocationID::INT AS DOLocationID,
        $1:payment_type::INT AS payment_type,
        $1:fare_amount::FLOAT AS fare_amount,
        $1:extra::FLOAT AS extra,
        $1:mta_tax::FLOAT AS mta_tax,
        $1:tip_amount::FLOAT AS tip_amount,
        $1:tolls_amount::FLOAT AS tolls_amount,
        $1:improvement_surcharge::FLOAT AS improvement_surcharge,
        $1:total_amount::FLOAT AS total_amount
    FROM @CARTO_ACADEMY.CARTO.taxi_data_stage 
        (FILE_FORMAT => parquet_format, PATTERN => '.*2014.*\\.parquet')
)
SELECT 
    t.VendorID,
    t.tpep_pickup_datetime AS PICKUP_DATETIME,
    t.tpep_dropoff_datetime AS DROPOFF_DATETIME,
    t.passenger_count,
    t.trip_distance,
    t.RatecodeID,
    t.store_and_fwd_flag,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    pu.centroid_lon AS PICKUP_LONGITUDE,
    pu.centroid_lat AS PICKUP_LATITUDE,
    do.centroid_lon AS DROPOFF_LONGITUDE,
    do.centroid_lat AS DROPOFF_LATITUDE
FROM raw_trips t
LEFT JOIN ADVANCED_ANALYTICS.PUBLIC.taxi_zone_centroids pu 
    ON t.PULocationID = pu.LocationID
LEFT JOIN ADVANCED_ANALYTICS.PUBLIC.taxi_zone_centroids do 
    ON t.DOLocationID = do.LocationID
WHERE pu.centroid_lon IS NOT NULL 
  AND do.centroid_lon IS NOT NULL
  AND t.PULocationID IS NOT NULL
  AND t.DOLocationID IS NOT NULL;
```

> **Note:** This query may take 10-15 minutes to complete on a LARGE warehouse.

### Step 1e: Load 2015 Taxi Data

Load 2015 data with the same coordinate conversion:

```sql
CREATE OR REPLACE TABLE CARTO_ACADEMY.CARTO.TLC_YELLOW_TRIPS_2015 AS
WITH raw_trips AS (
    SELECT 
        $1:VendorID::INT AS VendorID,
        $1:tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime,
        $1:tpep_dropoff_datetime::TIMESTAMP AS tpep_dropoff_datetime,
        $1:passenger_count::INT AS passenger_count,
        $1:trip_distance::FLOAT AS trip_distance,
        $1:RatecodeID::INT AS RatecodeID,
        $1:store_and_fwd_flag::STRING AS store_and_fwd_flag,
        $1:PULocationID::INT AS PULocationID,
        $1:DOLocationID::INT AS DOLocationID,
        $1:payment_type::INT AS payment_type,
        $1:fare_amount::FLOAT AS fare_amount,
        $1:extra::FLOAT AS extra,
        $1:mta_tax::FLOAT AS mta_tax,
        $1:tip_amount::FLOAT AS tip_amount,
        $1:tolls_amount::FLOAT AS tolls_amount,
        $1:improvement_surcharge::FLOAT AS improvement_surcharge,
        $1:total_amount::FLOAT AS total_amount
    FROM @CARTO_ACADEMY.CARTO.taxi_data_stage 
        (FILE_FORMAT => parquet_format, PATTERN => '.*2015.*\\.parquet')
)
SELECT 
    t.VendorID,
    t.tpep_pickup_datetime AS PICKUP_DATETIME,
    t.tpep_dropoff_datetime AS DROPOFF_DATETIME,
    t.passenger_count,
    t.trip_distance,
    t.RatecodeID,
    t.store_and_fwd_flag,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    pu.centroid_lon AS PICKUP_LONGITUDE,
    pu.centroid_lat AS PICKUP_LATITUDE,
    do.centroid_lon AS DROPOFF_LONGITUDE,
    do.centroid_lat AS DROPOFF_LATITUDE
FROM raw_trips t
LEFT JOIN ADVANCED_ANALYTICS.PUBLIC.taxi_zone_centroids pu 
    ON t.PULocationID = pu.LocationID
LEFT JOIN ADVANCED_ANALYTICS.PUBLIC.taxi_zone_centroids do 
    ON t.DOLocationID = do.LocationID
WHERE pu.centroid_lon IS NOT NULL 
  AND do.centroid_lon IS NOT NULL
  AND t.PULocationID IS NOT NULL
  AND t.DOLocationID IS NOT NULL;
```

### Step 1f: Verify Data

Verify the data loaded correctly:

```sql
-- Check row counts (expect ~165M for 2014, ~145M for 2015)
SELECT 
    'TLC_YELLOW_TRIPS_2014' AS table_name, 
    COUNT(*) AS row_count, 
    MIN(PICKUP_DATETIME) AS min_date, 
    MAX(PICKUP_DATETIME) AS max_date
FROM CARTO_ACADEMY.CARTO.TLC_YELLOW_TRIPS_2014
UNION ALL
SELECT 
    'TLC_YELLOW_TRIPS_2015' AS table_name, 
    COUNT(*) AS row_count,
    MIN(PICKUP_DATETIME) AS min_date, 
    MAX(PICKUP_DATETIME) AS max_date
FROM CARTO_ACADEMY.CARTO.TLC_YELLOW_TRIPS_2015;

-- Verify coordinates are in NYC bounds (~-73.9 lon, ~40.7 lat)
SELECT 
    AVG(PICKUP_LONGITUDE) AS avg_pickup_lon,
    AVG(PICKUP_LATITUDE) AS avg_pickup_lat,
    MIN(PICKUP_LONGITUDE) AS min_lon,
    MAX(PICKUP_LONGITUDE) AS max_lon,
    MIN(PICKUP_LATITUDE) AS min_lat,
    MAX(PICKUP_LATITUDE) AS max_lat
FROM CARTO_ACADEMY.CARTO.TLC_YELLOW_TRIPS_2014;
```

### PredictHQ Events Data

Another dataset you will use is events data. Check if it's available on Snowflake Marketplace:

  * Navigate to Marketplace in Snowsight
  * Search for `PredictHQ Quickstart Demo`
  * If available, install it and name the database `PredictHQ_Demo`

<img src="images/image_004.png" alt="alt text" width="45%">

> **Note:** If PredictHQ is unavailable, you can skip Step 3 (Data Enrichment). The model will still work, just without event-based features.

---

**✅ Data acquisition complete!** The tables `TLC_YELLOW_TRIPS_2014` and `TLC_YELLOW_TRIPS_2015` now have the same structure as the original CARTO data. Continue with Step 2 below.

---

## Step 2. Data transformation

In this step, you'll divide New York into uniformly sized regions and assign each taxi pick-up location to one of these regions. We aim to get a table with the number of taxi trips per hour for each region.

To achieve this division, you will use the Discrete Global Grid H3. H3 organizes the world into a grid of equal-sized hexagonal cells, with each cell identified by a unique code (either a string or an integer). This hierarchical grid system allows cells to be combined into larger cells or subdivided into smaller ones, facilitating efficient geospatial data processing.

H3 offers 16 different resolutions for dividing areas into hexagons, ranging from resolution 0, where the world is segmented into 122 large hexagons, to resolution 15. At this resolution, each hexagon is less than a square meter, covering the world with approximately 600 trillion hexagons. You can read more about resolutions [here](https://h3geo.org/docs/core-library/restable/) . For our task, we will use resolution 8, where the size of each hexagon is about 0.7 sq. km (0.3 sq. miles).

As a source of the trips data you will use `TLC_YELLOW_TRIPS_2014` and `TLC_YELLOW_TRIPS_2015` tables from the CARTO listing. We are interested in the following fields:

  * Pickup Time
  * Dropoff Time
  * Distance
  * Pickup Location
  * Dropoff Location
  * Total Amount

First, specify the default Database, Schema and the Warehouse:

```sql
USE advanced_analytics.public;
USE WAREHOUSE my_wh;
```

Since CARTO's TABLE s contain raw data you might want to clean it before storing. In the following query you will do a few data cleaning steps:

  * Remove rows that are outside of latitude/longitude allowed values
  * Keep only trips with a duration longer than one minute and distances more than 10 meters.

Since you are interested in trip data for 2014 and 2015 you need to union `TLC_YELLOW_TRIPS_2014` and `TLC_YELLOW_TRIPS_2015` tables. On average, the execution time on the LARGE warehouse is under 4 minutes.

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides AS
SELECT
    -- Convert timestamps from UTC to New York time
    CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(PICKUP_DATETIME::VARCHAR)) AS PICKUP_TIME,
    CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(DROPOFF_DATETIME::VARCHAR)) AS DROPOFF_TIME,

    -- Create GEOGRAPHY point objects for locations
    ST_POINT(PICKUP_LONGITUDE, PICKUP_LATITUDE)   AS PICKUP_LOCATION,
    ST_POINT(DROPOFF_LONGITUDE, DROPOFF_LATITUDE) AS DROPOFF_LOCATION,

    -- Include trip distance and total amount
    trip_distance,
    total_amount
FROM
    (SELECT * FROM CARTO_ACADEMY.CARTO.TLC_YELLOW_TRIPS_2014
    UNION ALL
    SELECT * FROM CARTO_ACADEMY.CARTO.TLC_YELLOW_TRIPS_2015) all_trips
WHERE
    -- Filter for valid latitude and longitude coordinates
    pickup_latitude BETWEEN -90 AND 90
    AND dropoff_latitude BETWEEN -90 AND 90
    AND pickup_longitude BETWEEN -180 AND 180
    AND dropoff_longitude BETWEEN -180 AND 180

    -- Filter for trips with a meaningful distance and duration
    AND ST_DISTANCE(ST_POINT(PICKUP_LONGITUDE, PICKUP_LATITUDE), ST_POINT(DROPOFF_LONGITUDE, DROPOFF_LATITUDE)) > 10
    AND TIMEDIFF(MINUTE, TO_TIMESTAMP(PICKUP_DATETIME::VARCHAR), TO_TIMESTAMP(DROPOFF_DATETIME::VARCHAR)) > 1;
```

Now you will create a table where, for each pair of timestamp/H3, we calculate the number of trips. You will strip off minutes and seconds and keep only hours.

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_h3 AS
    SELECT TIME_SLICE(pickup_time, 60, 'minute', 'START') AS pickup_time,
        H3_POINT_TO_CELL_string(pickup_location, 8) AS h3,
        count(*) AS pickups
        FROM advanced_analytics.public.ny_taxi_rides
        GROUP BY 1, 2;
```

Since on resolution 8, you might have more than 1000 hexagons for New York, to speed; up the training process, you will keep only hexagons that had more than 1M pickups in 2014. This is shown in the following code block.

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_h3 AS
-- Use a Common Table Expression (CTE) to find the hexagons with a high volume of pickups
WITH all_hexagons AS (
    SELECT
        h3,
        SUM(pickups) AS total_pickups
    FROM
        advanced_analytics.public.ny_taxi_rides_h3
    WHERE
        YEAR(pickup_time) = 2014
    GROUP BY
        h3
)
-- Select all original data for the hexagons identified in the CTE
SELECT
    t1.*
FROM
    advanced_analytics.public.ny_taxi_rides_h3 AS t1
    INNER JOIN all_hexagons AS t2 ON t1.h3 = t2.h3
WHERE
    t2.total_pickups >= 1000000;
```

It's important to remember that if the raw data lacks records for a specific hour and area combination, the aggregated data for that period should be marked as 0. This step is crucial for accurate time series prediction. Run the following query to add records indicating that there were zero trips for any H3 location and timestamp pair without recorded trips.

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_h3 AS

-- Use a Common Table Expression (CTE) to generate a complete timeline
-- for every unique hexagon.
WITH all_dates_hexagons AS (
    SELECT
        -- Generate a continuous series of hourly timestamps from 2014 to 2015
        DATEADD(
            HOUR,
            VALUE::int,
            '2014-01-01'::timestamp
        ) AS pickup_time,
        h3
    FROM
        -- This generates a sequence of numbers for every hour in the 2-year range
        TABLE(FLATTEN(ARRAY_GENERATE_RANGE(0, DATEDIFF('hour', '2014-01-01', '2015-12-31 23:59:00') + 1)))
        -- Cross join with every unique hexagon to create a complete placeholder table
        CROSS JOIN (
            SELECT DISTINCT
                h3
            FROM
                advanced_analytics.public.ny_taxi_rides_h3
        )
)

-- Join the complete placeholder data with the actual pickup data
SELECT
    t1.pickup_time,
    t1.h3,
    -- If a matching record exists in the original table, use its pickup count.
    -- Otherwise, default to 0. (Corrected from 'IF F' to 'IFF')
    IFF(t2.pickups IS NOT NULL, t2.pickups, 0) AS pickups
FROM
    all_dates_hexagons AS t1
    LEFT JOIN advanced_analytics.public.ny_taxi_rides_h3 AS t2
        ON t1.pickup_time = t2.pickup_time AND t1.h3 = t2.h3;
```

## Step 3. Data Enrichment (continued)

In this step, you will enhance our dataset with extra features that could improve the accuracy of our predictions. The Cortex model for time series automatically encodes days of the week as a separate feature, but it makes sense to consider that public or school holidays could affect the demand for taxi services. Likewise, areas hosting sporting events might experience a surge in taxi pickups. To incorporate this insight, you will use data from [PredictHQ - Quickstart Demo](https://app.snowflake.com/marketplace/listing/GZSTZ3TGTNLQM/predicthq-quickstart-demo) listing, which provides information on events in New York for the years 2014-2015.

Run the following query to enrich the data with holiday, and event information. For sports events, you will include only those with a high rank.

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_h3 AS
SELECT
    t1.*,
    -- Add a column for school holidays
    IFF(
        t2.category = 'school-holidays',
        'school-holidays',
        'None'
    ) AS school_holiday,
    -- Add a column for public holidays, listing the specific holiday names
    IFF(
        t3.category = 'public-holidays',
        ARRAY_TO_STRING(t3.labels, ', '),
        'None'
    ) AS public_holiday,
    -- Add a column for the specific type of sports event
    IFF(
        t4.category = 'sports',
        t4.labels[0]::string,
        'None'
    ) AS sport_event
FROM
    advanced_analytics.public.ny_taxi_rides_h3 AS t1
    -- Join to find school holidays in New York
    LEFT JOIN (
        SELECT DISTINCT
            title,
            category,
            event_start,
            event_end,
            labels
        FROM
            PREDICTHQ_DEMO.PREDICTHQ.PREDICTHQ_EVENTS_SNOWFLAKE_SUMMIT_2024
        WHERE
            category = 'school-holidays' AND title ILIKE 'New York%'
    ) AS t2
        ON DATE(t1.pickup_time) BETWEEN t2.event_start AND t2.event_end
    -- Join to find national public holidays
    LEFT JOIN (
        SELECT DISTINCT
            title,
            category,
            event_start,
            event_end,
            labels
        FROM
            PREDICTHQ_DEMO.PREDICTHQ.PREDICTHQ_EVENTS_SNOWFLAKE_SUMMIT_2024
        WHERE
            ARRAY_CONTAINS('holiday-national'::variant, labels)
    ) AS t3
        ON DATE(t1.pickup_time) BETWEEN t3.event_start AND t3.event_end
    -- Join to find high-ranking sports events happening at the same time and location
    LEFT JOIN (
        SELECT
            *
        FROM
            PREDICTHQ_DEMO.PREDICTHQ.PREDICTHQ_EVENTS_SNOWFLAKE_SUMMIT_2024
        WHERE
            phq_rank > 70 AND category = 'sports'
    ) AS t4
        ON t1.pickup_time = DATE_TRUNC('hour', t4.event_start)
        AND t1.h3 = H3_POINT_TO_CELL_STRING(t4.geo, 8);
```

## Step 4. Training and Prediction

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_h3_train AS
SELECT
    *
FROM
    advanced_analytics.public.ny_taxi_rides_h3
WHERE
    DATE(pickup_time) < DATE('2015-06-05 12:00:00');
```

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_h3_predict AS
SELECT
    h3,
    pickup_time,
    SCHOOL_HOLIDAY,
    PUBLIC_HOLIDAY,
    SPORT_EVENT
FROM
    advanced_analytics.public.ny_taxi_rides_h3
WHERE
    DATE(pickup_time) >= DATE('2015-06-05')
    AND DATE(pickup_time) < DATE('2015-06-12');
```

```sql
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ny_taxi_rides_model(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'advanced_analytics.public.ny_taxi_rides_h3_train'),
    SERIES_COLNAME => 'h3',
    TIMESTAMP_COLNAME => 'pickup_time',
    TARGET_COLNAME => 'pickups'
);
```

```sql
BEGIN
    CALL ny_taxi_rides_model!FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'advanced_analytics.public.ny_taxi_rides_h3_predict'),
        SERIES_COLNAME => 'h3',
        TIMESTAMP_COLNAME => 'pickup_time',
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    LET query_id := SQLID;
    CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_model_forecast AS
    SELECT
        series::STRING AS h3,
        ts AS pickup_time,
        CASE WHEN forecast < 0 THEN 0 ELSE forecast END AS forecast,
        CASE WHEN lower_bound < 0 THEN 0 ELSE lower_bound END AS lower_bound,
        CASE WHEN upper_bound < 0 THEN 0 ELSE upper_bound END AS upper_bound
    FROM TABLE(RESULT_SCAN(:query_id));
END;
```

```sql
CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_compare AS
SELECT
    t1.h3,
    t1.pickup_time,
    t2.pickups,
    ROUND(t1.forecast, 0) AS forecast
FROM
    advanced_analytics.public.ny_taxi_rides_model_forecast AS t1
    INNER JOIN advanced_analytics.public.ny_taxi_rides_h3 AS t2
        ON t1.h3 = t2.h3
        AND t1.pickup_time = t2.pickup_time;
```

```sql
BEGIN
    CALL ny_taxi_rides_model!show_evaluation_metrics();
    LET sql_id := SQLID;
    CREATE OR REPLACE TABLE advanced_analytics.public.ny_taxi_rides_metrics AS
    SELECT
        series::string AS h3,
        metric_value,
        error_metric
    FROM TABLE(RESULT_SCAN(:sql_id));
END;
```

```sql
CREATE OR REPLACE TABLE
advanced_analytics.public.ny_taxi_rides_metrics AS
SELECT h3, metric_value AS smape
FROM advanced_analytics.public.ny_taxi_rides_metrics
WHERE error_metric::string = 'SMAPE'
ORDER BY 2 ASC;
```

## Step 5. Visualization and analysis

<img src="images/image_006.png" alt="alt text" width="45%">

<img src="images/image_007.png" alt="alt text" width="45%">

> For simplicity this code has been broken out to [Stream App Part 1](/code/Part1_Streamlit_App.py)

<img src="images/image_008.png" alt="alt text" width="45%">

<img src="images/image_009.png" alt="alt text" width="45%">

You have information about public and school holidays and sports events among our features. Perhaps adding information about other local events, such as festivals, could improve the overall quality of the model.

---

## Step 6. LTE Coverage on Highways (NY) using OpenStreetMap

This section answers “What percent of highways have LTE coverage?” using the free OpenStreetMap New York dataset. It uses the exact objects and columns from your Marketplace install so no edits are required.

### 6.1 Install Marketplace Data

1) In Snowsight, go to Marketplace and search for “OpenStreetMap New York”. Click Get and install.
2) The installed objects are available under:
   - Database: `openstreetmap_new_york`
   - Schema: `new_york`
   - Views:
     - `openstreetmap_new_york.new_york.V_OSM_NY_WAY` (road network)
     - `openstreetmap_new_york.new_york.V_OSM_NY_NODE` (point features, includes towers)
     - `openstreetmap_new_york.new_york.V_OSM_NY_BOUNDARY` (admin boundaries)

Column names used below:
- WAY: `ID`, `TAGS` (VARIANT), `COORDINATES` (GEOGRAPHY)
- NODE: `ID`, `TAGS` (VARIANT), `COORDINATES` (GEOGRAPHY)
- BOUNDARY: `GEO` (GEOGRAPHY)

### 6.2 Setup and Boundary

```sql
USE ROLE accountadmin;
USE WAREHOUSE my_wh;
USE advanced_analytics.public;

-- Create a single New York boundary polygon
CREATE OR REPLACE TABLE ny_boundary AS
SELECT
    ST_UNION_AGG(GEO) AS boundary
FROM openstreetmap_new_york.new_york.V_OSM_NY_BOUNDARY;

-- Quick sanity check
SELECT ST_AREA(boundary) AS area_m2 FROM ny_boundary;
```

### 6.3 Highways subset (clipped to boundary)

```sql
-- Create a highways table focusing on major highway classes.
CREATE OR REPLACE TABLE ny_highways AS
WITH raw AS (
    SELECT
        ID,
        TRY_TO_VARCHAR(TAGS:'highway') AS highway_class,
        COORDINATES AS geo
    FROM openstreetmap_new_york.new_york.V_OSM_NY_WAY
),
clipped AS (
    SELECT
        ID,
        highway_class,
        ST_INTERSECTION(geo, (SELECT boundary FROM ny_boundary)) AS geo
    FROM raw
    WHERE highway_class IN ('motorway','trunk','primary','secondary','tertiary')
      AND ST_INTERSECTS(geo, (SELECT boundary FROM ny_boundary))
)
SELECT * FROM clipped
WHERE geo IS NOT NULL;

-- Optional: length sanity check
SELECT
  highway_class,
  ROUND(SUM(ST_LENGTH(geo))/1000, 1) AS total_km
FROM ny_highways
GROUP BY 1
ORDER BY 1;
```

### 6.4 Cell towers and approximate LTE coverage buffers

```sql
-- Identify communication/cell towers based on OSM tags.
CREATE OR REPLACE TABLE ny_cell_towers AS
SELECT
    ID,
    COORDINATES AS geo
FROM openstreetmap_new_york.new_york.V_OSM_NY_NODE
WHERE
    COALESCE(LOWER(TRY_TO_VARCHAR(TAGS:'communication:mobile_phone')), '') IN ('yes','true','1')
    OR COALESCE(LOWER(TRY_TO_VARCHAR(TAGS:'tower:type')), '') LIKE '%communication%'
    OR COALESCE(LOWER(TRY_TO_VARCHAR(TAGS:'man_made')), '') IN ('mast','tower')
    OR COALESCE(LOWER(TRY_TO_VARCHAR(TAGS:'telecom')), '') IN ('antenna','tower');

-- Create circular buffers around towers as a simple LTE coverage proxy.
-- Start with 1500 meters; adjust as appropriate for your narrative.
CREATE OR REPLACE TABLE ny_lte_coverage_buffers AS
SELECT
    ID,
    ST_BUFFER(geo, 1500) AS geo
FROM ny_cell_towers;

-- Dissolve buffers into a single coverage area for efficient intersections
CREATE OR REPLACE TABLE ny_lte_coverage_area AS
SELECT ST_UNION_AGG(geo) AS geo
FROM ny_lte_coverage_buffers;
```

### 6.5 Highway coverage intersection and statistics

```sql
-- Intersect highways with LTE coverage area
CREATE OR REPLACE TABLE ny_highways_coverage AS
SELECT
    h.ID,
    h.highway_class,
    h.geo AS highway_geo,
    ST_INTERSECTION(h.geo, c.geo) AS covered_geo
FROM ny_highways h
CROSS JOIN ny_lte_coverage_area c;

-- Compute length-based coverage by class
CREATE OR REPLACE TABLE ny_highways_coverage_stats AS
WITH lengths AS (
    SELECT
        highway_class,
        SUM(ST_LENGTH(highway_geo)) AS total_m,
        SUM(ST_LENGTH(covered_geo)) AS covered_m
    FROM ny_highways_coverage
    GROUP BY 1
)
SELECT
    highway_class,
    total_m,
    covered_m,
    IFF(total_m > 0, ROUND(covered_m / total_m * 100, 2), NULL) AS pct_covered
FROM lengths
ORDER BY 1;

-- Overall percentage of all major highways covered
SELECT
  ROUND(SUM(covered_m) / NULLIF(SUM(total_m),0) * 100, 2) AS pct_covered_overall
FROM ny_highways_coverage_stats;
```

### 6.6 Notes

- Buffer radius is a modeling choice. Use 1000–3000 meters for sensitivity testing. Larger radii increase coverage and runtime.
- You can visualize results by loading `ny_highways_coverage_stats` into a simple worksheet chart, or by building a quick Streamlit layer showing `ny_highways` colored by `pct_covered` (optional).

## Continue to [Part 2](/lab_instructions/Part2.md)
