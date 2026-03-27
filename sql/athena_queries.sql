-- Step 1: Create your personal database

CREATE DATABASE IF NOT EXISTS ridewave_raw_surajsingh;

-- Step 2: Create external table for rides

CREATE EXTERNAL TABLE ridewave_raw_surajsingh.rides (
    ride_id      STRING,
    driver_id    STRING,
    customer_id  STRING,
    vehicle_id   STRING,
    city         STRING,
    fare_amount  DOUBLE,
    distance_km  DOUBLE,
    ride_status  STRING,
    ride_date    STRING,
    pickup_time  STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://s3-de-q1-26/DE-Training/Day10/surajsingh_ridewave/raw/rides/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Step 3: Create external table for drivers
CREATE EXTERNAL TABLE ridewave_raw_surajsingh.drivers (
    driver_id    STRING,
    driver_name  STRING,
    city         STRING,
    vehicle_type STRING,
    rating       DOUBLE,
    total_rides  INT,
    joined_date  STRING,
    is_active    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://s3-de-q1-26/DE-Training/Day10/surajsingh_ridewave/raw/drivers/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Standard Query 1: Which city has most rides and revenue?

SELECT
    city,
    COUNT(*)                   AS total_rides,
    ROUND(SUM(fare_amount), 2) AS total_revenue
FROM ridewave_raw_surajsingh.rides
WHERE fare_amount IS NOT NULL
GROUP BY city
ORDER BY total_rides DESC;

-- Standard Query 2: What is the ride status breakdown?
SELECT
    ride_status,
    COUNT(*) AS count
FROM ridewave_raw_surajsingh.rides
GROUP BY ride_status
ORDER BY count DESC;

-- Standard Query 3: Top 5 earners (drivers)

SELECT
    driver_id,
    COUNT(*)                   AS total_rides,
    ROUND(SUM(fare_amount), 2) AS total_earned
FROM ridewave_raw_surajsingh.rides
WHERE fare_amount IS NOT NULL
GROUP BY driver_id
ORDER BY total_earned DESC
LIMIT 5;



-- S1a: RANK drivers by fare within each city

SELECT
    driver_id,
    city,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    RANK() OVER (
        PARTITION BY city
        ORDER BY SUM(fare_amount) DESC
    ) AS city_rank
FROM ridewave_raw_surajsingh.rides
WHERE fare_amount IS NOT NULL
GROUP BY driver_id, city;

-- S1b: Month-over-month ride count using LAG

WITH monthly AS (
    SELECT
        DATE_TRUNC('month', CAST(ride_date AS DATE)) AS month,
        COUNT(*) AS ride_count
    FROM ridewave_raw_surajsingh.rides
    GROUP BY 1
)
SELECT
    month,
    ride_count,
    LAG(ride_count) OVER (ORDER BY month) AS prev_month_count,
    ride_count - LAG(ride_count)
        OVER (ORDER BY month)              AS change
FROM monthly
ORDER BY month;

-- S1c: CTE — Drivers with more than 5 rides AND completed status

WITH completed_counts AS (
    SELECT driver_id, COUNT(*) AS total_rides
    FROM ridewave_raw_surajsingh.rides
    GROUP BY driver_id
    HAVING COUNT(*) > 5
),
completed_status AS (
    SELECT DISTINCT driver_id
    FROM ridewave_raw_surajsingh.rides
    WHERE ride_status = 'completed'
)
SELECT
    cc.driver_id,
    cc.total_rides,
    CASE WHEN cs.driver_id IS NOT NULL
         THEN 1 ELSE 0 END AS has_completed
FROM completed_counts cc
LEFT JOIN completed_status cs ON cc.driver_id = cs.driver_id
ORDER BY cc.total_rides DESC;