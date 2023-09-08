-- Rainmakers.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q10 CASCADE;

CREATE TABLE q10(
    driver_id INTEGER,
    month CHAR(2),
    mileage_2020 FLOAT,
    billings_2020 FLOAT,
    mileage_2021 FLOAT,
    billings_2021 FLOAT,
    mileage_increase FLOAT,
    billings_increase FLOAT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS monthly_stats CASCADE;
DROP VIEW IF EXISTS monthly_stats_2020 CASCADE;
DROP VIEW IF EXISTS monthly_stats_2021 CASCADE;


-- Define views for your intermediate steps here:

-- Find the total crow-flies distance and billed amount for each driver for each year
CREATE VIEW monthly_stats AS
SELECT driver.driver_id,
       LEFT(to_char(request.datetime, 'YYYY MM'), 4) as year,
        RIGHT(to_char(request.datetime, 'YYYY MM'), 2) as month,
       sum(request.destination <@> request.source) as mileage,
        sum(billed.amount) as billed
FROM driver LEFT OUTER JOIN ((((request JOIN dropoff ON request.request_id = dropoff.request_id)
    JOIN dispatch ON request.request_id = dispatch.request_id)
    JOIN clockedin ON dispatch.shift_id = clockedin.shift_id)
    JOIN billed ON billed.request_id = request.request_id)
    ON driver.driver_id = clockedin.driver_id
GROUP BY driver.driver_id, to_char(request.datetime, 'YYYY MM');

-- Create table for storing all the months in 2020 and 2021
DROP TABLE IF EXISTS all_months_20_21 CASCADE;

CREATE TABLE all_months_20_21(
    year CHAR(4),
    month CHAR(2)
);

INSERT INTO all_months_20_21 VALUES
    ('2020', '01'), ('2020', '02'), ('2020', '03'), ('2020', '04'), ('2020', '05'), ('2020', '06'),
    ('2020', '07'), ('2020', '08'), ('2020', '09'), ('2020', '10'), ('2020', '11'), ('2020', '12'),
    ('2021', '01'), ('2021', '02'), ('2021', '03'), ('2021', '04'), ('2021', '05'), ('2021', '06'),
    ('2021', '07'), ('2021', '08'), ('2021', '09'), ('2021', '10'), ('2021', '11'), ('2021', '12');

-- All data for each driver for 2020
CREATE VIEW monthly_stats_2020 AS
SELECT driver.driver_id, am.year, am.month, COALESCE(ms.mileage, 0) as mileage,
       COALESCE(ms.billed, 0) as billed
FROM (driver CROSS JOIN all_months_20_21 am) LEFT JOIN monthly_stats ms
    ON am.year||am.month = ms.year||ms.month and driver.driver_id = ms.driver_id
WHERE am.year = '2020'
ORDER BY driver.driver_id, am.year, am.month;

-- All data for each driver for 2020
CREATE VIEW monthly_stats_2021 AS
SELECT driver.driver_id, am.year, am.month, COALESCE(ms.mileage, 0) as mileage,
       COALESCE(ms.billed, 0) as billed
FROM (driver CROSS JOIN all_months_20_21 am) LEFT JOIN monthly_stats ms
    ON am.year||am.month = ms.year||ms.month and driver.driver_id = ms.driver_id
WHERE am.year = '2021'
ORDER BY driver.driver_id, am.year, am.month;


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q10

SELECT monthly_stats_2020.driver_id, monthly_stats_2020.month,
       monthly_stats_2020.mileage as mileage_2020, monthly_stats_2020.billed as billings_2020,
       monthly_stats_2021.mileage as mileage_2021, monthly_stats_2021.billed as billings_2021,
       monthly_stats_2021.mileage - monthly_stats_2020.mileage as mileage_increase,
       monthly_stats_2021.billed - monthly_stats_2020.billed as billings_increase
FROM monthly_stats_2020 JOIN monthly_stats_2021
    ON monthly_stats_2020.month = monthly_stats_2021.month
    AND monthly_stats_2020.driver_id = monthly_stats_2021.driver_id
ORDER BY monthly_stats_2020.driver_id, monthly_stats_2020.month;