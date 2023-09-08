-- Ratings histogram.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q7 CASCADE;

CREATE TABLE q7(
    driver_id INTEGER,
    r5 INTEGER,
    r4 INTEGER,
    r3 INTEGER,
    r2 INTEGER,
    r1 INTEGER
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS all_ratings CASCADE;
DROP VIEW IF EXISTS ratings_per_driver CASCADE;

-- Define views for your intermediate steps here:

-- Tables in use
-- driver (driver_id), driverrating(request_id, rating),
-- clockedin(shift_id, driver_id), dispatch(request_id, shift_id)

-- Outer join all drivers to the ratings in DriverRatings
CREATE VIEW all_ratings AS
SELECT driver.driver_id, rating, count(rating) as times
FROM driver LEFT OUTER JOIN
    ((clockedin JOIN dispatch on clockedin.shift_id = dispatch.shift_id)
    NATURAL JOIN driverrating)
    ON driver.driver_id = clockedin.driver_id
GROUP BY driver.driver_id, rating;

-- Add the count of each rating per driver
CREATE VIEW ratings_per_driver AS
SELECT driver_id,
       CASE WHEN rating = 5 THEN times ELSE 0 END as r5,
       CASE WHEN rating = 4 THEN times ELSE 0 END as r4,
       CASE WHEN rating = 3 THEN times ELSE 0 END as r3,
       CASE WHEN rating = 2 THEN times ELSE 0 END as r2,
       CASE WHEN rating = 1 THEN times ELSE 0 END as r1
FROM all_ratings;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q7

-- sum the ratings under split driver_id
SELECT driver_id, sum(r5) as r5, sum(r4) as r4, sum(r3) as r3,
       sum(r2) as r2, sum(r1) as r1
FROM ratings_per_driver
GROUP BY driver_id;