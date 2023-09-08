-- Consistent raters.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q9 CASCADE;

CREATE TABLE q9(
    client_id INTEGER,
    email VARCHAR(30)
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS master_table CASCADE;
DROP VIEW IF EXISTS clients_rated_one_instance CASCADE;
DROP VIEW IF EXISTS clients_rated_all_drivers CASCADE;

-- Define views for your intermediate steps here:

-- Find all clients who have had a ride
-- For each client, find the drivers, and their ratings for each ride
CREATE VIEW master_table AS
SELECT request.client_id, clockedin.driver_id, request.request_id, driverrating.rating,
       CASE WHEN driverrating.rating IS NULL THEN 0 ELSE 1 END as if_rated
FROM (((request JOIN dropoff ON request.request_id = dropoff.request_id)
    JOIN dispatch ON request.request_id = dispatch.request_id)
    JOIN clockedin ON dispatch.shift_id = clockedin.shift_id)
    LEFT OUTER JOIN driverrating ON driverrating.request_id = request.request_id;

-- Find the clients who rated at least one instance of a ride with every driver
CREATE VIEW clients_rated_one_instance AS
SELECT client_id, driver_id, max(if_rated) as if_rated
FROM master_table
GROUP BY client_id, driver_id;

-- Find the clients who rated all the drivers
CREATE VIEW clients_rated_all_drivers AS
SELECT client_id, min(if_rated) as if_rated
FROM clients_rated_one_instance
GROUP BY client_id;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q9
SELECT client_id, email
FROM clients_rated_all_drivers NATURAL JOIN client
WHERE if_rated = 1;