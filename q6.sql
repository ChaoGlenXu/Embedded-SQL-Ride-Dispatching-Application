-- Frequent riders.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q6 CASCADE;

CREATE TABLE q6(
    client_id INTEGER,
    year CHAR(4),
    rides INTEGER
);

-- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
DROP VIEW IF EXISTS ride_years CASCADE;
DROP VIEW IF EXISTS year_clients CASCADE;
DROP VIEW IF EXISTS client_rides_one CASCADE;
DROP VIEW IF EXISTS max_min_one CASCADE;
DROP VIEW IF EXISTS max_min_one_clients CASCADE;
DROP VIEW IF EXISTS client_rides_two CASCADE;
DROP VIEW IF EXISTS max_min_two CASCADE;
DROP VIEW IF EXISTS max_min_two_clients CASCADE;
DROP VIEW IF EXISTS client_rides_three CASCADE;
DROP VIEW IF EXISTS max_min_three CASCADE;
DROP VIEW IF EXISTS max_min_three_clients CASCADE;

-- Define views for your intermediate steps here:

-- Find the years where some client had a ride
-- match all clients with all years
CREATE VIEW ride_years AS
SELECT to_char(request.datetime, 'YYYY') AS year
FROM request JOIN dropoff on request.request_id = dropoff.request_id;
-- GROUP BY to_char(request.datetime, 'YYYY');
-- ORDER BY to_char(request.datetime, 'YYYY'), count(request.request_id);

-- Find all combinations of clients and years
CREATE VIEW year_clients AS
SELECT year, client.client_id
FROM ride_years, client
GROUP BY year, client.client_id;

-- Get number of rides per client per year
CREATE VIEW client_rides_one AS
SELECT year, year_clients.client_id, COALESCE(count(request.request_id), 0) as num_rides
FROM year_clients LEFT OUTER JOIN request ON year_clients.client_id = request.client_id
    AND year = to_char(request.datetime, 'YYYY')
GROUP BY year, year_clients.client_id;

-- Find the highest and lowest number of rides per year
CREATE VIEW max_min_one AS
SELECT year, max(num_rides) as max_value, min(num_rides) as min_value
FROM client_rides_one
GROUP BY client_rides_one.year;

-- Find the clients with highest and lowest number of rides per year
CREATE VIEW max_min_one_clients AS
SELECT client_rides_one.year, client_rides_one.client_id, num_rides, max_value, min_value
FROM client_rides_one JOIN max_min_one ON client_rides_one.year = max_min_one.year
WHERE client_rides_one.num_rides = max_value OR client_rides_one.num_rides = min_value;

-- Remove the clients with highest and lowest number of rides per year
-- from the pool of all clients
CREATE VIEW client_rides_two AS
(select year, client_id, num_rides from client_rides_one)
    EXCEPT
(select year, client_id, num_rides from max_min_one_clients);

-- Find the SECOND highest and lowest number of rides per year
CREATE VIEW max_min_two AS
SELECT year, max(num_rides) as max_value, min(num_rides) as min_value
FROM client_rides_two
GROUP BY client_rides_two.year;

-- Find the clients with SECOND highest and lowest number of rides per year
CREATE VIEW max_min_two_clients AS
SELECT client_rides_two.year, client_rides_two.client_id, num_rides, max_value, min_value
FROM client_rides_two JOIN max_min_two ON client_rides_two.year = max_min_two.year
WHERE client_rides_two.num_rides = max_value OR client_rides_two.num_rides = min_value;

-- Remove the clients with SECOND highest and lowest number of rides per year
-- from the pool of all clients
CREATE VIEW client_rides_three AS
(select year, client_id, num_rides from client_rides_two)
    EXCEPT
(select year, client_id, num_rides from max_min_two_clients);

-- Find the THIRD highest and lowest number of rides per year
CREATE VIEW max_min_three AS
SELECT year, max(num_rides) as max_value, min(num_rides) as min_value
FROM client_rides_three
GROUP BY client_rides_three.year;

-- Find the clients with highest and lowest number of rides per year
CREATE VIEW max_min_three_clients AS
SELECT client_rides_three.year, client_rides_three.client_id, num_rides, max_value, min_value
FROM client_rides_three JOIN max_min_three ON client_rides_three.year = max_min_three.year
WHERE client_rides_three.num_rides = max_value OR client_rides_three.num_rides = min_value;

-- Remove the clients with highest and lowest number of rides per year
-- from the pool of all clients
-- CREATE VIEW client_rides_two AS
-- (select year, client_id from client_rides_three)
--     EXCEPT
-- (select year, client_id from max_min_clients);



-- CREATE VIEW max_rides AS
-- SELECT to_char(request.datetime, 'YYYY'), client.client_id, count(request.request_id)
-- FROM request NATURAL JOIN dropoff NATURAL JOIN client
-- GROUP BY to_char(request.datetime, 'YYYY'), client.client_id
-- HAVING count(request.request_id) > ALL (count(request.request_id))


-- CREATE VIEW max_rides AS
--     SELECT * FROM client


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q6

(SELECT client_id, year, num_rides as rides FROM max_min_one_clients)
UNION
(SELECT client_id, year, num_rides as rides FROM max_min_two_clients)
UNION
(SELECT client_id, year, num_rides as rides FROM max_min_three_clients);